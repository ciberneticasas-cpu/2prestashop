use std::collections::{HashMap, HashSet};
use std::error::Error;
use std::fs::{self, File};
use std::io::Write;
use std::time::Instant;

use chrono::Local;
use futures_util::stream::StreamExt;
use mysql_async::{params, prelude::*, Pool};
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;

// --- CONFIGURACION DE MARIADB (PrestaShop) ---
const DB_PREFIX: &str = "ps_";
const LOCAL_SYNC_CONFIG: &str = "/opt/2prestashopsync/sync_config.env";
const DEFAULT_AUDIT_HOSTS: [&str; 2] = ["192.168.0.231", "192.168.123.231"];

#[derive(Debug)]
struct Product {
    id_product: u32,
    name: String,
    ean13: String,
    reference: String,
    current_qty: Option<i32>,
    current_price: Option<f64>,
}

#[derive(Debug, Clone)]
struct ErpConnection {
    port: u16,
    database: String,
    user: String,
    password: String,
}

#[derive(Debug)]
struct ErpStock {
    by_ean: HashMap<String, ErpItem>,
    by_ref: HashMap<String, ErpItem>,
}

#[derive(Debug, Clone)]
struct ErpItem {
    productoid: String,
    name: String,
    qty: f64,
    unit: String,
    sales_price: Option<f64>,
    price_lists: String,
}

#[derive(Debug)]
struct ProductUpdate {
    id_product: u32,
    current_qty: i32,
    erp_qty: i32,
    pending_qty: i32,
    final_qty: i32,
    erp_key: String,
    current_price: f64,
    final_price: Option<f64>,
    update_stock: bool,
    update_price: bool,
}

#[derive(Debug)]
struct AuditRow {
    id_product: u32,
    name: String,
    reference: String,
    ean13: String,
    code: String,
    erp_name: String,
    erp_unit: String,
    mariadb_unit: String,
    conversion_factor: f64,
    stock_prod: Option<f64>,
    inventory_for_mariadb: Option<i32>,
    stock_mariadb: i32,
    pending_qty: i32,
    sync_final_qty: Option<i32>,
    price_mariadb: f64,
    price_erp: Option<f64>,
    price_for_mariadb: Option<f64>,
    price_lists: String,
    action: String,
}

fn config_value(config: &HashMap<String, String>, key: &str, default: &str) -> String {
    config
        .get(key)
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| default.to_string())
}

fn read_local_sync_config() -> HashMap<String, String> {
    let mut values = HashMap::new();

    if let Ok(contents) = fs::read_to_string(LOCAL_SYNC_CONFIG) {
        for line in contents.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            if let Some((key, value)) = line.split_once('=') {
                values.insert(key.trim().to_lowercase(), value.trim().to_string());
            }
        }
    }

    values
}

fn parse_hosts(value: Option<&String>, sync_host: &str) -> Vec<String> {
    let mut hosts: Vec<String> = value
        .map(|v| v.as_str())
        .unwrap_or("192.168.0.231,192.168.123.231")
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    if hosts.is_empty() {
        hosts = DEFAULT_AUDIT_HOSTS.iter().map(|s| s.to_string()).collect();
    }

    if !hosts.iter().any(|h| h == sync_host) {
        hosts.push(sync_host.to_string());
    }

    hosts
}

fn lookup_stock<'a>(
    stock: &'a ErpStock,
    ean13: &str,
    reference: &str,
) -> (Option<&'a ErpItem>, String, &'static str) {
    if !ean13.is_empty() {
        if let Some(item) = stock.by_ean.get(ean13) {
            return (Some(item), ean13.to_string(), "EAN");
        }
    }

    if !reference.is_empty() {
        if let Some(item) = stock.by_ref.get(reference) {
            return (Some(item), reference.to_string(), "REF");
        }
    }

    let code = if !reference.is_empty() {
        reference.to_string()
    } else {
        ean13.to_string()
    };

    (None, code, "SIN_MATCH")
}

async fn load_erp_stock(
    host: &str,
    erp: &ErpConnection,
    almacenes_in_clause: &str,
) -> Result<ErpStock, Box<dyn Error>> {
    let mut mssql_config = Config::new();
    mssql_config.host(host);
    mssql_config.port(erp.port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&erp.user, &erp.password));
    mssql_config.database(&erp.database);
    mssql_config.encryption(tiberius::EncryptionLevel::NotSupported);
//    mssql_config.trust_cert();

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;

    let mssql_query = format!(
        "SELECT \
            TRIM(p.productoid) AS CodigoProducto, \
            TRIM(p.barras) AS CodigoBarras, \
            TRIM(hp.referencia) AS Referencia, \
            TRIM(hp.nombre) AS NombreProducto, \
            TRIM(hp.unidad) AS UnidadERP, \
            CAST(p.valor AS VARCHAR(40)) AS PrecioVenta, \
            CAST(SUM(COALESCE(ip.invenactua, 0)) AS VARCHAR(40)) AS InventarioUnidades \
         FROM Producto p \
         INNER JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         LEFT JOIN InveProd ip ON ip.productoid = p.productoid AND ip.almacenid IN ({}) \
         GROUP BY p.productoid, p.barras, hp.referencia, hp.nombre, hp.unidad, p.valor",
        almacenes_in_clause
    );

    let mut select_stream = mssql_client.query(mssql_query, &[]).await?;
    let mut records: Vec<(String, String, ErpItem)> = Vec::new();
    let mut product_ids: HashSet<String> = HashSet::new();

    while let Some(row_result) = select_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let productoid = row.get::<&str, _>("CodigoProducto").unwrap_or("").trim().to_string();
            let ean = row.get::<&str, _>("CodigoBarras").unwrap_or("").trim().to_string();
            let reference = row.get::<&str, _>("Referencia").unwrap_or("").trim().to_string();
            let name = row.get::<&str, _>("NombreProducto").unwrap_or("").trim().to_string();
            let unit = row.get::<&str, _>("UnidadERP").unwrap_or("").trim().to_string();
            let sales_price = row
                .get::<&str, _>("PrecioVenta")
                .and_then(|value| value.trim().replace(',', ".").parse::<f64>().ok());
            let qty = row
                .get::<&str, _>("InventarioUnidades")
                .and_then(|value| value.trim().replace(',', ".").parse::<f64>().ok())
                .unwrap_or(0.0)
                .max(0.0);

            if !productoid.is_empty() {
                product_ids.insert(productoid.clone());
            }

            records.push((
                ean,
                reference.clone(),
                ErpItem {
                    productoid,
                    name,
                    qty,
                    unit,
                    sales_price,
                    price_lists: String::new(),
                },
            ));
        }
    }

    drop(select_stream);

    let price_lists_query = "SELECT \
            CAST(p.productoid AS VARCHAR(50)) AS CodigoProducto, \
            CONCAT('Producto valor3: ', CAST(p.valor3 AS VARCHAR(40))) AS PrecioLista \
         FROM Producto p \
         WHERE p.valor3 IS NOT NULL AND p.valor3 <> 0 \
         UNION ALL \
         SELECT \
            CAST(p.productoid AS VARCHAR(50)) AS CodigoProducto, \
            CONCAT('Producto valor5: ', CAST(p.valor5 AS VARCHAR(40))) AS PrecioLista \
         FROM Producto p \
         WHERE p.valor5 IS NOT NULL AND p.valor5 <> 0 \
         UNION ALL \
         SELECT \
            CAST(lp.ProductoId AS VARCHAR(50)) AS CodigoProducto, \
            CONCAT('Lista ', CAST(lp.Lista AS VARCHAR(20)), ': ', CAST(lp.Valor AS VARCHAR(40))) AS PrecioLista \
         FROM ListaPrecio lp \
         UNION ALL \
         SELECT \
            CAST(lpt.ProductoId AS VARCHAR(50)) AS CodigoProducto, \
            CONCAT('Tercero ', CAST(lpt.ListaPrecioId AS VARCHAR(20)), ' ', COALESCE(hlpt.Nombre, ''), ': ', CAST(lpt.Valor AS VARCHAR(40))) AS PrecioLista \
         FROM ListaPrecioTercero lpt \
         LEFT JOIN HeadListaPrecioTercero hlpt ON hlpt.ListaPrecioId = lpt.ListaPrecioId";

    let mut price_stream = mssql_client.query(price_lists_query, &[]).await?;
    let mut price_lists_by_product: HashMap<String, Vec<String>> = HashMap::new();
    let mut primary_price_by_product: HashMap<String, f64> = HashMap::new();

    while let Some(row_result) = price_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let productoid = row.get::<&str, _>("CodigoProducto").unwrap_or("").trim().to_string();
            if !product_ids.contains(&productoid) {
                continue;
            }

            let price_text = row.get::<&str, _>("PrecioLista").unwrap_or("").trim().to_string();
            if !price_text.is_empty() {
                if let Some(value) = price_text.strip_prefix("Lista 1: ") {
                    if let Ok(price) = value.trim().replace(',', ".").parse::<f64>() {
                        primary_price_by_product.insert(productoid.clone(), price);
                    }
                }

                price_lists_by_product
                    .entry(productoid)
                    .or_default()
                    .push(price_text);
            }
        }
    }

    let mut by_ean: HashMap<String, ErpItem> = HashMap::new();
    let mut by_ref: HashMap<String, ErpItem> = HashMap::new();

    for (ean, reference, mut item) in records {
        item.price_lists = price_lists_by_product
            .get(&item.productoid)
            .map(|items| items.join(" | "))
            .unwrap_or_default();
        if item.sales_price.is_none() {
            item.sales_price = primary_price_by_product.get(&item.productoid).copied();
        }

        if !ean.is_empty() {
            by_ean.insert(ean, item.clone());
        }
        if !reference.is_empty() {
            by_ref.insert(reference, item);
        }
    }

    Ok(ErpStock { by_ean, by_ref })
}

async fn inspect_erp_schema(host: &str, erp: &ErpConnection) -> Result<(), Box<dyn Error>> {
    let mut mssql_config = Config::new();
    mssql_config.host(host);
    mssql_config.port(erp.port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&erp.user, &erp.password));
    mssql_config.database(&erp.database);
    mssql_config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;
    let query = "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_NAME IN ('HeadProd', 'Producto', 'InveProd', 'Almacen', 'ListaPrecio', 'ListaPrecioTercero', 'HeadListaPrecioTercero') \
                 ORDER BY TABLE_NAME, ORDINAL_POSITION";
    let mut stream = mssql_client.query(query, &[]).await?;

    println!("Columnas ERP en {}:", host);
    while let Some(row_result) = stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let table = row.get::<&str, _>("TABLE_NAME").unwrap_or("");
            let column = row.get::<&str, _>("COLUMN_NAME").unwrap_or("");
            let data_type = row.get::<&str, _>("DATA_TYPE").unwrap_or("");
            println!("{}.{} ({})", table, column, data_type);
        }
    }

    drop(stream);

    println!("Muestra de precios base en Producto:");
    let sample_query = "SELECT TOP 20 \
            TRIM(productoid) AS productoid, \
            TRIM(barras) AS barras, \
            CAST(valor AS VARCHAR(40)) AS valor, \
            CAST(valor3 AS VARCHAR(40)) AS valor3, \
            CAST(valor5 AS VARCHAR(40)) AS valor5 \
         FROM Producto \
         WHERE valor IS NOT NULL OR valor3 IS NOT NULL OR valor5 IS NOT NULL \
         ORDER BY productoid";
    let mut sample_stream = mssql_client.query(sample_query, &[]).await?;
    while let Some(row_result) = sample_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let productoid = row.get::<&str, _>("productoid").unwrap_or("");
            let barras = row.get::<&str, _>("barras").unwrap_or("");
            let valor = row.get::<&str, _>("valor").unwrap_or("");
            let valor3 = row.get::<&str, _>("valor3").unwrap_or("");
            let valor5 = row.get::<&str, _>("valor5").unwrap_or("");
            println!(
                "productoid={} barras={} valor={} valor3={} valor5={}",
                productoid, barras, valor, valor3, valor5
            );
        }
    }

    drop(sample_stream);

    println!("Muestra de ListaPrecio:");
    let mut list_stream = mssql_client
        .query(
            "SELECT TOP 20 CAST(ProductoId AS VARCHAR(50)) AS ProductoId, CAST(Lista AS VARCHAR(20)) AS Lista, CAST(Valor AS VARCHAR(40)) AS Valor FROM ListaPrecio ORDER BY ProductoId, Lista",
            &[],
        )
        .await?;
    while let Some(row_result) = list_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "ProductoId={} Lista={} Valor={}",
                row.get::<&str, _>("ProductoId").unwrap_or(""),
                row.get::<&str, _>("Lista").unwrap_or(""),
                row.get::<&str, _>("Valor").unwrap_or("")
            );
        }
    }

    drop(list_stream);

    println!("Muestra de ListaPrecioTercero:");
    let mut third_stream = mssql_client
        .query(
            "SELECT TOP 20 CAST(ProductoId AS VARCHAR(50)) AS ProductoId, CAST(ListaPrecioId AS VARCHAR(20)) AS ListaPrecioId, CAST(Valor AS VARCHAR(40)) AS Valor FROM ListaPrecioTercero ORDER BY ProductoId, ListaPrecioId",
            &[],
        )
        .await?;
    while let Some(row_result) = third_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "ProductoId={} ListaPrecioId={} Valor={}",
                row.get::<&str, _>("ProductoId").unwrap_or(""),
                row.get::<&str, _>("ListaPrecioId").unwrap_or(""),
                row.get::<&str, _>("Valor").unwrap_or("")
            );
        }
    }

    Ok(())
}

async fn inspect_erp_product(
    host: &str,
    erp: &ErpConnection,
    product_code: &str,
) -> Result<(), Box<dyn Error>> {
    let mut mssql_config = Config::new();
    mssql_config.host(host);
    mssql_config.port(erp.port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&erp.user, &erp.password));
    mssql_config.database(&erp.database);
    mssql_config.encryption(tiberius::EncryptionLevel::NotSupported);

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;
    let product_query = format!(
        "SELECT \
            TRIM(p.productoid) AS productoid, TRIM(p.barras) AS barras, TRIM(p.barras2) AS barras2, TRIM(p.Barras3) AS barras3, \
            TRIM(hp.referencia) AS referencia, TRIM(hp.nombre) AS nombre, TRIM(hp.unidad) AS unidad, \
            CAST(hp.factor AS VARCHAR(40)) AS factor, CAST(hp.PUMContenidoInterno AS VARCHAR(40)) AS pum_contenido, \
            TRIM(hp.PUMUnidadMedida) AS pum_unidad, CAST(p.Cantidad1 AS VARCHAR(40)) AS cantidad1, \
            CAST(p.Cantidad2 AS VARCHAR(40)) AS cantidad2, CAST(p.Cantidad3 AS VARCHAR(40)) AS cantidad3 \
         FROM Producto p \
         JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         WHERE p.productoid = {code} OR hp.referencia = {code} OR p.barras = {code} OR p.barras2 = {code} OR p.Barras3 = {code}",
        code = sql_string(product_code)
    );

    println!("Producto ERP {}:", product_code);
    let mut product_stream = mssql_client.query(product_query, &[]).await?;
    while let Some(row_result) = product_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "productoid={} barras={} barras2={} barras3={} referencia={} nombre={} unidad={} factor={} pum={} {} cantidades=[{}, {}, {}]",
                row.get::<&str, _>("productoid").unwrap_or(""),
                row.get::<&str, _>("barras").unwrap_or(""),
                row.get::<&str, _>("barras2").unwrap_or(""),
                row.get::<&str, _>("barras3").unwrap_or(""),
                row.get::<&str, _>("referencia").unwrap_or(""),
                row.get::<&str, _>("nombre").unwrap_or(""),
                row.get::<&str, _>("unidad").unwrap_or(""),
                row.get::<&str, _>("factor").unwrap_or(""),
                row.get::<&str, _>("pum_contenido").unwrap_or(""),
                row.get::<&str, _>("pum_unidad").unwrap_or(""),
                row.get::<&str, _>("cantidad1").unwrap_or(""),
                row.get::<&str, _>("cantidad2").unwrap_or(""),
                row.get::<&str, _>("cantidad3").unwrap_or("")
            );
        }
    }
    drop(product_stream);

    let inventory_query = format!(
        "SELECT \
            ip.almacenid AS almacenid, a.nombre AS almacen, \
            CAST(ip.invenactua AS VARCHAR(40)) AS invenactua, \
            CAST(ip.invenfracc AS VARCHAR(40)) AS invenfracc, \
            CAST(ip.invensepar AS VARCHAR(40)) AS invensepar, \
            CAST(ip.invenpedid AS VARCHAR(40)) AS invenpedid, \
            CAST(ip.inventario AS VARCHAR(40)) AS inventario_unidades, \
            CAST(ip.inveninfra AS VARCHAR(40)) AS inveninfra, \
            CAST(ip.InvenOrdCo AS VARCHAR(40)) AS InvenOrdCo, \
            CAST(ip.InvenOrdPr AS VARCHAR(40)) AS InvenOrdPr, \
            CAST(COALESCE(ip.invenactua,0) - COALESCE(ip.invensepar,0) AS VARCHAR(40)) AS disponible \
         FROM InveProd ip \
         JOIN Producto p ON p.productoid = ip.productoid \
         LEFT JOIN Almacen a ON a.almacenid = ip.almacenid \
         LEFT JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         WHERE p.productoid = {code} OR hp.referencia = {code} OR p.barras = {code} OR p.barras2 = {code} OR p.Barras3 = {code} \
         ORDER BY ip.almacenid",
        code = sql_string(product_code)
    );

    println!("Inventario ERP por almacen {}:", product_code);
    let mut inventory_stream = mssql_client.query(inventory_query, &[]).await?;
    while let Some(row_result) = inventory_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            println!(
                "almacen={} nombre={} invenactua={} invenfracc={} invensepar={} invenpedid={} inventario_unidades={} inveninfra={} InvenOrdCo={} InvenOrdPr={} disponible={}",
                row.get::<&str, _>("almacenid").unwrap_or(""),
                row.get::<&str, _>("almacen").unwrap_or(""),
                row.get::<&str, _>("invenactua").unwrap_or(""),
                row.get::<&str, _>("invenfracc").unwrap_or(""),
                row.get::<&str, _>("invensepar").unwrap_or(""),
                row.get::<&str, _>("invenpedid").unwrap_or(""),
                row.get::<&str, _>("inventario_unidades").unwrap_or(""),
                row.get::<&str, _>("inveninfra").unwrap_or(""),
                row.get::<&str, _>("InvenOrdCo").unwrap_or(""),
                row.get::<&str, _>("InvenOrdPr").unwrap_or(""),
                row.get::<&str, _>("disponible").unwrap_or("")
            );
        }
    }

    Ok(())
}

fn fmt_qty(value: Option<i32>) -> String {
    value.map(|v| v.to_string()).unwrap_or_else(|| "SIN_MATCH".to_string())
}

fn fmt_qty_decimal(value: Option<f64>) -> String {
    value
        .map(|v| {
            if (v.fract()).abs() < 0.000001 {
                format!("{:.0}", v)
            } else {
                format!("{:.2}", v)
            }
        })
        .unwrap_or_else(|| "SIN_MATCH".to_string())
}

fn fmt_price(value: Option<f64>) -> String {
    value
        .map(|v| format!("{:.2}", v))
        .unwrap_or_else(|| "SIN_PRECIO".to_string())
}

fn price_is_different(current: f64, target: Option<f64>) -> bool {
    target
        .map(|price| (current - price).abs() >= 0.005)
        .unwrap_or(false)
}

fn normalize_unit(value: &str) -> String {
    let unit = value.trim().to_uppercase();
    match unit.as_str() {
        "KL" | "KG" | "KILO" | "KILOS" | "KILOGRAMO" | "KILOGRAMOS" => "KG".to_string(),
        "GR" | "G" | "GRAMO" | "GRAMOS" => "G".to_string(),
        "LT" | "L" | "LITRO" | "LITROS" => "L".to_string(),
        "ML" | "MILILITRO" | "MILILITROS" => "ML".to_string(),
        "UND" | "UN" | "UNIDAD" | "UNIDADES" => "UND".to_string(),
        "PAQ" | "PQT" | "PAQUETE" => "PAQ".to_string(),
        _ => unit,
    }
}

fn unit_token(value: &str) -> Option<&'static str> {
    match value {
        "g" | "gr" | "gramo" | "gramos" => Some("G"),
        "kg" | "kl" | "kilo" | "kilos" | "kilogramo" | "kilogramos" => Some("KG"),
        _ => None,
    }
}

fn parse_amount_token(value: &str) -> Option<f64> {
    let cleaned = value
        .trim_start_matches('x')
        .trim_start_matches('X')
        .replace(',', ".");
    cleaned.parse::<f64>().ok()
}

fn factor_from_weight(amount: f64, unit: &str) -> f64 {
    match unit {
        "G" => amount / 1000.0,
        "KG" => amount,
        _ => 1.0,
    }
}

fn infer_local_unit_and_factor(name: &str, erp_unit: &str) -> (String, f64) {
    let text = name.to_lowercase();
    let erp = normalize_unit(erp_unit);

    if erp != "KG" {
        return (erp, 1.0);
    }

    let tokens: Vec<&str> = text
        .split(|c: char| !(c.is_ascii_alphanumeric() || c == ',' || c == '.'))
        .filter(|s| !s.is_empty())
        .collect();

    let suffixes = [
        ("kilogramos", "KG"),
        ("kilogramo", "KG"),
        ("kilos", "KG"),
        ("kilo", "KG"),
        ("kg", "KG"),
        ("kl", "KG"),
        ("gramos", "G"),
        ("gramo", "G"),
        ("gr", "G"),
        ("g", "G"),
    ];

    for (index, token) in tokens.iter().enumerate() {
        for (suffix, unit) in suffixes {
            if let Some(number_part) = token.strip_suffix(suffix) {
                if let Some(amount) = parse_amount_token(number_part) {
                    let factor = factor_from_weight(amount, unit);
                    return (format!("{} {}", amount, unit), factor);
                }
            }
        }

        if let Some(amount) = parse_amount_token(token) {
            if let Some(next) = tokens.get(index + 1) {
                if let Some(unit) = unit_token(next) {
                    let factor = factor_from_weight(amount, unit);
                    return (format!("{} {}", amount, unit), factor);
                }
            }
        }
    }

    if erp == "KG" && (text.contains("500 gr") || text.contains("500gr") || text.contains("500 g")) {
        return ("500 G".to_string(), 0.5);
    }

    (erp, 1.0)
}

fn csv_escape(value: &str) -> String {
    let escaped = value.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn sql_string(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('\'', "''");
    format!("'{}'", escaped)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let total_start = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let apply_changes = args.iter().any(|arg| arg == "--apply");
    let audit_only = !apply_changes;

    println!(
        "[{}] Iniciando batch de sincronizacion de stock en Rust...",
        Local::now().format("%Y-%m-%d %H:%M:%S")
    );
    if audit_only {
        println!("MODO AUDITORIA: no se actualizara MariaDB. Use --apply para aplicar cambios.");
    } else {
        println!("MODO APLICAR: se actualizaran inventario y precio en MariaDB.");
    }

    let mariadb_start = Instant::now();
    let mariadb_host = std::env::var("MARIADB_HOST").unwrap_or_else(|_| "www.mercaboy.com".to_string());
    let mariadb_port = std::env::var("MARIADB_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3306);
    let mariadb_user = std::env::var("MARIADB_USER").unwrap_or_else(|_| "waplicaciones2".to_string());
    let mariadb_pass = std::env::var("MARIADB_PASSWORD").unwrap_or_else(|_| "RL{MeEj)vQ(F".to_string());
    let mariadb_db = std::env::var("MARIADB_DATABASE").unwrap_or_else(|_| "mercaboy_2024".to_string());

    let connection_url = format!(
        "mysql://{}:{}@{}:{}/{}",
        mariadb_user, mariadb_pass, mariadb_host, mariadb_port, mariadb_db
    );

    let opts = mysql_async::Opts::from_url(&connection_url)?;
    let pool = Pool::new(opts);
    let mut conn = pool.get_conn().await?;
    println!("Conectado a MariaDB con exito.");
    let mariadb_connect_ms = mariadb_start.elapsed().as_millis();

    let config_start = Instant::now();
    let config_query = format!(
        "SELECT name, value FROM {}configuration WHERE name LIKE 'MERCABOY_ERP_%'",
        DB_PREFIX
    );
    let config_rows: Vec<(String, Option<String>)> = conn.query(config_query).await?;
    let config: HashMap<String, String> = config_rows
        .into_iter()
        .map(|(k, v)| (k, v.unwrap_or_default()))
        .collect();

    let erp_connection = ErpConnection {
        port: config
            .get("MERCABOY_ERP_PORT")
            .filter(|s| !s.trim().is_empty())
            .and_then(|p| p.parse().ok())
            .unwrap_or(1433),
        database: config_value(&config, "MERCABOY_ERP_DATABASE", "ERPFIVE_MERCABOY"),
        user: config_value(&config, "MERCABOY_ERP_USER", "sa"),
        password: "Cmmi.2025".to_string(),
    };
    // password: config_value(&config, "MERCABOY_ERP_PASSWORD", "Cmmi.2025"),

    let almacenes_str = config_value(&config, "MERCABOY_ERP_ALMACENES", "001,002,003");
    let pending_window: i32 = config
        .get("MERCABOY_ERP_PENDING_WINDOW")
        .filter(|s| !s.trim().is_empty())
        .and_then(|w| w.parse().ok())
        .unwrap_or(10);

    let local_config = read_local_sync_config();
    let sync_host = local_config
        .get("sync_host")
        .cloned()
        .unwrap_or_else(|| "192.168.0.231".to_string());
    let audit_hosts = parse_hosts(local_config.get("audit_hosts"), &sync_host);

    println!("Archivo local de sincronizacion: {}", LOCAL_SYNC_CONFIG);
    println!("Servidor seleccionado para sincronizar MariaDB: {}", sync_host);
    println!("Servidores auditados: {}", audit_hosts.join(", "));
    println!("Almacenes ERP: {}", almacenes_str);
    let config_ms = config_start.elapsed().as_millis();

    if args.iter().any(|arg| arg == "--inspect-erp-schema") {
        inspect_erp_schema(&sync_host, &erp_connection).await?;
        return Ok(());
    }
    if let Some(position) = args.iter().position(|arg| arg == "--inspect-product") {
        let product_code = args
            .get(position + 1)
            .ok_or("--inspect-product requiere un codigo de producto")?;
        inspect_erp_product(&sync_host, &erp_connection, product_code).await?;
        return Ok(());
    }

    let products_start = Instant::now();
    let products_query = format!(
        "SELECT p.id_product, COALESCE(pl.name, ''), COALESCE(p.ean13, ''), COALESCE(p.reference, ''), sa.quantity, product_shop.price \
         FROM {}product p \
         INNER JOIN {}product_shop product_shop ON (product_shop.id_product = p.id_product AND product_shop.id_shop = 1) \
         LEFT JOIN {}product_lang pl ON (pl.id_product = p.id_product AND pl.id_shop = 1 \
             AND pl.id_lang = (SELECT CAST(value AS UNSIGNED) FROM {}configuration WHERE name = 'PS_LANG_DEFAULT' LIMIT 1)) \
         LEFT JOIN {}stock_available sa ON (sa.id_product = p.id_product AND sa.id_product_attribute = 0 AND sa.id_shop = 1) \
         WHERE product_shop.active = 1 \
           AND ((p.reference IS NOT NULL AND p.reference <> '') OR (p.ean13 IS NOT NULL AND p.ean13 <> ''))",
        DB_PREFIX, DB_PREFIX, DB_PREFIX, DB_PREFIX, DB_PREFIX
    );

    let ps_products: Vec<Product> = conn
        .query_map(
            products_query,
            |(id_product, name, ean13, reference, current_qty, current_price): (
                u32,
                String,
                String,
                String,
                Option<i32>,
                Option<f64>,
            )| Product {
                id_product,
                name,
                ean13,
                reference,
                current_qty,
                current_price,
            },
        )
        .await?;
    println!("Encontrados {} productos activos en PrestaShop.", ps_products.len());
    let products_ms = products_start.elapsed().as_millis();

    let almacenes_list: Vec<String> = almacenes_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect();
    let almacenes_in_clause = almacenes_list.join(", ");

    let mut stocks_by_host: HashMap<String, ErpStock> = HashMap::new();
    let erp_start = Instant::now();
    for host in &audit_hosts {
        println!("Consultando inventario ERP en {}...", host);
        let stock = load_erp_stock(host, &erp_connection, &almacenes_in_clause).await?;
        println!(
            "Cargados {} registros de stock desde {}.",
            stock.by_ean.len() + stock.by_ref.len(),
            host
        );
        stocks_by_host.insert(host.clone(), stock);
    }
    let erp_ms = erp_start.elapsed().as_millis();

    let sync_stock = stocks_by_host
        .get(&sync_host)
        .ok_or_else(|| format!("No se cargo stock para el servidor de sincronizacion {}", sync_host))?;

    let pending_start = Instant::now();
    let cancel_rows: Vec<String> = conn
        .query(format!(
            "SELECT value FROM {}configuration WHERE name IN ('PS_OS_CANCELED', 'PS_OS_ERROR')",
            DB_PREFIX
        ))
        .await?;
    let cancel_list_str = if cancel_rows.is_empty() {
        "6,8".to_string()
    } else {
        cancel_rows
            .into_iter()
            .filter(|s| !s.is_empty())
            .collect::<Vec<String>>()
            .join(",")
    };

    let export_table = format!("{}erp_order_export", DB_PREFIX);
    let export_table_exists: Option<u8> = conn
        .exec_first(
            "SELECT 1 FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :table_name LIMIT 1",
            params! {
                "table_name" => &export_table,
            },
        )
        .await?;

    let pending_rows: Vec<(u32, Option<i64>)> = if export_table_exists.is_some() {
        let pending_query = format!(
            "SELECT od.product_id, CAST(SUM(od.product_quantity) AS SIGNED) AS qty \
             FROM {}order_detail od \
             INNER JOIN {}orders o ON o.id_order = od.id_order \
             LEFT JOIN {}erp_order_export e ON e.id_order = o.id_order \
             WHERE od.product_attribute_id = 0 \
               AND o.current_state NOT IN ({}) \
               AND ( \
                     (e.id_erp_order_export IS NOT NULL AND e.erp_status IN ('pending', 'exported')) \
                     OR ( \
                         (e.erp_confirmed_at IS NULL OR e.id_erp_order_export IS NULL) \
                         AND o.date_add >= DATE_SUB(NOW(), INTERVAL :pending_window MINUTE) \
                     ) \
               ) \
             GROUP BY od.product_id",
            DB_PREFIX, DB_PREFIX, DB_PREFIX, cancel_list_str
        );

        conn.exec(
            pending_query,
            params! {
                "pending_window" => pending_window,
            },
        )
        .await?
    } else {
        println!(
            "Aviso: no existe la tabla {}; se calcularan pendientes como 0.",
            export_table
        );
        Vec::new()
    };

    let mut pending_qty_map: HashMap<u32, i32> = HashMap::new();
    for (product_id, qty) in pending_rows {
        if let Some(q) = qty {
            pending_qty_map.insert(product_id, q as i32);
        }
    }
    let pending_ms = pending_start.elapsed().as_millis();

    let audit_calc_start = Instant::now();
    let mut products_to_update: Vec<ProductUpdate> = Vec::new();
    let mut audit_rows: Vec<AuditRow> = Vec::new();
    let mut skipped_count = 0u32;
    let mut matched_by_ean = 0u32;
    let mut matched_by_ref = 0u32;
    let mut not_found_sync = 0u32;
    let mut different_stock = 0u32;
    let mut different_price = 0u32;
    let production_host = "192.168.0.231";

    for product in &ps_products {
        let ean13 = product.ean13.trim();
        let ref_code = product.reference.trim();
        let current_qty = product.current_qty.unwrap_or(0);
        let current_price = product.current_price.unwrap_or(0.0);
        let pending_qty = pending_qty_map.get(&product.id_product).copied().unwrap_or(0);

        let (sync_item, sync_key, match_type) = lookup_stock(sync_stock, ean13, ref_code);
        if match_type == "EAN" {
            matched_by_ean += 1;
        } else if match_type == "REF" {
            matched_by_ref += 1;
        }

        let prod_qty = stocks_by_host
            .get(production_host)
            .and_then(|stock| lookup_stock(stock, ean13, ref_code).0.map(|item| item.qty));
        let (sync_final_qty, inventory_for_mariadb, erp_name, erp_unit, mariadb_unit, conversion_factor, price_erp, price_for_mariadb, price_lists, action) = if let Some(erp_item) = sync_item {
            let erp_qty = erp_item.qty;
            let (mariadb_unit, conversion_factor) = infer_local_unit_and_factor(&product.name, &erp_item.unit);
            let stock_factor = if conversion_factor > 0.0 { conversion_factor } else { 1.0 };
            let inventory_for_mariadb = (erp_qty / stock_factor).floor().max(0.0) as i32;
            let final_qty = (inventory_for_mariadb - pending_qty).max(0);
            let converted_price = erp_item.sales_price.map(|price| price * conversion_factor);
            let update_stock = current_qty != final_qty;
            let update_price = price_is_different(current_price, converted_price);

            if update_stock {
                different_stock += 1;
            }
            if update_price {
                different_price += 1;
            }

            if update_stock || update_price {
                products_to_update.push(ProductUpdate {
                    id_product: product.id_product,
                    current_qty,
                    erp_qty: inventory_for_mariadb,
                    pending_qty,
                    final_qty,
                    erp_key: sync_key.clone(),
                    current_price,
                    final_price: converted_price,
                    update_stock,
                    update_price,
                });
                let action = match (update_stock, update_price) {
                    (true, true) => "ACTUALIZAR_STOCK_PRECIO",
                    (true, false) => "ACTUALIZAR_STOCK",
                    (false, true) => "ACTUALIZAR_PRECIO",
                    (false, false) => "SIN_CAMBIO",
                };
                (
                    Some(final_qty),
                    Some(inventory_for_mariadb),
                    erp_item.name.clone(),
                    normalize_unit(&erp_item.unit),
                    mariadb_unit,
                    conversion_factor,
                    erp_item.sales_price,
                    converted_price,
                    erp_item.price_lists.clone(),
                    action.to_string(),
                )
            } else {
                skipped_count += 1;
                (
                    Some(final_qty),
                    Some(inventory_for_mariadb),
                    erp_item.name.clone(),
                    normalize_unit(&erp_item.unit),
                    mariadb_unit,
                    conversion_factor,
                    erp_item.sales_price,
                    converted_price,
                    erp_item.price_lists.clone(),
                    "SIN_CAMBIO".to_string(),
                )
            }
        } else {
            not_found_sync += 1;
            skipped_count += 1;
            (
                None,
                None,
                String::new(),
                String::new(),
                String::new(),
                1.0,
                None,
                None,
                String::new(),
                "PRODUCTO PARA CREAR".to_string(),
            )
        };

        audit_rows.push(AuditRow {
            id_product: product.id_product,
            name: product.name.clone(),
            reference: product.reference.clone(),
            ean13: product.ean13.clone(),
            code: sync_key,
            erp_name,
            erp_unit,
            mariadb_unit,
            conversion_factor,
            stock_prod: prod_qty,
            inventory_for_mariadb,
            stock_mariadb: current_qty,
            pending_qty,
            sync_final_qty,
            price_mariadb: current_price,
            price_erp,
            price_for_mariadb,
            price_lists,
            action,
        });
    }
    let audit_calc_ms = audit_calc_start.elapsed().as_millis();

    println!();
    println!("============================================================");
    println!("RESUMEN GERENCIAL DE AUDITORIA");
    println!("============================================================");
    println!("Productos Prestashop              : {}", ps_products.len());
    println!("Servidor sincronizacion MariaDB   : {}", sync_host);
    println!("Coincidencias sync por EAN        : {}", matched_by_ean);
    println!("Coincidencias sync por REF        : {}", matched_by_ref);
    println!("Sin coincidencia en sync          : {}", not_found_sync);
    println!("Inventarios diferentes           : {}", different_stock);
    println!("Precios diferentes                : {}", different_price);
    println!("Registros que serian actualizados : {}", products_to_update.len());
    println!("Modo                              : {}", if audit_only { "AUDITORIA" } else { "APLICAR" });
    println!("Omitidos                          : {}", skipped_count);
    println!("============================================================");
    println!();

    println!(
        "{:<10} {:<20} {:>10} {:>10} {:>10} {:>8} {:>12} {:>12} {:>10} {:>10} {:>12} {:<24}",
        "ID",
        "CODIGO",
        "INV_ERP",
        "INV_MDB",
        "MARIADB",
        "UND_ERP",
        "UND_MDB",
        "FACTOR",
        "PS_PRECIO",
        "PRECIO_ERP",
        "PRECIO_MDB",
        "ACCION"
    );
    println!("{}", "-".repeat(118));

    for row in audit_rows
        .iter()
        .filter(|r| r.action != "SIN_CAMBIO")
        .take(200)
    {
        println!(
            "{:<10} {:<20} {:>10} {:>10} {:>10} {:>8} {:>12} {:>12.4} {:>10.2} {:>10} {:>12} {:<24}",
            row.id_product,
            row.code,
            fmt_qty_decimal(row.stock_prod),
            fmt_qty(row.inventory_for_mariadb),
            row.stock_mariadb,
            row.erp_unit,
            row.mariadb_unit,
            row.conversion_factor,
            row.price_mariadb,
            fmt_price(row.price_erp),
            fmt_price(row.price_for_mariadb),
            row.action
        );
    }

    println!(
        "Auditoria detecto {} registros que deberian actualizarse contra {}.",
        products_to_update.len(),
        sync_host
    );
    println!(
        "De {} productos activos, {} requieren actualizar stock y/o precio.",
        ps_products.len(),
        products_to_update.len()
    );

    let csv_start = Instant::now();
    let csv_name = format!(
        "stock_auditoria_{}.csv",
        Local::now().format("%Y%m%d_%H%M%S")
    );

    let mut csv = File::create(&csv_name)?;
    writeln!(
        csv,
        "id_product,nombre_prestashop,nombre_erp,referencia,ean13,codigo_match,unidad_erp,unidad_mariadb_inferida,factor_conversion_precio,inventario_erp_192_168_0_231,inventario_para_mariadb,inventario_mariadb,pendiente,final_sync,precio_mariadb,precio_erp,precio_para_mariadb,otras_listas_precios_erp,servidor_sync,accion"
    )?;

    for row in audit_rows.iter().filter(|row| row.action != "SIN_CAMBIO") {
        writeln!(
            csv,
            "{},{},{},{},{},{},{},{},{:.6},{},{},{},{},{},{:.2},{},{},{},{},{}",
            row.id_product,
            csv_escape(&row.name),
            csv_escape(&row.erp_name),
            csv_escape(&row.reference),
            csv_escape(&row.ean13),
            csv_escape(&row.code),
            csv_escape(&row.erp_unit),
            csv_escape(&row.mariadb_unit),
            row.conversion_factor,
            fmt_qty_decimal(row.stock_prod),
            fmt_qty(row.inventory_for_mariadb),
            row.stock_mariadb,
            row.pending_qty,
            fmt_qty(row.sync_final_qty),
            row.price_mariadb,
            fmt_price(row.price_erp),
            fmt_price(row.price_for_mariadb),
            csv_escape(&row.price_lists),
            sync_host,
            row.action
        )?;
    }

    println!("Archivo CSV generado: {}", csv_name);
    let csv_ms = csv_start.elapsed().as_millis();

    let update_start = Instant::now();
    let mut updated_count = 0;
    if audit_only {
        println!(
            "MODO AUDITORIA: se omitio la actualizacion. Revise el CSV y ejecute con --apply para aplicar."
        );
    } else {
        for chunk in products_to_update.chunks(500) {
            let mut tx = conn.start_transaction(mysql_async::TxOpts::default()).await?;

            let stock_chunk: Vec<&ProductUpdate> = chunk
                .iter()
                .filter(|product| product.update_stock)
                .collect();
            let price_chunk: Vec<&ProductUpdate> = chunk
                .iter()
                .filter(|product| product.update_price && product.final_price.is_some())
                .collect();

            if !stock_chunk.is_empty() {
                let ids = stock_chunk
                    .iter()
                    .map(|product| product.id_product.to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                let qty_cases = stock_chunk
                    .iter()
                    .map(|product| format!("WHEN {} THEN {}", product.id_product, product.final_qty))
                    .collect::<Vec<String>>()
                    .join(" ");

                tx.query_drop(format!(
                    "UPDATE {}stock_available \
                     SET quantity = CASE id_product {} ELSE quantity END, \
                         physical_quantity = CASE id_product {} ELSE physical_quantity END \
                     WHERE id_product_attribute = 0 AND id_shop = 1 AND id_product IN ({})",
                    DB_PREFIX, qty_cases, qty_cases, ids
                ))
                .await?;

                let log_values = stock_chunk
                    .iter()
                    .map(|product| {
                        format!(
                            "({}, {}, {}, {}, {}, {}, {}, NOW())",
                            sql_string(&product.erp_key),
                            product.id_product,
                            product.erp_qty,
                            product.pending_qty,
                            product.final_qty,
                            product.current_qty,
                            product.final_qty
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(",");

                tx.query_drop(format!(
                    "INSERT INTO {}erp_stock_sync_log ( \
                        erp_productoid, id_product, erp_disponible, ps_pendiente, \
                        qty_calculada, qty_anterior_ps, qty_aplicada, sync_at \
                     ) VALUES {}",
                    DB_PREFIX, log_values
                ))
                .await?;
            }

            if !price_chunk.is_empty() {
                let ids = price_chunk
                    .iter()
                    .map(|product| product.id_product.to_string())
                    .collect::<Vec<String>>()
                    .join(",");
                let price_cases = price_chunk
                    .iter()
                    .map(|product| {
                        format!(
                            "WHEN {} THEN {:.6}",
                            product.id_product,
                            product.final_price.unwrap_or(product.current_price)
                        )
                    })
                    .collect::<Vec<String>>()
                    .join(" ");

                tx.query_drop(format!(
                    "UPDATE {}product_shop \
                     SET price = CASE id_product {} ELSE price END \
                     WHERE id_shop = 1 AND id_product IN ({})",
                    DB_PREFIX, price_cases, ids
                ))
                .await?;

                tx.query_drop(format!(
                    "UPDATE {}product \
                     SET price = CASE id_product {} ELSE price END \
                     WHERE id_product IN ({})",
                    DB_PREFIX, price_cases, ids
                ))
                .await?;
            }

            updated_count += chunk.len();
            tx.commit().await?;
        }
    }
    let update_ms = update_start.elapsed().as_millis();

    let metadata_start = Instant::now();
    if !audit_only {
        let now_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
        conn.exec_drop(
            format!(
                "UPDATE {}configuration SET value = :now WHERE name = 'MERCABOY_ERP_LAST_BATCH_AT'",
                DB_PREFIX
            ),
            params! { "now" => now_str },
        )
        .await?;

        conn.exec_drop(
            format!(
                "UPDATE {}configuration SET value = :updated WHERE name = 'MERCABOY_ERP_LAST_BATCH_UPDATED'",
                DB_PREFIX
            ),
            params! { "updated" => updated_count.to_string() },
        )
        .await?;
    }
    let metadata_ms = metadata_start.elapsed().as_millis();

    if audit_only {
        println!(
            "Auditoria finalizada correctamente: {} diferencias detectadas, 0 actualizados.",
            products_to_update.len()
        );
    } else {
        println!(
            "Sincronizacion finalizada correctamente: {} actualizados, {} omitidos (sin cambios o sin match).",
            updated_count, skipped_count
        );
    }
    println!();
    println!("============================================================");
    println!("TIEMPOS DE EJECUCION (ms)");
    println!("============================================================");
    println!("Conexion MariaDB          : {}", mariadb_connect_ms);
    println!("Configuracion             : {}", config_ms);
    println!("Productos PrestaShop      : {}", products_ms);
    println!("Consulta ERP total        : {}", erp_ms);
    println!("Pendientes PrestaShop     : {}", pending_ms);
    println!("Calculo auditoria         : {}", audit_calc_ms);
    println!("CSV auditoria             : {}", csv_ms);
    println!("Actualizacion MariaDB     : {}", update_ms);
    println!("Metadata batch            : {}", metadata_ms);
    println!("Total programa            : {}", total_start.elapsed().as_millis());
    println!("============================================================");
    Ok(())
}
