use std::collections::HashMap;
use std::error::Error;
use mysql_async::{prelude::*, Pool, params};
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use futures_util::stream::StreamExt;
use chrono::Local;

// --- CONFIGURACIÓN DE MARIADB (PrestaShop) ---
const DB_PREFIX: &str = "ps_";

#[derive(Debug)]
struct Product {
    id_product: u32,
    ean13: String,
    reference: String,
    current_qty: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("[{}] Iniciando batch de sincronización de stock en Rust...", Local::now().format("%Y-%m-%d %H:%M:%S"));

    // 1. Conectar a MariaDB (PrestaShop) usando variables de entorno con valores por defecto del servidor 192.168.0.162
    let mariadb_host = std::env::var("MARIADB_HOST").unwrap_or_else(|_| "192.168.0.162".to_string());
    let mariadb_port = std::env::var("MARIADB_PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(3306);
    let mariadb_user = std::env::var("MARIADB_USER").unwrap_or_else(|_| "prestashop".to_string());
    let mariadb_pass = std::env::var("MARIADB_PASSWORD").unwrap_or_else(|_| "prestashop".to_string());
    let mariadb_db = std::env::var("MARIADB_DATABASE").unwrap_or_else(|_| "mercaboy_2024".to_string());

    let connection_url = format!(
        "mysql://{}:{}@{}:{}/{}",
        mariadb_user, mariadb_pass, mariadb_host, mariadb_port, mariadb_db
    );
    
    let opts = mysql_async::Opts::from_url(&connection_url)?;
    let pool = Pool::new(opts);
    let mut conn = pool.get_conn().await?;
    println!("Conectado a MariaDB con éxito.");

    // 2. Obtener la configuración del ERP guardada en PrestaShop
    let config_query = format!("SELECT name, value FROM {}configuration WHERE name LIKE 'MERCABOY_ERP_%'", DB_PREFIX);
    let config_rows: Vec<(String, Option<String>)> = conn.query(config_query).await?;
    let config: HashMap<String, String> = config_rows
        .into_iter()
        .map(|(k, v)| (k, v.unwrap_or_default()))
        .collect();

    // Filtramos cadenas vacías para que usen correctamente el valor por defecto (igual que hace PrestaShop)
    let mssql_host = config.get("MERCABOY_ERP_HOST")
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| "192.168.123.231".to_string());

    let mssql_port: u16 = config.get("MERCABOY_ERP_PORT")
        .filter(|s| !s.trim().is_empty())
        .and_then(|p| p.parse().ok())
        .unwrap_or(1433);

    let mssql_db = config.get("MERCABOY_ERP_DATABASE")
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| "ERPFIVE_MERCABOY".to_string());

    let mssql_user = config.get("MERCABOY_ERP_USER")
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| "sa".to_string());

    let mssql_pass = config.get("MERCABOY_ERP_PASSWORD")
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| "Cmmi.2025".to_string());

    let almacenes_str = config.get("MERCABOY_ERP_ALMACENES")
        .filter(|s| !s.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| "001,002,003".to_string());

    let pending_window: i32 = config.get("MERCABOY_ERP_PENDING_WINDOW")
        .filter(|s| !s.trim().is_empty())
        .and_then(|w| w.parse().ok())
        .unwrap_or(10);

    println!("Configuración ERP leída: Host={}:{}, Almacenes={}", mssql_host, mssql_port, almacenes_str);

    // 3. Conectar a Microsoft SQL Server (Tiberius)
    let mut mssql_config = Config::new();
    mssql_config.host(&mssql_host);
    mssql_config.port(mssql_port);
    mssql_config.authentication(tiberius::AuthMethod::sql_server(&mssql_user, &mssql_pass));
    mssql_config.database(&mssql_db);
    mssql_config.trust_cert(); // Confiar en certificado autofirmado (TrustServerCertificate=true)

    let tcp = TcpStream::connect(mssql_config.get_addr()).await?;
    tcp.set_nodelay(true)?;

    let mut mssql_client = Client::connect(mssql_config, tcp.compat_write()).await?;
    println!("Conectado al servidor Microsoft SQL Server.");

    // 4. Obtener productos activos de PrestaShop
    let products_query = format!(
        "SELECT p.id_product, COALESCE(p.ean13, ''), COALESCE(p.reference, ''), sa.quantity \
         FROM {}product p \
         INNER JOIN {}product_shop product_shop ON (product_shop.id_product = p.id_product AND product_shop.id_shop = 1) \
         LEFT JOIN {}stock_available sa ON (sa.id_product = p.id_product AND sa.id_product_attribute = 0 AND sa.id_shop = 1) \
         WHERE product_shop.active = 1 \
           AND ((p.reference IS NOT NULL AND p.reference <> '') OR (p.ean13 IS NOT NULL AND p.ean13 <> ''))",
        DB_PREFIX, DB_PREFIX, DB_PREFIX
    );

    let ps_products: Vec<Product> = conn
        .query_map(
            products_query,
            |(id_product, ean13, reference, current_qty): (u32, String, String, Option<i32>)| Product {
                id_product,
                ean13,
                reference,
                current_qty,
            },
        )
        .await?;
    println!("Encontrados {} productos activos en PrestaShop.", ps_products.len());

    // 5. Consultar todo el stock disponible del ERP (MSSQL)
    let almacenes_list: Vec<String> = almacenes_str
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| format!("'{}'", s.replace('\'', "''")))
        .collect();
    let almacenes_in_clause = almacenes_list.join(", ");

    let mssql_query = format!(
        "SELECT \
            TRIM(p.barras) AS CodigoBarras, \
            TRIM(hp.referencia) AS Referencia, \
            CAST(SUM(COALESCE(ip.invenactua, 0)) - SUM(COALESCE(ip.invensepar, 0)) AS INT) AS InventarioDisponible \
         FROM Producto p \
         INNER JOIN HeadProd hp ON hp.headprodid = p.headprodid \
         LEFT JOIN InveProd ip ON ip.productoid = p.productoid AND ip.almacenid IN ({}) \
         GROUP BY p.barras, hp.referencia",
        almacenes_in_clause
    );

    let mut select_stream = mssql_client.query(mssql_query, &[]).await?;
    let mut erp_by_ean: HashMap<String, i32> = HashMap::new();
    let mut erp_by_ref: HashMap<String, i32> = HashMap::new();

    while let Some(row_result) = select_stream.next().await {
        let item = row_result?;
        if let tiberius::QueryItem::Row(row) = item {
            let ean = row.get::<&str, _>("CodigoBarras").unwrap_or("").trim().to_string();
            let reference = row.get::<&str, _>("Referencia").unwrap_or("").trim().to_string();
            let qty = row.get::<i32, _>("InventarioDisponible").unwrap_or(0).max(0);

            if !ean.is_empty() {
                erp_by_ean.insert(ean, qty);
            }
            if !reference.is_empty() {
                erp_by_ref.insert(reference, qty);
            }
        }
    }
    println!("Cargados {} registros de stock del ERP.", erp_by_ean.len() + erp_by_ref.len());

    // 6. Obtener estados a excluir y calcular todas las ventas pendientes (Optimizado en 1 query)
    let cancel_rows: Vec<String> = conn
        .query(format!("SELECT value FROM {}configuration WHERE name IN ('PS_OS_CANCELED', 'PS_OS_ERROR')", DB_PREFIX))
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

    let pending_rows: Vec<(u32, Option<i64>)> = conn
        .exec(
            pending_query,
            params! {
                "pending_window" => pending_window,
            },
        )
        .await?;

    let mut pending_qty_map: HashMap<u32, i32> = HashMap::new();
    for (product_id, qty) in pending_rows {
        if let Some(q) = qty {
            pending_qty_map.insert(product_id, q as i32);
        }
    }

    // 7. Determinar qué productos necesitan actualización
    let mut products_to_update = Vec::new();
    let mut skipped_count = 0;

    for product in ps_products {
        let ean13 = product.ean13.trim();
        let ref_code = product.reference.trim();

        let mut erp_qty = None;
        let mut erp_key = String::new();

        if !ean13.is_empty() && erp_by_ean.contains_key(ean13) {
            erp_qty = erp_by_ean.get(ean13).copied();
            erp_key = ean13.to_string();
        } else if !ref_code.is_empty() && erp_by_ref.contains_key(ref_code) {
            erp_qty = erp_by_ref.get(ref_code).copied();
            erp_key = ref_code.to_string();
        }

        let erp_qty = match erp_qty {
            Some(q) => q,
            None => {
                skipped_count += 1;
                continue;
            }
        };

        let pending_qty = pending_qty_map.get(&product.id_product).copied().unwrap_or(0);
        let final_qty = (erp_qty - pending_qty).max(0);

        if product.current_qty.is_none() || product.current_qty.unwrap() != final_qty {
            products_to_update.push((product, erp_qty, pending_qty, final_qty, erp_key));
        } else {
            skipped_count += 1;
        }
    }

    println!("De {} productos activos, {} requieren actualizar stock.", skipped_count + products_to_update.len(), products_to_update.len());

    // 8. Actualizar MariaDB en bloques de 500 productos (Chunked Commits) para evitar bloqueos prolongados
    let mut updated_count = 0;
    for chunk in products_to_update.chunks(500) {
        let mut tx = conn.start_transaction(mysql_async::TxOpts::default()).await?;

        let update_stmt = tx.prep(format!(
            "UPDATE {}stock_available \
             SET quantity = :qty, physical_quantity = :qty \
             WHERE id_product = :id_product AND id_product_attribute = 0 AND id_shop = 1",
            DB_PREFIX
        )).await?;

        let log_stmt = tx.prep(format!(
            "INSERT INTO {}erp_stock_sync_log ( \
                erp_productoid, id_product, erp_disponible, ps_pendiente, \
                qty_calculada, qty_anterior_ps, qty_aplicada, sync_at \
             ) VALUES (:erp_key, :id_product, :erp_qty, :pending_qty, :final_qty, :current_qty, :final_qty, NOW())",
            DB_PREFIX
        )).await?;

        for (product, erp_qty, pending_qty, final_qty, erp_key) in chunk {
            tx.exec_drop(
                &update_stmt,
                params! {
                    "qty" => *final_qty,
                    "id_product" => product.id_product,
                },
            ).await?;

            tx.exec_drop(
                &log_stmt,
                params! {
                    "erp_key" => erp_key,
                    "id_product" => product.id_product,
                    "erp_qty" => *erp_qty,
                    "pending_qty" => *pending_qty,
                    "final_qty" => *final_qty,
                    "current_qty" => product.current_qty.unwrap_or(0),
                },
            ).await?;

            updated_count += 1;
        }
        tx.commit().await?;
    }

    // 9. Actualizar metadatos del batch en PrestaShop
    let now_str = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    conn.exec_drop(
        format!("UPDATE {}configuration SET value = :now WHERE name = 'MERCABOY_ERP_LAST_BATCH_AT'", DB_PREFIX),
        params! { "now" => now_str },
    ).await?;
    
    conn.exec_drop(
        format!("UPDATE {}configuration SET value = :updated WHERE name = 'MERCABOY_ERP_LAST_BATCH_UPDATED'", DB_PREFIX),
        params! { "updated" => updated_count.to_string() },
    ).await?;

    println!("Sincronización finalizada correctamente: {} actualizados, {} omitidos (sin cambios).", updated_count, skipped_count);
    Ok(())
}
