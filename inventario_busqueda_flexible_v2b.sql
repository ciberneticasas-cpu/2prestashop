SET NOCOUNT ON;

-- Consulta flexible mejorada: acepta un término de búsqueda que puede ser productoid, referencia, codigprove, barras o texto en nombre
-- Maneja búsquedas numéricas sin ceros a la izquierda (ej. '2801' -> '002801')
-- Parámetros de ejemplo (editar según necesidad):
DECLARE @Search NVARCHAR(200) = '2801';
DECLARE @Almacenes TABLE(AlmacenId NCHAR(4));
INSERT INTO @Almacenes VALUES('001'),('002'),('003');

-- Normalizar búsqueda para LIKE
DECLARE @SearchLike NVARCHAR(200) = '%' + REPLACE(LTRIM(RTRIM(@Search)), ' ', '%') + '%';

-- Preparar variantes numéricas con ceros a la izquierda
DECLARE @SearchPadded NVARCHAR(15) = RIGHT('000000' + @Search, 6);

-- Encontrar productoid(s) que coincidan
DECLARE @Matches TABLE(productoid NVARCHAR(15));

INSERT INTO @Matches(productoid)
SELECT DISTINCT p.productoid
FROM Producto p
LEFT JOIN HeadProd hp ON p.headprodid = hp.headprodid
WHERE p.productoid = @Search
   OR p.productoid = @SearchPadded
   OR hp.referencia = @Search
   OR hp.codigprove = @Search
   OR p.barras = @Search
   OR p.barras2 = @Search
   OR p.barras3 = @Search
   OR hp.referencia LIKE @SearchLike
   OR hp.nombrrefer LIKE @SearchLike
   OR hp.nombre LIKE @SearchLike
   OR p.barras LIKE @SearchLike;

-- Si no hay matches exactos, intentar búsqueda en nombre por tokens
IF NOT EXISTS(SELECT 1 FROM @Matches)
BEGIN
    INSERT INTO @Matches(productoid)
    SELECT DISTINCT p.productoid
    FROM Producto p
    LEFT JOIN HeadProd hp ON p.headprodid = hp.headprodid
    WHERE hp.nombre LIKE @SearchLike
       OR hp.nombrrefer LIKE @SearchLike;
END

-- Mostrar qué productoid(s) se encontraron
SELECT 'MatchedProduct' AS Type, * FROM @Matches;

-- Asegúrate de que hp.precio sea el campo de precio de venta correcto en tu esquema.
-- Si tu tabla usa otro nombre, reemplázalo por el campo real (por ejemplo precio_venta, precventa, precio1, etc.).
-- Agregación por almacén y total para los productoid encontrados
SELECT
  ip.almacenid AS AlmacenId,
  a.nombre AS NombreAlmacen,
  p.productoid AS CodigoProducto,
  hp.referencia AS ReferenciaInterna,
  hp.nombre AS NombreProducto,
  hp.precio AS PrecioVenta,
  SUM(COALESCE(ip.invenactua,0)) AS InventarioActual,
  SUM(COALESCE(ip.invensepar,0)) AS InventarioSeparado,
  SUM(COALESCE(ip.invenactua,0)) - SUM(COALESCE(ip.invensepar,0)) AS InventarioDisponible
FROM InveProd ip
JOIN Producto p ON p.productoid = ip.productoid
JOIN HeadProd hp ON p.headprodid = hp.headprodid
JOIN Almacen a ON ip.almacenid = a.almacenid
JOIN @Matches m ON m.productoid = p.productoid
WHERE ip.almacenid IN (SELECT AlmacenId FROM @Almacenes)
GROUP BY ip.almacenid, a.nombre, p.productoid, hp.referencia, hp.nombre, hp.precio
ORDER BY ip.almacenid;

-- Total consolidado
SELECT
  p.productoid AS CodigoProducto,
  hp.referencia AS ReferenciaInterna,
  hp.nombre AS NombreProducto,
  hp.precio AS PrecioVenta,
  SUM(COALESCE(ip.invenactua,0)) - SUM(COALESCE(ip.invensepar,0)) AS InventarioDisponibleTotal
FROM InveProd ip
JOIN Producto p ON p.productoid = ip.productoid
JOIN HeadProd hp ON p.headprodid = hp.headprodid
JOIN @Matches m ON m.productoid = p.productoid
WHERE ip.almacenid IN (SELECT AlmacenId FROM @Almacenes)
GROUP BY p.productoid, hp.referencia, hp.nombre, hp.precio;
