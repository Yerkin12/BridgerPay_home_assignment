-- Top-5 categories by GMV (last 7 days), line-level, USD-normalized
WITH orders AS (
  SELECT
    CAST(order_id AS INT64) AS order_id,
    TIMESTAMP_SECONDS(CAST(order_timestamp AS INT64)) AS order_ts,
    CAST(currency AS STRING) AS currency
  FROM `bp-nw-next-yerkin.northwind_next.bronze_opdb_raw`
  WHERE table = 'orders'
),
items AS (
  SELECT
    CAST(order_id AS INT64) AS order_id,
    CAST(sku AS STRING)     AS sku,
    CAST(quantity AS INT64) AS quantity,
    CAST(unit_price AS NUMERIC) AS unit_price
  FROM `bp-nw-next-yerkin.northwind_next.bronze_opdb_raw`
  WHERE table = 'order_items'
),
fx AS (
  SELECT DATE(date) AS fx_date, CAST(eur_usd AS NUMERIC) AS eur_usd
  FROM `bp-nw-next-yerkin.northwind_next.bronze_fx_rates`
),
lines AS (
  SELECT
    o.order_ts,
    i.sku,
    (i.quantity * i.unit_price) AS line_amount_source,
    CASE
      WHEN o.currency = 'USD' THEN (i.quantity * i.unit_price)
      WHEN o.currency = 'EUR' THEN (i.quantity * i.unit_price) * f.eur_usd
      ELSE (i.quantity * i.unit_price)
    END AS line_amount_usd
  FROM items i
  JOIN orders o ON o.order_id = i.order_id
  LEFT JOIN fx f ON f.fx_date = DATE(o.order_ts)
  WHERE o.order_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
),
sku_asof AS (
  SELECT l.*, p.category
  FROM lines l
  LEFT JOIN `bp-nw-next-yerkin.northwind_next.dim_product` p
    ON l.sku = p.sku
   AND DATE(l.order_ts) BETWEEN p.valid_from AND p.valid_to
)
SELECT COALESCE(category, 'unknown') AS category,
       SUM(line_amount_usd) AS gmv_usd
FROM sku_asof
GROUP BY category
ORDER BY gmv_usd DESC
LIMIT 5;
