-- Rebuild-all fact_orders (orders + item_count + USD normalization), CTAS
CREATE OR REPLACE TABLE `bp-nw-next-yerkin.northwind_next.fact_orders`
PARTITION BY DATE(order_ts) AS
WITH orders AS (
  SELECT
    CAST(order_id AS INT64) AS order_id,
    CAST(customer_id AS STRING) AS customer_id,
    TIMESTAMP_SECONDS(CAST(order_timestamp AS INT64)) AS order_ts,
    CAST(status AS STRING) AS status,
    CAST(currency AS STRING) AS currency,
    CAST(total_amount AS NUMERIC) AS total_amount_source
  FROM `bp-nw-next-yerkin.northwind_next.bronze_opdb_raw`
  WHERE table = 'orders'
),
item_counts AS (
  SELECT
    CAST(order_id AS INT64) AS order_id,
    COUNT(1) AS item_count
  FROM `bp-nw-next-yerkin.northwind_next.bronze_opdb_raw`
  WHERE table = 'order_items'
  GROUP BY order_id
),
fx AS (
  SELECT DATE(date) AS fx_date, CAST(eur_usd AS NUMERIC) AS eur_usd
  FROM `bp-nw-next-yerkin.northwind_next.bronze_fx_rates`
)
SELECT
  o.order_id, o.customer_id, o.order_ts, o.status,
  COALESCE(ic.item_count, 0) AS item_count,
  o.currency, o.total_amount_source,
  CASE
    WHEN o.currency = 'USD' THEN o.total_amount_source
    WHEN o.currency = 'EUR' THEN o.total_amount_source * fx.eur_usd
    ELSE o.total_amount_source
  END AS total_amount_usd
FROM orders o
LEFT JOIN item_counts ic ON ic.order_id = o.order_id
LEFT JOIN fx ON fx.fx_date = DATE(o.order_ts);
