-- Build SCD2 dimension for products from daily snapshots (CTAS)
CREATE OR REPLACE TABLE `bp-nw-next-yerkin.northwind_next.dim_product`
PARTITION BY valid_from AS
WITH base AS (
  SELECT
    sku, title, category,
    CAST(unit_cost AS NUMERIC) AS unit_cost,
    CAST(active_flag AS BOOL)  AS active_flag,
    DATE(snapshot_date)        AS snapshot_date
  FROM `bp-nw-next-yerkin.northwind_next.bronze_catalog_raw`
  WHERE sku IS NOT NULL AND snapshot_date IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY sku, snapshot_date
    ORDER BY sku
  ) = 1
),
sequenced AS (
  SELECT
    sku, title, category, unit_cost, active_flag,
    snapshot_date AS valid_from,
    LEAD(snapshot_date) OVER (PARTITION BY sku ORDER BY snapshot_date) AS next_day
  FROM dedup
)
SELECT
  sku, title, category, unit_cost, active_flag,
  valid_from,
  IFNULL(DATE_SUB(next_day, INTERVAL 1 DAY), DATE '9999-12-31') AS valid_to,
  next_day IS NULL AS is_current
FROM sequenced;
