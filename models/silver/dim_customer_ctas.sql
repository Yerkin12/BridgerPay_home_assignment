-- Build SCD2 dimension for customers using CTAS (Sandbox-friendly, no DML)
CREATE OR REPLACE TABLE `bp-nw-next-yerkin.northwind_next.dim_customer`
PARTITION BY DATE(valid_from) AS
WITH base AS (
  SELECT
    customer_id,
    status,
    country,
    TIMESTAMP(op_ts) AS op_ts
  FROM `bp-nw-next-yerkin.northwind_next.bronze_opdb_raw`
  WHERE table = 'customers' AND customer_id IS NOT NULL AND op_ts IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY customer_id, op_ts
    ORDER BY customer_id, op_ts DESC
  ) = 1
),
sequenced AS (
  SELECT
    customer_id, status, country,
    op_ts AS valid_from,
    LEAD(op_ts) OVER (PARTITION BY customer_id ORDER BY op_ts) AS next_ts
  FROM dedup
)
SELECT
  customer_id,
  status,
  country,
  valid_from,
  IFNULL(TIMESTAMP_SUB(next_ts, INTERVAL 1 SECOND),
         TIMESTAMP('9999-12-31 00:00:00')) AS valid_to,
  next_ts IS NULL AS is_current
FROM sequenced;
