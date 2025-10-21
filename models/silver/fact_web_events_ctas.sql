-- Rebuild-all fact_web_events with dedup (earliest ts), CTAS
CREATE OR REPLACE TABLE `bp-nw-next-yerkin.northwind_next.fact_web_events`
PARTITION BY DATE(ts) AS
WITH base AS (
  SELECT
    CAST(event_id AS STRING) AS event_id,
    TIMESTAMP(CAST(ts AS STRING)) AS ts,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(event AS STRING) AS event,
    CAST(sku AS STRING) AS sku,
    CAST(device AS STRING) AS device
  FROM `bp-nw-next-yerkin.northwind_next.bronze_web_events_raw`
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts ASC) = 1
)
SELECT * FROM dedup;
