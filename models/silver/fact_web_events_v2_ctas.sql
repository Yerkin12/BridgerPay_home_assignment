-- Rebuild-all fact_web_events_v2 with dedup and lateness, CTAS
CREATE OR REPLACE TABLE `bp-nw-next-yerkin.northwind_next.fact_web_events_v2`
PARTITION BY DATE(ts) AS
WITH base AS (
  SELECT
    CAST(event_id AS STRING)                AS event_id,
    TIMESTAMP(CAST(ts AS STRING))           AS ts,
    CAST(customer_id AS STRING)             AS customer_id,
    CAST(event AS STRING)                   AS event,
    CAST(sku AS STRING)                     AS sku,
    CAST(device AS STRING)                  AS device,
    TIMESTAMP_SUB(
      TIMESTAMP(DATE_ADD(CAST(load_day AS DATE), INTERVAL 1 DAY)),
      INTERVAL 1 SECOND
    ) AS load_ts
  FROM `bp-nw-next-yerkin.northwind_next.bronze_web_events_raw_v2`
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ts ASC) = 1
)
SELECT
  event_id, ts, customer_id, event, sku, device, load_ts,
  TIMESTAMP_DIFF(load_ts, ts, HOUR) AS delay_h
FROM dedup;
