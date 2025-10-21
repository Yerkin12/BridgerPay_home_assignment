-- 1) PK uniqueness
SELECT 'fact_orders_pk_unique' AS test, COUNT(*) AS failures
FROM (
  SELECT order_id
  FROM `bp-nw-next-yerkin.northwind_next.fact_orders`
  GROUP BY order_id
  HAVING COUNT(*) > 1
);

SELECT 'fact_web_events_pk_unique' AS test, COUNT(*) AS failures
FROM (
  SELECT event_id
  FROM `bp-nw-next-yerkin.northwind_next.fact_web_events`
  GROUP BY event_id
  HAVING COUNT(*) > 1
);

-- 2) Not-null key columns
SELECT 'orders_not_null' AS test,
       COUNTIF(order_id IS NULL OR order_ts IS NULL) AS failures
FROM `bp-nw-next-yerkin.northwind_next.fact_orders`;

SELECT 'events_not_null' AS test,
       COUNTIF(event_id IS NULL OR ts IS NULL) AS failures
FROM `bp-nw-next-yerkin.northwind_next.fact_web_events`;

-- 3) Freshness of gold (warn if stale > 2 days)
SELECT 'gold_freshness' AS test,
       COUNTIF(date < DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)) AS stale_days
FROM `bp-nw-next-yerkin.northwind_next.gold_daily_metrics`;
