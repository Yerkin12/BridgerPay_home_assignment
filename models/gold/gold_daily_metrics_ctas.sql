-- Rebuild-all gold_daily_metrics (no CTE version for Sandbox)
CREATE OR REPLACE TABLE `bp-nw-next-yerkin.northwind_next.gold_daily_metrics`
PARTITION BY date
AS
SELECT
  e.date,
  d.orders,
  d.gmv_usd,
  SAFE_DIVIDE(d.gmv_usd, NULLIF(d.orders,0)) AS aov_usd,
  SAFE_DIVIDE(e.add_to_carts, NULLIF(e.page_views,0)) AS add_to_cart_rate,
  SAFE_DIVIDE(e.purchases,   NULLIF(e.page_views,0))  AS conversion_rate
FROM (
  SELECT
    DATE(ts) AS date,
    COUNTIF(event = 'page_view')   AS page_views,
    COUNTIF(event = 'add_to_cart') AS add_to_carts,
    COUNTIF(event = 'purchase')    AS purchases
  FROM `bp-nw-next-yerkin.northwind_next.fact_web_events`
  GROUP BY 1
) AS e
LEFT JOIN (
  SELECT
    DATE(order_ts) AS date,
    COUNTIF(status != 'cancelled') AS orders,
    SUM(total_amount_usd)          AS gmv_usd
  FROM `bp-nw-next-yerkin.northwind_next.fact_orders`
  GROUP BY 1
) AS d
USING (date);
