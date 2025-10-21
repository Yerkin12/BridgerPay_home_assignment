-- Conversion funnel (last 14 days)
WITH agg AS (
  SELECT event, COUNT(*) AS cnt
  FROM `bp-nw-next-yerkin.northwind_next.fact_web_events`
  WHERE ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
  GROUP BY event
)
SELECT
  (SELECT cnt FROM agg WHERE event='page_view')      AS page_views,
  (SELECT cnt FROM agg WHERE event='add_to_cart')    AS add_to_cart,
  (SELECT cnt FROM agg WHERE event='checkout_start') AS checkout_start,
  (SELECT cnt FROM agg WHERE event='purchase')       AS purchase,
  SAFE_DIVIDE((SELECT cnt FROM agg WHERE event='add_to_cart'),
              NULLIF((SELECT cnt FROM agg WHERE event='page_view'),0)) AS add_to_cart_rate,
  SAFE_DIVIDE((SELECT cnt FROM agg WHERE event='purchase'),
              NULLIF((SELECT cnt FROM agg WHERE event='page_view'),0)) AS conversion_rate;
