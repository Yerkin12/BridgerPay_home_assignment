-- Share of late events (>6h) and daily share of late purchases
WITH agg AS (
  SELECT
    COUNT(*) AS total_events,
    COUNTIF(delay_h > 6) AS late_events
  FROM `bp-nw-next-yerkin.northwind_next.fact_web_events_v2`
),
impact AS (
  SELECT
    DATE(ts) AS date,
    COUNTIF(event = 'purchase') AS purchases,
    COUNTIF(event = 'purchase' AND delay_h > 6) AS late_purchases
  FROM `bp-nw-next-yerkin.northwind_next.fact_web_events_v2`
  GROUP BY date
)
SELECT
  SAFE_DIVIDE((SELECT late_events FROM agg), (SELECT total_events FROM agg)) AS share_late_events,
  AVG(SAFE_DIVIDE(late_purchases, NULLIF(purchases,0))) AS avg_daily_late_purchase_share
FROM impact;
