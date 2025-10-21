-- Daily breakdown of late events impact (>6h)
CREATE OR REPLACE TABLE `bp-nw-next-yerkin.northwind_next.gold_daily_late_impact`
PARTITION BY date AS
SELECT
  DATE(ts) AS date,
  COUNT(*) AS events,
  COUNTIF(delay_h > 6) AS late_events,
  COUNTIF(event='purchase') AS purchases,
  COUNTIF(event='purchase' AND delay_h > 6) AS late_purchases,
  SAFE_DIVIDE(COUNTIF(delay_h > 6), COUNT(*)) AS share_late_events,
  SAFE_DIVIDE(COUNTIF(event='purchase' AND delay_h > 6),
              NULLIF(COUNTIF(event='purchase'),0)) AS share_late_purchases
FROM `bp-nw-next-yerkin.northwind_next.fact_web_events_v2`
GROUP BY date;
