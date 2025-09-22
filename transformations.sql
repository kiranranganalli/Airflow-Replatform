
-- transformations.sql
WITH cleaned AS (
  SELECT
    order_id,
    customer_id,
    CAST(order_ts AS TIMESTAMP) AS order_ts,
    UPPER(status) AS status,
    CAST(amount_usd AS NUMERIC) AS amount_usd
  FROM raw.orders
),
enriched AS (
  SELECT c.segment, cl.*
  FROM cleaned cl
  LEFT JOIN dim.customers c ON c.customer_id = cl.customer_id
),
daily_revenue AS (
  SELECT
    DATE(order_ts) AS ds,
    segment,
    SUM(amount_usd) AS revenue_usd,
    COUNT(*) AS orders
  FROM enriched
  WHERE status IN ('PLACED','DELIVERED','SHIPPED')
  GROUP BY 1,2
)
INSERT INTO mart.daily_revenue (ds, segment, revenue_usd, orders)
SELECT ds, segment, revenue_usd, orders
FROM daily_revenue
ON CONFLICT (ds, segment)
DO UPDATE SET
  revenue_usd = EXCLUDED.revenue_usd,
  orders = EXCLUDED.orders;
