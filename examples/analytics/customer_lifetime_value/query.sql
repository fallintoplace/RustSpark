WITH customer_totals AS (
  SELECT customer_id, region,
         COUNT(*) AS orders,
         SUM(amount) AS lifetime_revenue
  FROM events
  GROUP BY customer_id, region
)
SELECT region,
       COUNT(*) AS customers,
       SUM(lifetime_revenue) AS revenue,
       MAX(lifetime_revenue) AS highest_customer_value
FROM customer_totals
GROUP BY region
ORDER BY region;
