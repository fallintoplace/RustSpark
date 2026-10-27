WITH buyers AS (
  SELECT customer_id, COUNT(*) AS orders, SUM(amount) AS revenue
  FROM events
  GROUP BY customer_id
)
SELECT COUNT(*) AS customers,
       SUM(CASE WHEN orders >= 2 THEN 1 ELSE 0 END) AS repeat_buyers,
       SUM(CASE WHEN orders >= 2 THEN revenue ELSE 0 END) AS repeat_revenue
FROM buyers;
