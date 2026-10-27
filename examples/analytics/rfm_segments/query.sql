SELECT customer_id,
       100 - MAX(day) AS days_since_purchase,
       COUNT(*) AS orders,
       SUM(amount) AS lifetime_revenue,
       CASE WHEN MAX(day) >= 90 AND COUNT(*) >= 2 THEN 'loyal'
            WHEN MAX(day) < 80 THEN 'at_risk'
            ELSE 'active'
       END AS segment
FROM events
GROUP BY customer_id
ORDER BY customer_id;
