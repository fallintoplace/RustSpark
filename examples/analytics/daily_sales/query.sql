SELECT day,
       COUNT(*) AS paid_orders,
       SUM(amount) AS paid_revenue
FROM events
WHERE status = 'paid'
GROUP BY day
ORDER BY day;
