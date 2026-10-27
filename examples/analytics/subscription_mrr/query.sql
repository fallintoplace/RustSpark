SELECT month,
       COUNT(*) AS paying_subscribers,
       SUM(monthly_fee) AS monthly_recurring_revenue
FROM events
WHERE is_active = 1
GROUP BY month
ORDER BY month;
