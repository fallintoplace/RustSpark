SELECT customer_id,
       MAX(day) AS last_active_day,
       100 - MAX(day) AS days_inactive,
       CASE WHEN 100 - MAX(day) >= 30 THEN 1 ELSE 0 END AS is_dormant
FROM events
GROUP BY customer_id
ORDER BY customer_id;
