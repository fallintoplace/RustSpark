WITH ranked AS (
  SELECT user_id, channel,
         ROW_NUMBER() OVER (
           PARTITION BY user_id ORDER BY day, event_name, channel
         ) AS position
  FROM events
),
purchase_totals AS (
  SELECT user_id, SUM(amount) AS purchase_revenue
  FROM events
  WHERE event_name = 'purchase'
  GROUP BY user_id
)
SELECT f.channel,
       COUNT(*) AS acquired_users,
       SUM(COALESCE(p.purchase_revenue, 0)) AS attributed_revenue
FROM ranked f
LEFT JOIN purchase_totals p ON f.user_id = p.user_id
WHERE f.position = 1
GROUP BY f.channel
ORDER BY f.channel;
