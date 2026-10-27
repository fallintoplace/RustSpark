WITH first_seen AS (
  SELECT user_id, MIN(week_id) AS first_week
  FROM events
  GROUP BY user_id
),
weekly_activity AS (
  SELECT DISTINCT user_id, week_id
  FROM events
)
SELECT a.week_id,
       COUNT(*) AS active_users,
       SUM(CASE WHEN a.week_id = f.first_week THEN 1 ELSE 0 END) AS new_users,
       SUM(CASE WHEN a.week_id > f.first_week THEN 1 ELSE 0 END) AS returning_users
FROM weekly_activity a
JOIN first_seen f ON a.user_id = f.user_id
GROUP BY a.week_id
ORDER BY a.week_id;
