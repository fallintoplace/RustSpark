WITH user_steps AS (
  SELECT user_id,
         MAX(CASE WHEN event_name = 'visit' THEN 1 ELSE 0 END) AS visited,
         MAX(CASE WHEN event_name = 'signup' THEN 1 ELSE 0 END) AS signed_up,
         MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS purchased
  FROM events
  GROUP BY user_id
)
SELECT SUM(visited) AS visitors,
       SUM(CASE WHEN visited = 1 AND signed_up = 1 THEN 1 ELSE 0 END) AS signups,
       SUM(CASE WHEN visited = 1 AND signed_up = 1 AND purchased = 1
                THEN 1 ELSE 0 END) AS purchasers
FROM user_steps;
