WITH daily AS (
  SELECT day, SUM(amount) AS daily_revenue
  FROM events
  GROUP BY day
)
SELECT day, daily_revenue,
       SUM(daily_revenue) OVER (
         ORDER BY day ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
       ) AS trailing_three_days
FROM daily
ORDER BY day;
