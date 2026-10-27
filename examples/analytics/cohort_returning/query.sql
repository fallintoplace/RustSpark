WITH cohorts AS (
  SELECT customer_id, MIN(month) AS cohort
  FROM events
  GROUP BY customer_id
),
returning AS (
  SELECT DISTINCT c.customer_id, c.cohort
  FROM cohorts c
  JOIN events e ON c.customer_id = e.customer_id
  WHERE e.month > c.cohort
)
SELECT c.cohort,
       COUNT(*) AS cohort_size,
       COUNT(r.customer_id) AS returned
FROM cohorts c
LEFT JOIN returning r ON c.customer_id = r.customer_id
GROUP BY c.cohort
ORDER BY c.cohort;
