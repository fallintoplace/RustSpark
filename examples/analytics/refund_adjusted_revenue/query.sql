WITH monthly AS (
  SELECT month,
         SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS gross_revenue,
         SUM(CASE WHEN status IN ('refunded', 'chargeback')
                  THEN amount ELSE 0 END) AS deductions
  FROM events
  GROUP BY month
)
SELECT month, gross_revenue, deductions,
       gross_revenue - deductions AS net_revenue
FROM monthly
ORDER BY month;
