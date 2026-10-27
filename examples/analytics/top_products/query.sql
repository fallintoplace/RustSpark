WITH product_totals AS (
  SELECT category, product, SUM(units * unit_price) AS revenue
  FROM events
  GROUP BY category, product
),
ranked AS (
  SELECT category, product, revenue,
         ROW_NUMBER() OVER (
           PARTITION BY category ORDER BY revenue DESC, product
         ) AS position
  FROM product_totals
)
SELECT category, product, revenue, position
FROM ranked
WHERE position <= 2
ORDER BY category, position;
