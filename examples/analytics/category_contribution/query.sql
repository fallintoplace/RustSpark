WITH category_totals AS (
  SELECT category, SUM(units * unit_price) AS revenue
  FROM events
  GROUP BY category
)
SELECT category,
       revenue,
       SUM(revenue) OVER () AS overall_revenue,
       ROW_NUMBER() OVER (ORDER BY revenue DESC, category) AS revenue_rank
FROM category_totals
ORDER BY revenue_rank;
