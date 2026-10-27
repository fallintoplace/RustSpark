SELECT sku, day, delta,
       SUM(delta) OVER (
         PARTITION BY sku
         ORDER BY day
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
       ) AS stock_on_hand
FROM events
ORDER BY sku, day;
