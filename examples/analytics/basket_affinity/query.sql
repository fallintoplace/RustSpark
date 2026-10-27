SELECT a.product AS product_a,
       b.product AS product_b,
       COUNT(*) AS shared_orders
FROM events a
JOIN events b
  ON a.order_id = b.order_id
 AND a.product < b.product
GROUP BY a.product, b.product
ORDER BY shared_orders DESC, product_a, product_b;
