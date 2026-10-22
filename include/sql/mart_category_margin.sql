CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    p.category,
    sum(o.order_amount) AS net_revenue,
    sum(o.order_amount - o.quantity * p.cost_price) AS gross_margin
FROM stg_orders o
JOIN stg_products p USING (product_id)
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
  AND p.dq_status = 'PASS'
GROUP BY p.category
ORDER BY p.category;