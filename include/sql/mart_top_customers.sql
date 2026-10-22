CREATE OR REPLACE TABLE mart_top_customers AS
SELECT customer_id, loyalty_tier, total_spend
FROM (
    SELECT
        o.customer_id,
        c.loyalty_tier,
        sum(o.order_amount) AS total_spend,
        row_number() OVER (ORDER BY sum(o.order_amount) DESC, o.customer_id) AS spend_rank
    FROM stg_orders o
    JOIN stg_customers c USING (customer_id)
    WHERE o.dq_status = 'PASS'
      AND o.order_status = 'DELIVERED'
      AND c.dq_status = 'PASS'
    GROUP BY o.customer_id, c.loyalty_tier
)
WHERE spend_rank <= 10
ORDER BY total_spend DESC;