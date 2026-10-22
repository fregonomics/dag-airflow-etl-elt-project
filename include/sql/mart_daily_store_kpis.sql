CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    s.store_id,
    s.store_name,
    s.zone,
    count(*) FILTER (WHERE o.order_status = 'DELIVERED') AS delivered_orders,
    sum(o.order_amount) FILTER (WHERE o.order_status = 'DELIVERED') AS net_revenue
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.dq_status = 'PASS'
    AND s.dq_status = 'PASS'
GROUP BY o.order_date, s.store_id, s.store_name, s.zone
ORDER BY o.order_date, s.store_id;