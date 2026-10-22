CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    s.store_id,
    s.store_name,
    s.zone,
    avg(o.delivery_minutes) AS average_delivery_minutes,
    count(*) FILTER (WHERE o.delivery_minutes <= 15) AS delivered_within_15_minutes,
    count(*) AS delivered_orders,
    100.0 * count(*) FILTER (WHERE o.delivery_minutes <= 15) / nullif(count(*), 0) AS pct_within_15_minutes
FROM stg_orders o
JOIN stg_stores s USING (store_id)
WHERE o.dq_status = 'PASS'
  AND o.order_status = 'DELIVERED'
  AND s.dq_status = 'PASS'
GROUP BY s.store_id, s.store_name, s.zone
ORDER BY s.store_id;