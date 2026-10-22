CREATE OR REPLACE TABLE mart_dq_rejections AS
WITH rejected AS (
    SELECT 'orders.csv' AS file_name, order_id AS row_key, failure_reason, _run_id FROM stg_orders WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'payments.csv', payment_id, failure_reason, _run_id FROM stg_payments WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'customers.csv', customer_id, failure_reason, _run_id FROM stg_customers WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'products.csv', product_id, failure_reason, _run_id FROM stg_products WHERE dq_status = 'FAIL'
    UNION ALL
    SELECT 'stores.csv', store_id, failure_reason, _run_id FROM stg_stores WHERE dq_status = 'FAIL'
)
SELECT file_name, row_key, trim(reason) AS failure_reason, _run_id AS run_id
FROM rejected
CROSS JOIN UNNEST(string_split(failure_reason, '; ')) AS split(reason)
WHERE trim(reason) <> '';