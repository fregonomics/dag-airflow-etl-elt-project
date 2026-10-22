CREATE TABLE IF NOT EXISTS dq_results (
    run_id VARCHAR,
    dag_id VARCHAR,
    check_name VARCHAR,
    dimension VARCHAR,
    severity VARCHAR,
    rows_checked BIGINT,
    rows_failed BIGINT,
    status VARCHAR,
    checked_at TIMESTAMPTZ
);

DELETE FROM dq_results
WHERE run_id = '__RUN_ID__' AND dag_id = 'nammamart_elt_sushanth';

INSERT INTO dq_results
WITH checks AS (
    SELECT 'orders_required_fields' AS check_name, 'Completeness' AS dimension, 'Critical' AS severity,
           count(*) AS rows_checked,
           count(*) FILTER (WHERE order_id IS NULL OR order_ts IS NULL OR customer_id IS NULL
               OR store_id IS NULL OR product_id IS NULL OR quantity IS NULL OR unit_price IS NULL
               OR discount_pct IS NULL OR order_amount IS NULL OR payment_mode IS NULL OR order_status IS NULL
               OR (order_status = 'DELIVERED' AND delivery_minutes IS NULL)) AS rows_failed
    FROM stg_orders
    UNION ALL
    SELECT 'payments_required_fields', 'Completeness', 'Critical', count(*),
           count(*) FILTER (WHERE payment_id IS NULL OR order_id IS NULL OR payment_ts IS NULL
               OR payment_mode IS NULL OR amount_paid IS NULL OR payment_status IS NULL)
    FROM stg_payments
    UNION ALL
    SELECT 'customers_required_fields', 'Completeness', 'Critical', count(*),
           count(*) FILTER (WHERE customer_id IS NULL)
    FROM stg_customers
    UNION ALL
    SELECT 'products_required_fields', 'Completeness', 'Critical', count(*),
           count(*) FILTER (WHERE product_id IS NULL OR product_name IS NULL OR category IS NULL OR mrp IS NULL OR cost_price IS NULL)
    FROM stg_products
    UNION ALL
    SELECT 'stores_required_fields', 'Completeness', 'Critical', count(*),
           count(*) FILTER (WHERE store_id IS NULL OR store_name IS NULL OR zone IS NULL)
    FROM stg_stores
    UNION ALL
    SELECT 'orders_id_unique', 'Uniqueness', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%duplicate order_id%')
    FROM stg_orders
    UNION ALL
    SELECT 'payments_id_unique', 'Uniqueness', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%duplicate payment_id%')
    FROM stg_payments
    UNION ALL
    SELECT 'customers_id_unique', 'Uniqueness', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%duplicate customer_id%')
    FROM stg_customers
    UNION ALL
    SELECT 'products_id_unique', 'Uniqueness', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%duplicate product_id%')
    FROM stg_products
    UNION ALL
    SELECT 'stores_id_unique', 'Uniqueness', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%duplicate store_id%')
    FROM stg_stores
    UNION ALL
    SELECT 'orders_status_allowed', 'Validity', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%order_status is invalid%')
    FROM stg_orders
    UNION ALL
    SELECT 'orders_payment_mode_allowed', 'Validity', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%payment_mode is invalid%')
    FROM stg_orders
    UNION ALL
    SELECT 'payments_status_allowed', 'Validity', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%payment_status is invalid%')
    FROM stg_payments
    UNION ALL
    SELECT 'customers_email_format', 'Validity', 'Warning', count(*),
           count(*) FILTER (WHERE dq_warning LIKE '%email format is invalid%')
    FROM stg_customers
    UNION ALL
    SELECT 'customers_phone_10_digits', 'Validity', 'Warning', count(*),
           count(*) FILTER (WHERE dq_warning LIKE '%phone must contain 10 digits%')
    FROM stg_customers
    UNION ALL
    SELECT 'customers_pincode_6_digits', 'Validity', 'Warning', count(*),
           count(*) FILTER (WHERE dq_warning LIKE '%pincode must contain 6 digits%')
    FROM stg_customers
    UNION ALL
    SELECT 'orders_quantity_positive', 'Accuracy', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%quantity must be a positive integer%')
    FROM stg_orders
    UNION ALL
    SELECT 'orders_discount_range', 'Accuracy', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%discount_pct must be between%')
    FROM stg_orders
    UNION ALL
    SELECT 'orders_amount_formula', 'Accuracy', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%order_amount does not match%')
    FROM stg_orders
    UNION ALL
    SELECT 'orders_delivery_bounds', 'Accuracy', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%delivery_minutes must be between%'
               OR failure_reason LIKE '%delivered order requires delivery_minutes%')
    FROM stg_orders
    UNION ALL
    SELECT 'orders_delivery_typical_range', 'Accuracy', 'Warning', count(*),
           count(*) FILTER (WHERE dq_warning LIKE '%usual 8-35 minute range%')
    FROM stg_orders
    UNION ALL
    SELECT 'products_mrp_positive', 'Accuracy', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%mrp must be positive%')
    FROM stg_products
    UNION ALL
    SELECT 'products_cost_not_above_mrp', 'Accuracy', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%cost_price exceeds mrp%')
    FROM stg_products
    UNION ALL
    SELECT 'orders_customer_foreign_key', 'Consistency', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%customer_id is missing or invalid%')
    FROM stg_orders
    UNION ALL
    SELECT 'orders_product_foreign_key', 'Consistency', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%product_id is missing or invalid%')
    FROM stg_orders
    UNION ALL
    SELECT 'orders_store_foreign_key', 'Consistency', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%store_id is missing or invalid%')
    FROM stg_orders
    UNION ALL
    SELECT 'payments_order_foreign_key', 'Consistency', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%order_id is missing from staged orders%')
    FROM stg_payments
    UNION ALL
    SELECT 'successful_payment_amount_matches_order', 'Consistency', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%successful payment does not match order_amount%')
    FROM stg_payments
    UNION ALL
    SELECT 'orders_not_future_dated', 'Timeliness', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%order_ts is in the future%')
    FROM stg_orders
    UNION ALL
    SELECT 'payments_not_future_dated', 'Timeliness', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%payment_ts is in the future%')
    FROM stg_payments
    UNION ALL
    SELECT 'customers_not_future_dated', 'Timeliness', 'Critical', count(*),
           count(*) FILTER (WHERE failure_reason LIKE '%signup_date is in the future%')
    FROM stg_customers
    UNION ALL
    SELECT 'orders_latest_timestamp_within_1_day', 'Timeliness', 'Warning', 1,
           CASE WHEN max(order_ts) < current_timestamp - INTERVAL '1 day' THEN 1 ELSE 0 END
    FROM stg_orders
    WHERE dq_status = 'PASS'
    UNION ALL
    SELECT 'orders_volume_drop_90pct', 'Volume', 'Warning', 1,
           CASE WHEN count(*) < 2412 * 0.1 THEN 1 ELSE 0 END
    FROM raw_orders
    WHERE _run_id = '__RUN_ID__' AND _source_file = 'orders.csv'
)
SELECT
    '__RUN_ID__',
    'nammamart_elt_sushanth',
    check_name,
    dimension,
    severity,
    rows_checked,
    rows_failed,
    CASE WHEN rows_failed = 0 THEN 'PASS'
           WHEN severity = 'Warning' THEN 'WARN'
           ELSE 'FAIL' END,
    current_timestamp
FROM checks;