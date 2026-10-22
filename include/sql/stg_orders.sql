CREATE TABLE IF NOT EXISTS stg_orders AS
SELECT
    CAST(NULL AS VARCHAR) AS order_id,
    CAST(NULL AS TIMESTAMP) AS order_ts,
    CAST(NULL AS DATE) AS order_date,
    CAST(NULL AS VARCHAR) AS customer_id,
    CAST(NULL AS VARCHAR) AS store_id,
    CAST(NULL AS VARCHAR) AS product_id,
    CAST(NULL AS INTEGER) AS quantity,
    CAST(NULL AS DOUBLE) AS unit_price,
    CAST(NULL AS DOUBLE) AS discount_pct,
    CAST(NULL AS DOUBLE) AS order_amount,
    CAST(NULL AS VARCHAR) AS payment_mode,
    CAST(NULL AS VARCHAR) AS order_status,
    CAST(NULL AS VARCHAR) AS channel,
    CAST(NULL AS DOUBLE) AS delivery_minutes,
    CAST(NULL AS VARCHAR) AS dq_status,
    CAST(NULL AS VARCHAR) AS failure_reason,
    CAST(NULL AS VARCHAR) AS dq_warning,
    CAST(NULL AS VARCHAR) AS _loaded_at,
    CAST(NULL AS VARCHAR) AS _source_file,
    CAST(NULL AS VARCHAR) AS _run_id
WHERE FALSE;

MERGE INTO stg_orders AS target
USING (
    WITH versions AS (
        SELECT *,
               row_number() OVER (PARTITION BY _source_file, _run_id, order_id ORDER BY _loaded_at DESC) AS source_rank,
               count(*) OVER (PARTITION BY _source_file, _run_id, order_id) AS source_key_count
        FROM raw_orders
    ),
    snapshot_rows AS (
        SELECT * FROM versions WHERE source_rank = 1
    ),
    preferred AS (
        SELECT *, row_number() OVER (
            PARTITION BY order_id
            ORDER BY CASE WHEN _source_file = 'orders_2026-10-01_increment.csv' THEN 0 ELSE 1 END,
                     _loaded_at DESC, _run_id DESC
        ) AS latest_rank
        FROM snapshot_rows
    ),
    typed AS (
        SELECT *, try_cast(nullif(trim(order_ts), '') AS TIMESTAMP) AS parsed_ts,
                  try_cast(nullif(trim(quantity), '') AS INTEGER) AS parsed_quantity,
                  try_cast(nullif(trim(unit_price), '') AS DOUBLE) AS parsed_price,
                  try_cast(nullif(trim(discount_pct), '') AS DOUBLE) AS parsed_discount,
                  try_cast(nullif(trim(order_amount), '') AS DOUBLE) AS parsed_amount,
                  try_cast(nullif(trim(delivery_minutes), '') AS DOUBLE) AS parsed_delivery
        FROM preferred WHERE latest_rank = 1
    ),
    checked AS (
        SELECT typed.*,
               concat_ws('; ',
                   CASE WHEN coalesce(trim(order_id), '') = '' THEN 'order_id is required' END,
                   CASE WHEN source_key_count > 1 THEN 'duplicate order_id in source file' END,
                   CASE WHEN parsed_ts IS NULL THEN 'order_ts is missing or invalid' END,
                   CASE WHEN parsed_ts > current_timestamp THEN 'order_ts is in the future' END,
                   CASE WHEN parsed_quantity IS NULL OR parsed_quantity <= 0 THEN 'quantity must be a positive integer' END,
                   CASE WHEN parsed_price IS NULL OR parsed_price <= 0 THEN 'unit_price must be positive' END,
                   CASE WHEN parsed_discount IS NULL OR parsed_discount NOT BETWEEN 0 AND 100 THEN 'discount_pct must be between 0 and 100' END,
                   CASE WHEN parsed_amount IS NULL OR abs(parsed_amount - round(parsed_quantity * parsed_price * (1 - parsed_discount / 100), 2)) > 0.005 THEN 'order_amount does not match pricing formula' END,
                   CASE WHEN coalesce(upper(trim(order_status)), '') NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'order_status is invalid' END,
                   CASE WHEN coalesce(lower(trim(payment_mode)), '') NOT IN ('upi', 'card', 'cash on delivery', 'cod', 'wallet') THEN 'payment_mode is invalid' END,
                   CASE WHEN upper(trim(order_status)) = 'DELIVERED' AND parsed_delivery IS NULL THEN 'delivered order requires delivery_minutes' END,
                   CASE WHEN parsed_delivery < 0 OR parsed_delivery >= 120 THEN 'delivery_minutes must be between 0 and 119' END,
                   CASE WHEN NOT EXISTS (SELECT 1 FROM stg_customers c WHERE c.customer_id = trim(typed.customer_id) AND c.dq_status = 'PASS') THEN 'customer_id is missing or invalid' END,
                   CASE WHEN NOT EXISTS (SELECT 1 FROM stg_products p WHERE p.product_id = trim(typed.product_id) AND p.dq_status = 'PASS') THEN 'product_id is missing or invalid' END,
                   CASE WHEN NOT EXISTS (SELECT 1 FROM stg_stores s WHERE s.store_id = trim(typed.store_id) AND s.dq_status = 'PASS') THEN 'store_id is missing or invalid' END
               ) AS failure_reason
        FROM typed
    )
    SELECT
        trim(order_id) AS order_id,
        parsed_ts AS order_ts,
        CAST(parsed_ts AS DATE) AS order_date,
        trim(customer_id) AS customer_id,
        trim(store_id) AS store_id,
        trim(product_id) AS product_id,
        parsed_quantity AS quantity,
        parsed_price AS unit_price,
        parsed_discount AS discount_pct,
        parsed_amount AS order_amount,
        CASE lower(regexp_replace(trim(payment_mode), '\\s+', ' ', 'g'))
            WHEN 'upi' THEN 'UPI'
            WHEN 'card' THEN 'Card'
            WHEN 'cash on delivery' THEN 'Cash on Delivery'
            WHEN 'cod' THEN 'Cash on Delivery'
            WHEN 'wallet' THEN 'Wallet'
            ELSE trim(payment_mode)
        END AS payment_mode,
        upper(trim(order_status)) AS order_status,
        CASE upper(trim(channel))
            WHEN 'APP' THEN 'App'
            WHEN 'WEB' THEN 'Web'
            ELSE trim(channel)
        END AS channel,
        parsed_delivery AS delivery_minutes,
        CASE WHEN failure_reason = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
        failure_reason,
        CASE WHEN parsed_delivery BETWEEN 0 AND 7 OR parsed_delivery BETWEEN 36 AND 119
             THEN 'delivery time is outside the usual 8-35 minute range' END AS dq_warning,
        _loaded_at, _source_file, _run_id
    FROM checked
) AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET
    order_ts = source.order_ts,
    order_date = source.order_date,
    customer_id = source.customer_id,
    store_id = source.store_id,
    product_id = source.product_id,
    quantity = source.quantity,
    unit_price = source.unit_price,
    discount_pct = source.discount_pct,
    order_amount = source.order_amount,
    payment_mode = source.payment_mode,
    order_status = source.order_status,
    channel = source.channel,
    delivery_minutes = source.delivery_minutes,
    dq_status = source.dq_status,
    failure_reason = source.failure_reason,
    dq_warning = source.dq_warning,
    _loaded_at = source._loaded_at,
    _source_file = source._source_file,
    _run_id = source._run_id
WHEN NOT MATCHED THEN INSERT BY NAME;