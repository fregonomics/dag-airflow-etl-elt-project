CREATE OR REPLACE TABLE stg_payments AS
WITH versions AS (
    SELECT *,
           row_number() OVER (PARTITION BY _source_file, _run_id, payment_id ORDER BY _loaded_at DESC) AS source_rank,
           count(*) OVER (PARTITION BY _source_file, _run_id, payment_id) AS source_key_count
    FROM raw_payments
), latest AS (
    SELECT *, row_number() OVER (PARTITION BY payment_id ORDER BY _loaded_at DESC, _run_id DESC) AS latest_rank
    FROM versions WHERE source_rank = 1
), typed AS (
    SELECT *, try_cast(nullif(trim(payment_ts), '') AS TIMESTAMP) AS parsed_ts,
              try_cast(nullif(trim(amount_paid), '') AS DOUBLE) AS parsed_amount
    FROM latest WHERE latest_rank = 1
), checked AS (
    SELECT typed.*,
           concat_ws('; ',
               CASE WHEN coalesce(trim(payment_id), '') = '' THEN 'payment_id is required' END,
               CASE WHEN source_key_count > 1 THEN 'duplicate payment_id in source file' END,
               CASE WHEN parsed_ts IS NULL THEN 'payment_ts is missing or invalid' END,
               CASE WHEN parsed_ts > current_timestamp THEN 'payment_ts is in the future' END,
               CASE WHEN parsed_amount IS NULL THEN 'amount_paid is missing or invalid' END,
               CASE WHEN coalesce(upper(trim(payment_status)), '') NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'payment_status is invalid' END,
               CASE WHEN NOT EXISTS (SELECT 1 FROM stg_orders o WHERE o.order_id = trim(typed.order_id)) THEN 'order_id is missing from staged orders' END,
               CASE WHEN upper(trim(payment_status)) = 'SUCCESS' AND EXISTS (
                   SELECT 1 FROM stg_orders o
                   WHERE o.order_id = trim(typed.order_id)
                     AND abs(try_cast(typed.amount_paid AS DOUBLE) - o.order_amount) > 0.005
               ) THEN 'successful payment does not match order_amount' END
           ) AS failure_reason
    FROM typed
)
SELECT
    trim(payment_id) AS payment_id,
    trim(order_id) AS order_id,
    parsed_ts AS payment_ts,
    CASE lower(regexp_replace(trim(payment_mode), '\\s+', ' ', 'g'))
        WHEN 'upi' THEN 'UPI' WHEN 'card' THEN 'Card'
        WHEN 'cash on delivery' THEN 'Cash on Delivery'
        WHEN 'cod' THEN 'Cash on Delivery' WHEN 'wallet' THEN 'Wallet'
        ELSE trim(payment_mode) END AS payment_mode,
    parsed_amount AS amount_paid,
    upper(trim(payment_status)) AS payment_status,
    nullif(trim(gateway_ref), '') AS gateway_ref,
    CASE WHEN failure_reason = '' THEN 'PASS' ELSE 'FAIL' END AS dq_status,
    failure_reason,
    CAST(NULL AS VARCHAR) AS dq_warning,
    _loaded_at, _source_file, _run_id
FROM checked;