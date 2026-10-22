CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH successful AS (
    SELECT * FROM stg_payments WHERE payment_status = 'SUCCESS'
), mismatches AS (
    SELECT
        coalesce(o.order_id, p.order_id) AS order_id,
        p.payment_id,
        o.order_amount,
        p.amount_paid,
        o.payment_mode AS order_payment_mode,
        p.payment_mode,
        concat_ws('; ',
            CASE WHEN o.order_id IS NULL THEN 'payment has no matching order' END,
            CASE WHEN o.order_id IS NOT NULL AND o.dq_status = 'FAIL' THEN 'order failed staging quality checks' END,
            CASE WHEN o.order_status <> 'DELIVERED' THEN 'successful payment attached to non-delivered order' END,
            CASE WHEN o.order_id IS NOT NULL AND abs(p.amount_paid - o.order_amount) > 0.005 THEN 'payment amount differs from order_amount' END,
            CASE WHEN o.order_id IS NOT NULL AND p.payment_mode <> o.payment_mode THEN 'payment mode differs from order' END,
            CASE WHEN p.dq_status = 'FAIL' AND p.failure_reason <> '' THEN p.failure_reason END
        ) AS reason
    FROM successful p
    LEFT JOIN stg_orders o ON p.order_id = o.order_id
    WHERE o.order_id IS NULL
       OR o.dq_status = 'FAIL'
       OR o.order_status <> 'DELIVERED'
       OR abs(p.amount_paid - o.order_amount) > 0.005
       OR p.payment_mode <> o.payment_mode
       OR p.dq_status = 'FAIL'
), unpaid AS (
    SELECT
        o.order_id, CAST(NULL AS VARCHAR) AS payment_id,
        o.order_amount, CAST(NULL AS DOUBLE) AS amount_paid,
        o.payment_mode AS order_payment_mode, CAST(NULL AS VARCHAR) AS payment_mode,
        'delivered order has no successful payment' AS reason
    FROM stg_orders o
    LEFT JOIN successful p ON p.order_id = o.order_id
    WHERE o.order_status = 'DELIVERED' AND p.payment_id IS NULL
)
SELECT * FROM mismatches
UNION ALL
SELECT * FROM unpaid;