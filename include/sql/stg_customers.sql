CREATE OR REPLACE TABLE stg_customers AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY _source_file, _run_id, customer_id ORDER BY _loaded_at DESC) AS source_rank,
           count(*) OVER (PARTITION BY _source_file, _run_id, customer_id) AS source_key_count
    FROM raw_customers
), latest AS (
    SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY _loaded_at DESC, _run_id DESC) AS latest_rank
    FROM ranked
    WHERE source_rank = 1
)
SELECT
    nullif(trim(customer_id), '') AS customer_id,
    CASE WHEN nullif(trim(full_name), '') IS NULL THEN NULL ELSE '***' END AS full_name,
        CASE WHEN strpos(coalesce(trim(email), ''), '@') > 0
            THEN left(trim(email), 2) || '***@' || split_part(trim(email), '@', 2)
            WHEN coalesce(trim(email), '') = '' THEN '' ELSE '***' END AS email,
        CASE WHEN regexp_full_match(coalesce(trim(phone), ''), '[0-9]{10}')
            THEN left(trim(phone), 2) || '******' || right(trim(phone), 2)
            WHEN coalesce(trim(phone), '') = '' THEN '' ELSE '**********' END AS phone,
    CASE WHEN nullif(trim(city), '') IS NULL THEN NULL ELSE '***' END AS city,
    CASE WHEN nullif(trim(area), '') IS NULL THEN NULL ELSE '***' END AS area,
    CASE WHEN nullif(trim(pincode), '') IS NULL THEN NULL ELSE '******' END AS pincode,
    try_cast(nullif(trim(signup_date), '') AS DATE) AS signup_date,
    CASE upper(trim(loyalty_tier))
        WHEN 'BRONZE' THEN 'Bronze'
        WHEN 'SILVER' THEN 'Silver'
        WHEN 'GOLD' THEN 'Gold'
        WHEN 'PLATINUM' THEN 'Platinum'
        ELSE trim(loyalty_tier)
    END AS loyalty_tier,
    upper(trim(is_active)) AS is_active,
    CASE WHEN coalesce(trim(customer_id), '') = '' THEN 'FAIL'
         WHEN source_key_count > 1 THEN 'FAIL'
         WHEN try_cast(signup_date AS DATE) > current_date THEN 'FAIL'
         ELSE 'PASS' END AS dq_status,
    concat_ws('; ',
        CASE WHEN coalesce(trim(customer_id), '') = '' THEN 'customer_id is required' END,
        CASE WHEN source_key_count > 1 THEN 'duplicate customer_id in source file' END,
        CASE WHEN try_cast(signup_date AS DATE) > current_date THEN 'signup_date is in the future' END
    ) AS failure_reason,
    concat_ws('; ',
        CASE WHEN coalesce(trim(email), '') <> '' AND NOT regexp_full_match(trim(email), '[^[:space:]@]+@[^[:space:]@]+[.][^[:space:]@]+')
             THEN 'email format is invalid' END,
        CASE WHEN coalesce(trim(phone), '') <> '' AND NOT regexp_full_match(trim(phone), '[0-9]{10}')
             THEN 'phone must contain 10 digits' END,
        CASE WHEN coalesce(trim(pincode), '') <> '' AND NOT regexp_full_match(trim(pincode), '[0-9]{6}')
             THEN 'pincode must contain 6 digits' END
    ) AS dq_warning,
    _loaded_at, _source_file, _run_id
FROM latest
WHERE latest_rank = 1;