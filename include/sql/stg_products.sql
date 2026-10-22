CREATE OR REPLACE TABLE stg_products AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY _source_file, _run_id, product_id ORDER BY _loaded_at DESC) AS source_rank,
           count(*) OVER (PARTITION BY _source_file, _run_id, product_id) AS source_key_count
    FROM raw_products
), latest AS (
    SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY _loaded_at DESC, _run_id DESC) AS latest_rank
    FROM ranked WHERE source_rank = 1
)
SELECT
    nullif(trim(product_id), '') AS product_id,
    nullif(trim(product_name), '') AS product_name,
    nullif(trim(category), '') AS category,
    nullif(trim(sub_category), '') AS sub_category,
    nullif(trim(brand), '') AS brand,
    try_cast(nullif(trim(mrp), '') AS DOUBLE) AS mrp,
    try_cast(nullif(trim(cost_price), '') AS DOUBLE) AS cost_price,
    lower(trim(unit)) AS unit,
    lower(trim(is_perishable)) IN ('true', '1', 'yes', 'y') AS is_perishable,
    try_cast(nullif(trim(launch_date), '') AS DATE) AS launch_date,
    CASE WHEN coalesce(trim(product_id), '') = '' THEN 'FAIL'
         WHEN source_key_count > 1 THEN 'FAIL'
         WHEN nullif(trim(category), '') IS NULL THEN 'FAIL'
         WHEN try_cast(mrp AS DOUBLE) IS NULL OR try_cast(mrp AS DOUBLE) <= 0 THEN 'FAIL'
         WHEN try_cast(cost_price AS DOUBLE) IS NULL THEN 'FAIL'
         WHEN try_cast(cost_price AS DOUBLE) > try_cast(mrp AS DOUBLE) THEN 'FAIL'
         ELSE 'PASS' END AS dq_status,
    concat_ws('; ',
        CASE WHEN coalesce(trim(product_id), '') = '' THEN 'product_id is required' END,
        CASE WHEN source_key_count > 1 THEN 'duplicate product_id in source file' END,
        CASE WHEN nullif(trim(category), '') IS NULL THEN 'category is required' END,
        CASE WHEN try_cast(mrp AS DOUBLE) IS NULL OR try_cast(mrp AS DOUBLE) <= 0 THEN 'mrp must be positive and numeric' END,
        CASE WHEN try_cast(cost_price AS DOUBLE) IS NULL THEN 'cost_price must be numeric' END,
        CASE WHEN try_cast(cost_price AS DOUBLE) > try_cast(mrp AS DOUBLE) THEN 'cost_price exceeds mrp' END
    ) AS failure_reason,
    CAST(NULL AS VARCHAR) AS dq_warning,
    _loaded_at, _source_file, _run_id
FROM latest WHERE latest_rank = 1;