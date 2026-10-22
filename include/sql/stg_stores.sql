CREATE OR REPLACE TABLE stg_stores AS
WITH ranked AS (
    SELECT *,
           row_number() OVER (PARTITION BY _source_file, _run_id, store_id ORDER BY _loaded_at DESC) AS source_rank,
           count(*) OVER (PARTITION BY _source_file, _run_id, store_id) AS source_key_count
    FROM raw_stores
), latest AS (
    SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY _loaded_at DESC, _run_id DESC) AS latest_rank
    FROM ranked WHERE source_rank = 1
)
SELECT
    nullif(trim(store_id), '') AS store_id,
    nullif(trim(store_name), '') AS store_name,
    CASE upper(trim(zone))
        WHEN 'NORTH' THEN 'North'
        WHEN 'SOUTH' THEN 'South'
        WHEN 'EAST' THEN 'East'
        WHEN 'WEST' THEN 'West'
        ELSE trim(zone)
    END AS zone,
    nullif(trim(area), '') AS area,
    CASE WHEN nullif(trim(pincode), '') IS NULL THEN NULL ELSE '******' END AS pincode,
    try_cast(nullif(trim(open_date), '') AS DATE) AS open_date,
    CASE WHEN nullif(trim(manager_name), '') IS NULL THEN NULL ELSE '***' END AS manager_name,
    try_cast(nullif(trim(capacity_orders_per_day), '') AS INTEGER) AS capacity_orders_per_day,
    CASE WHEN coalesce(trim(store_id), '') = '' THEN 'FAIL'
         WHEN source_key_count > 1 THEN 'FAIL'
         WHEN coalesce(trim(zone), '') = '' OR upper(trim(zone)) NOT IN ('NORTH', 'SOUTH', 'EAST', 'WEST') THEN 'FAIL'
         ELSE 'PASS' END AS dq_status,
    concat_ws('; ',
        CASE WHEN coalesce(trim(store_id), '') = '' THEN 'store_id is required' END,
        CASE WHEN source_key_count > 1 THEN 'duplicate store_id in source file' END,
        CASE WHEN coalesce(trim(zone), '') = '' OR upper(trim(zone)) NOT IN ('NORTH', 'SOUTH', 'EAST', 'WEST') THEN 'zone is missing or invalid' END
    ) AS failure_reason,
    CAST(NULL AS VARCHAR) AS dq_warning,
    _loaded_at, _source_file, _run_id
FROM latest WHERE latest_rank = 1;