from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re

import duckdb

from include.pipeline_audit import write_pipeline_run


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"
SQL_DIR = PROJECT_ROOT / "include" / "sql"
WAREHOUSE_PATH = PROJECT_ROOT / "include" / "warehouse_sushanth.duckdb"
SOURCE_FILES = (
    "orders.csv",
    "payments.csv",
    "customers.csv",
    "products.csv",
    "stores.csv",
    "orders_2026-10-01_increment.csv",
)
RAW_TABLES = {
    "orders.csv": "raw_orders",
    "orders_2026-10-01_increment.csv": "raw_orders",
    "payments.csv": "raw_payments",
    "customers.csv": "raw_customers",
    "products.csv": "raw_products",
    "stores.csv": "raw_stores",
}
STAGING_SQL = (
    "stg_customers.sql",
    "stg_products.sql",
    "stg_stores.sql",
    "stg_orders.sql",
    "stg_payments.sql",
    "dq_results.sql",
)
MART_SQL = (
    "mart_daily_store_kpis.sql",
    "mart_category_margin.sql",
    "mart_delivery_sla.sql",
    "mart_payment_reconciliation.sql",
    "mart_top_customers.sql",
    "mart_dq_rejections.sql",
)


def _safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.:-]+", "_", run_id)


def extract_source(filename: str) -> dict:
    if filename not in SOURCE_FILES:
        raise ValueError(f"Unsupported source file: {filename}")
    path = DATA_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"Missing source file: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as source:
        rows_extracted = max(sum(1 for _ in source) - 1, 0)
    return {
        "filename": filename,
        "path": str(path),
        "rows_extracted": rows_extracted,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }


def load_raw(files: list[dict], run_id: str) -> dict:
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(WAREHOUSE_PATH))
    loaded_at = datetime.now(timezone.utc).isoformat()
    counts = {}
    try:
        for item in files:
            filename = item["filename"]
            table = RAW_TABLES[filename]
            path = item["path"].replace("'", "''")
            connection.execute(
                f"CREATE OR REPLACE TEMP VIEW incoming_raw AS "
                f"SELECT * FROM read_csv('{path}', header=true, all_varchar=true, "
                "nullstr='__DUCKDB_NULL_SENTINEL__')"
            )
            if not connection.execute(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_name = ?",
                [table],
            ).fetchone()[0]:
                connection.execute(
                    f"CREATE TABLE {table} AS SELECT *, "
                    "CAST(NULL AS VARCHAR) AS _loaded_at, "
                    "CAST(NULL AS VARCHAR) AS _source_file, "
                    "CAST(NULL AS VARCHAR) AS _run_id "
                    "FROM incoming_raw WHERE FALSE"
                )
            connection.execute(
                f"INSERT INTO {table} "
                "SELECT *, ?, ?, ? FROM incoming_raw",
                [loaded_at, filename, _safe_run_id(run_id)],
            )
            counts[filename] = connection.execute(
                "SELECT count(*) FROM incoming_raw"
            ).fetchone()[0]
            connection.execute("DROP VIEW incoming_raw")
    finally:
        connection.close()
    return {
        "run_id": _safe_run_id(run_id),
        "warehouse_path": str(WAREHOUSE_PATH),
        "rows_appended": counts,
    }


def _run_sql_files(connection, filenames: tuple[str, ...], run_id: str) -> None:
    sql_run_id = _safe_run_id(run_id).replace("'", "''")
    for filename in filenames:
        sql = (SQL_DIR / filename).read_text(encoding="utf-8")
        connection.execute(sql.replace("__RUN_ID__", sql_run_id))


def build_staging(raw_result: dict, run_id: str) -> dict:
    connection = duckdb.connect(raw_result["warehouse_path"])
    try:
        _run_sql_files(connection, STAGING_SQL, run_id)
        counts = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "stg_customers", "stg_products", "stg_stores", "stg_orders", "stg_payments", "dq_results"
            )
        }
        rows_loaded = connection.execute(
            "SELECT count(*) FROM stg_orders WHERE dq_status = 'PASS'"
        ).fetchone()[0]
        rows_quarantined = sum(
            connection.execute(
                f"SELECT count(*) FROM {table} WHERE dq_status = 'FAIL'"
            ).fetchone()[0]
            for table in (
                "stg_customers", "stg_products", "stg_stores", "stg_orders", "stg_payments"
            )
        )
    finally:
        connection.close()
    return {
        "run_id": _safe_run_id(run_id),
        "warehouse_path": raw_result["warehouse_path"],
        "raw_rows_appended": raw_result["rows_appended"],
        "staging_counts": counts,
        "rows_loaded": rows_loaded,
        "rows_quarantined": rows_quarantined,
    }


def build_marts(staging_result: dict) -> dict:
    connection = duckdb.connect(staging_result["warehouse_path"])
    try:
        _run_sql_files(connection, MART_SQL, staging_result["run_id"])
        counts = {
            table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            for table in (
                "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
                "mart_payment_reconciliation", "mart_top_customers", "mart_dq_rejections",
            )
        }
    finally:
        connection.close()
    return {
        "run_id": staging_result["run_id"],
        "warehouse_path": staging_result["warehouse_path"],
        "staging_counts": staging_result["staging_counts"],
        "mart_counts": counts,
    }


def report_marts(mart_result: dict) -> None:
    connection = duckdb.connect(mart_result["warehouse_path"], read_only=True)
    try:
        for table in (
            "mart_daily_store_kpis", "mart_category_margin", "mart_delivery_sla",
            "mart_payment_reconciliation", "mart_top_customers", "mart_dq_rejections",
        ):
            print(f"{table}:")
            print(connection.execute(f"SELECT * FROM {table} LIMIT 25").df().to_string(index=False))
        print("dq_results:")
        print(connection.execute(
            "SELECT check_name, dimension, severity, rows_checked, rows_failed, status "
            "FROM dq_results WHERE run_id = ? AND dag_id = 'nammamart_elt_sushanth' "
            "ORDER BY dimension, check_name",
            [mart_result["run_id"]],
        ).df().to_string(index=False))
        print(f"Staging counts: {mart_result['staging_counts']}")
        print(f"Mart counts: {mart_result['mart_counts']}")
    finally:
        connection.close()


def record_pipeline_run(context: dict) -> dict:
    task_instance = context["ti"]
    dag_run = context["dag_run"]
    extracted = [
        task_instance.xcom_pull(task_ids=f"extract_{Path(filename).stem}")
        for filename in SOURCE_FILES
    ]
    rows_extracted = sum(
        int(result.get("rows_extracted", 0))
        for result in extracted
        if isinstance(result, dict)
    )
    staging_result = task_instance.xcom_pull(task_ids="transform_staging") or {}
    rows_loaded = int(staging_result.get("rows_loaded", 0))
    rows_quarantined = int(staging_result.get("rows_quarantined", 0))
    required_outputs = [
        *(f"extract_{Path(filename).stem}" for filename in SOURCE_FILES),
        "append_raw", "transform_staging", "transform_marts", "report",
    ]
    failed = any(
        task_instance.xcom_pull(task_ids=task_id) is None
        for task_id in required_outputs
    )
    extract_times = [
        datetime.fromisoformat(result["extracted_at"])
        for result in extracted
        if isinstance(result, dict) and result.get("extracted_at")
    ]
    start_time = min(extract_times) if extract_times else task_instance.start_date
    return write_pipeline_run(
        str(WAREHOUSE_PATH),
        context["run_id"],
        "nammamart_elt_sushanth",
        start_time,
        rows_extracted,
        rows_loaded,
        rows_quarantined,
        "FAILED" if failed else "SUCCESS",
    )