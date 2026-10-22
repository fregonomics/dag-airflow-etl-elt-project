from datetime import datetime, timezone

import duckdb


def write_pipeline_run(
    warehouse_path: str,
    run_id: str,
    dag_id: str,
    start_time,
    rows_extracted: int,
    rows_loaded: int,
    rows_quarantined: int,
    status: str,
) -> dict:
    end_time = datetime.now(timezone.utc)
    connection = duckdb.connect(warehouse_path)
    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id VARCHAR,
                dag_id VARCHAR,
                start_time TIMESTAMPTZ,
                end_time TIMESTAMPTZ,
                rows_extracted BIGINT,
                rows_loaded BIGINT,
                rows_quarantined BIGINT,
                status VARCHAR
            )
        """)
        connection.execute(
            "DELETE FROM pipeline_runs WHERE run_id = ? AND dag_id = ?",
            [run_id, dag_id],
        )
        connection.execute("""
            INSERT INTO pipeline_runs
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            run_id,
            dag_id,
            start_time,
            end_time,
            rows_extracted,
            rows_loaded,
            rows_quarantined,
            status,
        ])
    finally:
        connection.close()
    return {
        "run_id": run_id,
        "dag_id": dag_id,
        "rows_extracted": rows_extracted,
        "rows_loaded": rows_loaded,
        "rows_quarantined": rows_quarantined,
        "status": status,
    }