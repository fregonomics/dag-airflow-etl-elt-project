from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from airflow.utils.trigger_rule import TriggerRule

from include.nammamart_elt import (
	SOURCE_FILES,
	build_marts,
	build_staging,
	extract_source,
	load_raw,
	report_marts,
	record_pipeline_run,
)


@dag(
	dag_id="nammamart_elt_sushanth",
	start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
	schedule="0 9 * * 1-6",
	catchup=False,
	default_args={
		"owner": "Sushanth",
		"retries": 2,
		"retry_delay": timedelta(minutes=5),
		"execution_timeout": timedelta(minutes=30),
	},
	tags=["nammamart", "elt"],
)
def nammamart_elt_sushanth():
	@task
	def extract(filename: str) -> dict:
		return extract_source(filename)

	@task
	def append_raw(files: list[dict]) -> dict:
		from airflow.sdk import get_current_context

		return load_raw(files, get_current_context()["run_id"])

	@task
	def transform_staging(raw_result: dict) -> dict:
		from airflow.sdk import get_current_context

		return build_staging(raw_result, get_current_context()["run_id"])

	@task
	def transform_marts(staging_result: dict) -> dict:
		return build_marts(staging_result)

	@task
	def report(mart_result: dict) -> dict:
		report_marts(mart_result)
		return {"reported": True}

	@task(trigger_rule=TriggerRule.ALL_DONE)
	def audit_run() -> dict:
		from airflow.sdk import get_current_context

		return record_pipeline_run(get_current_context())

	extracted_files = [
		extract.override(task_id=f"extract_{filename.removesuffix('.csv')}")(
			filename
		)
		for filename in SOURCE_FILES
	]
	raw_result = append_raw(extracted_files)
	staging_result = transform_staging(raw_result)
	mart_result = transform_marts(staging_result)
	reported = report(mart_result)
	audit_run().set_upstream(reported)


nammamart_elt_sushanth()
