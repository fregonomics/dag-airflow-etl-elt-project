from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task
from airflow.utils.trigger_rule import TriggerRule

from include.nammamart_etl import (
	SOURCE_FILES,
	extract_source,
	load_to_duckdb,
	print_reports,
	transform_data,
	validate_sources,
	record_pipeline_run,
)


@dag(
	dag_id="nammamart_etl_sushanth",
	start_date=pendulum.datetime(2026, 9, 1, tz="Asia/Kolkata"),
	schedule="0 9 * * 1-6",
	catchup=False,
	default_args={
		"owner": "Sushanth",
		"retries": 2,
		"retry_delay": timedelta(minutes=5),
		"execution_timeout": timedelta(minutes=30),
	},
	tags=["nammamart", "etl"],
)
def nammamart_etl_sushanth():
	@task
	def extract(filename: str) -> dict:
		return extract_source(filename)

	@task
	def validate(extracted_files: list[dict]) -> dict:
		run_id = get_current_context()["run_id"]
		return validate_sources(extracted_files, run_id)

	@task
	def transform(validated_files: dict) -> dict:
		run_id = get_current_context()["run_id"]
		return transform_data(validated_files, run_id)

	@task
	def load(transformed_files: dict) -> dict:
		return load_to_duckdb(transformed_files)

	@task
	def report(load_result: dict) -> dict:
		print_reports(load_result)
		return {"reported": True}

	@task(trigger_rule=TriggerRule.ALL_DONE)
	def audit_run() -> dict:
		return record_pipeline_run(get_current_context())

	extracted_files = [
		extract.override(task_id=f"extract_{filename.removesuffix('.csv')}")(
			filename
		)
		for filename in SOURCE_FILES
	]
	validated_files = validate(extracted_files)
	transformed_files = transform(validated_files)
	loaded_tables = load(transformed_files)
	reported = report(loaded_tables)
	audit_run().set_upstream(reported)


nammamart_etl_sushanth()
