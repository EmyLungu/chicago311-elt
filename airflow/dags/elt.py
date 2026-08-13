from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from cosmos import DbtTaskGroup

from config import (
    project_config,
    profile_config,
    execution_config,
    render_config,
    default_args,
)
from tasks.extract import extract_to_parquet
from tasks.load import load_parquet_to_duckdb, validate_ingestion


@task.branch(task_id="check_extraction")
def check_extraction(**context) -> str:
    ti = context["ti"]
    meta = ti.xcom_pull(key="extraction_metadata", task_ids="extract_bronze")
    if meta and meta.get("parquet_path"):
        return "load_bronze"
    return "finish"


with DAG(
    dag_id="chicago311pipeline",
    description=("ELT pipeline for the Chicago 311 Dataset"),
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    max_active_tasks=1,
    tags=["chicago311", "duckdb", "dbt"],
) as dag:
    start = EmptyOperator(task_id="start")

    extract = PythonOperator(
        task_id="extract_bronze", python_callable=extract_to_parquet
    )
    extract_check = check_extraction()

    load = PythonOperator(
        task_id="load_bronze", python_callable=load_parquet_to_duckdb
    )

    validate_ingest = PythonOperator(
        task_id="validate_ingestion",
        python_callable=validate_ingestion,
    )

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={
            "install_deps": True,
            "append_env": True,
        },
    )

    finish = EmptyOperator(task_id="finish")

    start >> extract >> extract_check
    extract_check >> load >> validate_ingest >> dbt_transform >> finish
    extract_check >> finish
