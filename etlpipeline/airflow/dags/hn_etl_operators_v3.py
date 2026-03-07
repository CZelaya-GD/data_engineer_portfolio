"""
HN ETL v3: Specialized Operators + Production Polish. 

Purposse: 
    - Replace generic PythonOperators with specialized ones. 
    - HttpOperator for HN API (native retries/timeouts). 
    - PostgresOperator for warehouse (SQL-aware error handling).
    - Add SLA monitoring, email alerts. 

Inputs: 
    - days_back: HN stories from N days ago (DAG param/ UI override). 
    - DATABASE_URL: SQLite/Postgres toggle via your db_config.py. 

Outputs:
    - hn_posts table populates in configured warehouse. 
    - XCom metadata: row counts between tasks. 
    - Airflow task logs + UI visualization. 

Raises: 
    - AirflowSkipException: Invalid inputs or empty upstream data. 
    - AirflowException: Task-specific failures (HN API down, DB unreachable)

Usage: Drop into dags/ -> Airflow auto-discovers. 
"""

from __future__ import annotations

import logging 
from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG 
from airflow.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException 
from airflow.sensors.http import SimpleHttpSensor

from etl.etl_runner import fetch_recent_stories, validate_inputs
from etl.schema import to_hn_schema
from etl.db_config import get_db_url
import pandas as pd 

logger = logging.getLogger(__name__)

def _transform_from_http_response(**context: Dict[str, Any]) -> None: 
    """
    Transform: 
        Process HttpOperator response -> cleaned records -> temp table. 

    Triggered by: 
        extract_topstories (HttpOperator). 
    """
    try: 
        ti = context["ti"]
        top_story_ids = ti.xcom_pull(task_ids="extract_topstories")["content"]

        days_back = 30
        validate_inputs(days_back)
        recent_stories = fetch_recent_stories(top_story_ids, days_back)

        df = pd.DataFrame(recent_stories)
        df_clean = to_hn_schema(df)

        ti.xcom_push(key="cleaned_records", value=df_clean.to_dict("records"))
        logger.info("✅ TRANSFORM: %s -> %s records", len(recent_stories), len(df_clean))

    except Exception as exc: 
        logger.error("TRANSFORM failed: %s", exc, exc_info=True)
        raise AirflowException(f"Transform failed: {exc}") from exc
    
default_args: Dict[str, Any] = {
    "owner": "data_engineer", 
    "depends_on_past": False,
    "retries": 3, 
    "retry_delay": timedelta(minutes=2), 
    "email_on_failure": True,
    "sla": timedelta(hours=4),
    "days_back": 30,
}

with DAG(
    dag_id="hn_etl_operators_v3", 
    description="Production HN ETL: HttpOperator -> Python -> PostgresOperator.",
    default_args=default_args,
    start_date=datetime(2026, 3, 1), 
    schedule_interval="@daily", 
    catchup=False, 
    max_active_runs=1,
    tags=["etl", "operators", "http", "postgres", "sla"]
) as dag: 
    
    # ✅ HTTP Sensor: Wait for HN API health
    wait_for_hn_api = SimpleHttpSensor(
        task_id="wait_for_hn_api", 
        http_conn_id="http_default",   # Airflow connection (auto-created)
        endpoint="topstories.json", 
        request_params={},
        response_check=lambda response: response.status_code == 200, 
        timeout=10, 
        poke_interval=30,
    )

    # ✅ HttpOperator: Extract top 500 story IDs
    extract_topstories = SimpleHttpOperator(
        task_id="extract_topstories",
        http_conn_id="http_default", 
        method="GET", 
        endpoint="topstories.json", 
        response_filter=lambda response: response.json()[:500],   # Top 500 only
        do_xcom_push=True,    # Auto-save response to XCom 
        retries=5,   # More retries for flaky APIs
    )

    # PythonOperator: business transform logic
    transform_stories = PythonOperator(
        task_id="transform_stories", 
        python_callable=_transform_from_http_response, 
        provide_context=True, 
    )

    # ✅ PostgresOperator: Raw SQL load (warehouse-aware)
    load_cleaned_stories = PostgresOperator(
        task_id="load_cleaned_stories", 
        postgres_conn_id="postgres_default",   # Airflow Postgres connection
        sql ="etl/sql/load_hn_posts.sql",
        parameters={"ti": "{{ ti }}"},   # Pass XCom dynamically
    )

    # Dependency chain
    wait_for_hn_api >> extract_topstories >> transform_stories >> load_cleaned_stories 