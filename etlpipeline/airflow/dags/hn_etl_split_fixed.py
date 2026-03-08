"""
HN Top Stories ETL - Split Airflow DAG (extract -> transform -> load).

Purpose: 
    - Demonstrate task-level orchestration with XCom data passing. 
    - Split etl_runner.py into 3 granular tasks for observability/retries. 
    - Reuse your exact production functions from etl_runner.py. 

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

Usage: 
    - Drop into airflow/dags/ 
    - docker compose --profile airflow,postgres up -d 
    - localhost:8080 -> Trigger "hn_etl_split_daily"
"""
from __future__ import annotations 

from etl.etl_runner import (fetch_top_story_ids,
                            fetch_recent_stories,
                            save_warehouse,
                            validate_inputs)
from etl.db_config import get_db_url
from etl.schema import to_hn_schema

import logging 
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG 
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.http import SimpleHttpOperator
from airflow.operators.bash import BashOperator

import pandas as pd

logger = logging.getLogger(__name__)

def _validate_days_back(days_back: int) -> None: 
    """
    Validate days_back parameter (reused from your single-task DAG). 

    Purpose: 
        Prevent nonsensical time windows from wasting compute. 

    Raises: 
        AirflowSkipException if invalid. 
    """
    if not isinstance(days_back, int): 
        raise AirflowSkipException("days_back must be an integer.")
    
    if days_back < 1 or days_back > 365: 
        raise AirflowSkipException("days_back must be 1-365")

def _extract_hn(**context: Dict[str, Any]) -> List[dict]:
    """
    Extract: HN API -> raw recent stories (your fetch_top_story_ids + 
    fetch_recent_stories).

    Return: 
        List of raw HN dicts for XCom (kept <48KB limit). 

    Raises: 
        AirflowException wrapping your existing requests.RequestException.
    """

    try: 
        # Get days_back from UI override or default
        dag: DAG = context["dag"]
        default_days_back = dag.default_args.get("days_back", 30)
        override_conf = context.get("dag_run").conf if context.get("dag_run") else {}

        days_back = override_conf.get("days_back", default_days_back)

        _validate_days_back(days_back)
        logger.info("EXTRACT: Starting HN fetch for days_back=%s", days_back)

        # Production Function
        story_ids = fetch_top_story_ids(max_stories=500)
        recent_stories = fetch_recent_stories(story_ids, days_back)

        logger.info("✅ EXTRACT complete: %s recent stories found", len(recent_stories))
        return recent_stories
    
    except AirflowSkipException: 
        logger.info("EXTRACT skipped due to invalid configuration.")
        raise

    except Exception as exc: 
        logger.error("EXTRACT failed: %s", exc, exc_info=True)
        raise AirflowException(f"HN extraction failed: {exc}") from exc


def _transform_hn(**context: Dict[str, Any]) -> List[dict]:
    """
    Transform: 
        Raw HN stories -> production schema DataFrame -> records. 

    Uses: 
        XCom from extract_hn task. 

    Returns: 
        Cleaned records for XCom (to_hn_schema logic).
    """

    try: 
        ti = context["ti"]
        raw_stories: List[dict] = ti.xcom_pull(task_ids="extract_hn")

        if not raw_stories: 
            raise AirflowSkipException("No stories to transform.")
        
        logger.info("TRANSFORM: Processing %s raw stories", len(raw_stories))

        # Transform Production logic
        df = pd.DataFrame(raw_stories)
        df_clean = to_hn_schema(df)

        # Convert back to lightweight XCom records
        records = df_clean.to_dict("records")
        logger.info("✅ TRANSFORM complete: %s -> %s cleaned records", len(raw_stories), len(records))

        return records
    
    except AirflowSkipException: 
        logger.info("TRANSFORM skipped (no input data).")
        raise

    except Exception as exc: 
        logger.error("TRANSFORM failed: %s", exc, exc_info=True)
        raise AirflowException(f"HN transform failed: {exc}") from exc


def _load_hn(**context: Dict[str, Any]) -> None:
    """
    Load: 
        Cleaned records -> warehouse (save_warhouse). 

    Uses: 
        XCom from transform_hn task
    """
    try: 
        ti = context["ti"]
        records: List[dict] = ti.xcom_pull(task_ids="transform_hn")

        if not records: 
            raise AirflowSkipException("No records to load.")
        
        logger.info("LOAD: Writing %s records to warehouse", len(records))

        # Production Loader
        df = pd.DataFrame(records)
        db_url = get_db_url()
        logger.info("LOAD target: %s", db_url)

        save_warehouse(df)

        logger.info("✅ LOAD complete: %s rows persisted", len(records))

    except AirflowSkipException: 
        logger.info("LOAD skipped (no input records).")
        raise

    except Exception as exc: 
        logger.error("LOAD failed: %s", exc, exc_info=True)
        raise AirflowException(f"HN load failed: {exc}") from exc
    

# Same production defaults as single-task DAG
default_args: Dict[str, Any] = {
    "owner": "data_engineer", 
    "depends_on_past": False, 
    "retries": 2, 
    "retry_delay": timedelta(minutes=5),
    "days_back": 30,
}

with DAG(
    dag_id = "hn_etl_split_daily",
    description="Daily HN ETL: extract -> transform -> load with XCom passing.", 
    default_args=default_args,
    start_date=datetime(2026, 3, 1), 
    schedule_interval="@daily", 
    catchup=False, 
    max_active_runs=1, 
    tags=["etl", "hn", "xcom", "split", "production"], 
) as dag: 
    extract_task = SimpleHttpOperator(
        task_id="extract_topstories", 
        endpoint="https://hacker-news.firebase.io.com/v0/topstories.json",
        method="GET", 
        response_filter=lambda r: r.json()[:500], 
        do_xcom_push=True,
    )

    transform_task = PythonOperator(
        task_id="transform_hn", 
        python_callable=_transform_hn, 
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_hn", 
        python_callable=_load_hn,
        provide_context=True,
    )

    upload_to_gcs = BashOperator(
        task_id="upload_to_gcs", 
        bash_command="""
        gsutil cp /app/data/hn_posts.csv gs://{{ var.value.gcs_bucket }}/daily/
        bq load --source_format=CSV \
            data_engineer_portfolio:hn_warehouse.hn_posts \
            gs://{{ var.value.gcs_bucket }}/daily/hn_posts.csv \
            id:INTEGER,title:STRING,user:STRING,score:INTEGER,...
        """
    )

    # Task dependencies
    extract_task >> transform_task >> load_task >> upload_to_gcs