"""
Airflow DAG orchestrating the HN Top Stories ETL. 

This DAG wraps the existing production ETL (etl_runner) 
and schedules it to run daily. 
It uses environment-driven database configuration via get_db_url()
so the same DAG can target SQLite or Postgres depending on deployement.

Purpose:
    - Provide a production-style scheduler for the HN ETL pipeline, 
    - Add retry behavior, logging, and clear separation between orchestration
    (Airlflow) and business logic (etl_package).

Inputs: 
    - Airflow environment variables and connections. 
    - DATABASE_URL from the runtime environment (used by get_db_url()).
    - days_back: number of days of HN posts to extract (Airflow DAG param). 

Output: 
    - Loaded HN posts into the configured warehouse (SQLite/Postres). 
    - Airflow task logs and metadata for monitoring. 

Raises: 
    - AirflowSkipException if input validation fails. 
    - AirflowException for unexpected failures during ETL execution. 

Usage: 
    - Place this file under the Airflow 'dags/' directory.
    - Start the Airflow scheduler and webserver. 
    - Trigger the 'hn_etl_daily' DAG manually or let it run on schedule 
"""

from __future__ import annotations 

import logging 
import os 
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG 
from airflow.exceptions import AirflowException, AirflowSkipException 
from airflow.operators.python import PythonOperator

from etl.db_config import get_db_url 
from etl.etl_runner import main as run_etl

logger = logging.getLogger(__name__)

def _validate_days_back(days_back: int) -> None: 
    """
    Validate the 'days_back' parameter used by the ETL pipeline. 

    Purpose: 
        Ensure Airflow does not trigger ETL runs with nonsensical time windows. 

    Inputs: 
        days_back: Integer number of days of data to fetch (must be >= 1).

    Output: 
        None. Raises if invalid. 

    Raises: 
        AirflowSkipException: If 'days_back' is not a positive integer. 
    """
    if not isinstance(days_back, int): 
        raise AirflowSkipException("days_back must be an integer.")
    
    if days_back < 1 or days_back > 365:
        raise AirflowSkipException("days_back must be 1-365.")
    
def _run_hn_etl(**context: Dict[str, Any]) -> None: 
    """
    Task callable to execute the HN ETL pipeline. 

    Purpose: 
        Orchestrate a single ETL run from within Airflow, using the same 
        configuration abstraction as your Dockerized pipeline.

    Inputs (via Airflow context): 
        - dag_run.conf (optional): may override "days_back".
        - Airflow Variables/Env: DATABASE_URL for warehouse target.

    Output: 
        None. 
        The ETL will load data into the target database. 

    Raises: 
        AirflowException: Wraps unexpected ETL failures. 
    """
    try: 
        # Resolve days_back: DAG param -> dag_run.conf override -> default. 
        params = context["params"]
        dag_runs_conf = context.get("dag_run").conf if context.get("dag_run") else {}

        days_back = dag_runs_conf.get("days_back", params.get("days_back", 30))
        days_back = int(days_back)
        _validate_days_back(days_back)

        db_url = get_db_url()
        logger.info("Starting HN ETL run. days_back=%s, db_url=%s", 
                    days_back,
                    db_url)
        
        run_etl(days_back)

        logger.info("HN ETL completed successfully. days_back=%s, db_url=%s",
                    days_back, 
                    db_url)
        
    except AirflowSkipException: 
        # Re-raise skips to mark task as SKIPPED rather than FAILED 
        logger.info("HN ETL skipped due to invalid configuration.")
        raise 

    except FileNotFoundError as exc: 
        logger.error("Required resource not found during ETL: %s", 
                     exc,
                     exc_info=True)
        raise AirflowException(f"Resource missing during ETL: {exc}") from exc
    
    except ValueError as exc: 
        logger.error("Validation error in ETL: %s", 
                     exc, 
                     exc_info=True)
        raise AirflowException(f"Validation error in ETL: {exc}") from exc
    
    except Exception as exc: 
        logger.error("Unexpected ETL failure: %s",
                     exc, 
                     exc_info=True)
        raise AirflowException(f"Unexpected ETL failures: {exc}") from exc
    

default_args: Dict[str, Any] = {
    "owner": "data_engineer", 
    "depends_on_past": False, 
    "retries": 2, 
    "retry_delay": timedelta(minutes=5), 
}

with DAG(
    dag_id="hn_etl_daily", 
    description="Daily Hacker News Top Stories ETL into SQLite/Postgres.",
    default_args=default_args,
    params={"days_back": 30}
    start_date=datetime(2026,3,25),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "docker", "hn", "portfolio"]
) as dag: 
    run_hn_etl = PythonOperator(
        task_id="run_hn_etl", 
        python_callable=_run_hn_etl,
        provide_context=True,
    )

    run_hn_etl