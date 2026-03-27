"""
BigQuery sync for HN posts (Day 33).

Purpose: 
    Sync hn_posts from local warehouse to BigQuery sandbox in a single call. 

Inputs: 
    - project_id: GCP project (BigQuery sandbox project). 
    - dataset_id: BigQuery dataset (e.g., hn_sandbox). 
    - table_id: BigQuery table name (hn_posts). 

Output: 
    - BigQuery table populated with latest hn_posts data. 
Raises: 
    - ValueError: Invalid inputs. 
    - Exception: Any load errors are propagated. 
"""

from __future__ import annotations

from typing import Optional 
import logging 
import pandas as pd 
from google.cloud import bigquery 

from etl.db_config import get_engine 

logger = logging.getLogger(__name__)

def sync_hn_posts_to_bigquery(
        project_id: str = "data-engineer-portfolio-489620",
        dataset_id: str = "hn_sandbox",
        table_id: str = "hn_posts", 
        limit: Optional[int] = 5000,
) -> None: 
    """
    Read hn_posts from local DB and load into BigQuery table. 

    Usage: 
        python -m etl.bigquery_sync
    """

    if not project_id or not dataset_id or not table_id: 
        raise ValueError("project_id, dataset_id and table_id  are required.")
    
    engine = get_engine()

    query = "SELECT * FROM hn_posts"
    if limit is not None: 
        query += f"LIMIT {int(limit)}"

    df = pd.read_sql(query, engine)
    logger.info("Loaded %s rows from local hn_posts.", len(df))

    client = bigquery.Client(project=project_id)
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    job = client.load_table_from_dataframe(df, full_table_id)
    job.result()

    logger.info("✅ Loaded %s rows into BigQuery table %s.", job.output_rows, full_table_id)


if __name__ == "__main__": 
    sync_hn_posts_to_bigquery()