"""
GCP BigQuery warehouse setup - Free tier 1TB queries/month, 100GB storage. 

Purpose:
    - Initialize BigQuery dataset for HN ETL migration from Docker Postgres.
    - Enable $0 prototyping before Composer/Airflow cloud migration. 

Inputs: 
    - GCP project ID 
    - Dataset: hnwarehouse (free tier)

Outputs: 
    - BigQuery dataset ready for etl.bigquery_sync.py
    - Client configured for Day 42 cloud_etl_pipeline

Raises: 
    - google.api_core.exceptions.GoogleAPIError: Auth/project issues 
    - ValueError: Invalid project/dataset names 

Usage: 
    - python gcp_bigquery_config.py --project my-project --dataset hnwarehouse
"""

import os 
import argparse 
import logging 
from typing import Optional 
from google.cloud import bigquery 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_bigquery(project_id: str, dataset_id: str) -> None: 
    "Create BigQuery dataset with HN schema if missing"
    if not project_id: 
        raise ValueError("GCP_PROJECT_ID environment variable or --project required")
    
    client = bigquery.Client(project=project_id)
    dataset_ref = client.dataset(dataset_id)

    try: 
        dataset = client.get_dataset(dataset_ref)
        logger.info(f"Dataset '{dataset_id}' exists in '{project_id}'")

    except Exception: 
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        logger.info(f"Created dataset '{dataset_id}' in '{project_id}'")

    # HN schema table 
    table_id = f"{project_id}.{dataset_id}.hnposts"
    schema = [
        bigquery.SchemaField("id", "INTEGER"), 
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("url", "STRING"), 
        bigquery.SchemaField("score", "INTEGER"), 
        bigquery.SchemaField("user", "STRING"),
        bigquery.SchemaField("comments", "INTEGER"), 
        bigquery.SchemaField("created_at", "TIMESTAMP"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table, exists_ok=True)
    logger.info(f"Table '{table_id}' ready for HN ETL")

def main(project_id: Optional[str] = None, dataset_id: str = "hnwarehouse") -> None: 

    project_id = project_id or os.getenv("GCP_PROJECT_ID")
    if not project_id: 
        raise ValueError("Set GCP_PROJECT_ID env or use --project")
    
    setup_bigquery(project_id, dataset_id)
    logger.info("GCP BigQuery ready - migrate HN ETL from Docker Postgres")

if __name__ == "__main__": 
    parser = argparse.ArgumentParser(description="GCP BigQuery HN Warehouse")
    parser.add_argument("--project", help="GCP project ID")
    parser.add_argument("--dataset", default="hnwarehouse", help="BigQuery dataset")

    args = parser.parse_args()
    main(args.project, args.dataset)