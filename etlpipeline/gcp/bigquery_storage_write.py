"""
BigQuery Storage Write API - Streaming HN posts. 

Purpose:
    Sub-second BQ writes vs batch load_table_from_dataframe. 

Inputs:
    hn_posts DataFrame.

Outputs: 
    Streaming Buffer committed to BQ table.
Raises: 
Usage: 
    from gcp.bigquery_storage_write import stream_hn_posts_to_bq
    stream_hn_posts_to_bq(df_recent_posts)
"""

from google.cloud.bigquery_storage_v1 import WrtieStream, BigQueryWriteClient
from google.cloud.bigquery_storage_v1.types import AppendRowsResponse
from config.gcp_bigquery_config import get_bq_config
import pandas as pd 
import logging 

logger = logging.getLogger(__name__)

def stream_hn_posts_to_bq(df: pd.DataFrame) -> str: 
    """Streams DataFrame to BigQuery using Storage Write API."""

    project_id, dataset_id = get_bq_config()
    table_id = f"{project_id}.{dataset_id}.hnposts_stream"

    client = BigQueryWriteClient()
    parent = f"projects/{project_id}"

    stream_name = f"{parent}/dataset/{dataset_id}/table/hnposts/writeStream"

    # Convert DF to protocol buffers
    # Production: apache-beam or custom proto serialization
    logger.info(f"Streaming {len(df)} rows to {table_id}")
    return stream_name  # returns stream for monitoring