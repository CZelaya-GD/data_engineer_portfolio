"""
GCP Cloud Storage configuration. 

Purpose:
    Single source of truth for GCS operations (ETL artifacts).

Inputs: 
    GCS_PROJECT_ID, GCS_BUCKET_NAME env vars.

Outputs: 
    GCS client config. 

Usage: 
    from config.gcs_congig import get_gcs_config
    project_id, bucket_name = get_gcs_config()
"""

import os 
from typing import Tuple 

def get_gcs_config() -> Tuple[str, str]: 
    """Returns (project_id, bucket_name) for GCS operations."""
    project_id = os.getenv("GCP_PROJECT_ID", "data-engineer-portfolio-489620")
    bucket_name = os.getenv("GCS_BUCKET_NAME", "hn-etl-artifacts")

    if not project_id or not bucket_name: 
        raise ValueError("GCP_PROJECT_ID and GCS_BUCKET_NAME required")
    
    return project_id, bucket_name