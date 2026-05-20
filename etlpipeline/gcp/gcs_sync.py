"""
GCS sync for ETL artifacts (run metadata, schema variation)

Purpose: 
    Durable storage for ETL lineage/monitoring alongside BigQuery. 

Inputs:
    etl_metadata dict (run_id, rows_loaded, days_back). 

Outputs:
    GCS blob with JSON metadata. 

Raises:
    google.cloud.exceptions.NotFound.

Usage: 
    from gcp.gcs_sync import sync_etl_metadata_to_gcs
    sync_etl_metadata_to_gcs({"rows_loaded": 499, "days_back": 30})
"""

import json 
from datetime import datetime 
from typing import Dict, Any
from google.cloud import storage 
from config.gcs_config import get_gcs_config
import logging

logger = logging.getLogger(__name__)

def sync_etl_metadata_to_gcs(metadata: Dict[str, Any]) -> None: 
    """Uploads ETL run metadata to GCS."""
    project_id, bucket_name = get_gcs_config()

    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)

    # GCS object path: etl-metadata/YYYY-MM-DD_HH-MM-SS.json
    timestamp = datetime.utcnow().strftime("%Y/%m/%d/%H-%M-%S")
    blob_name = f"etl-metadata/{timestamp}.json"

    blob = bucket.blob(blob_name)
    blob.upload_from_string(json.dumps(metadata, deffault=str),
                            content_type="application/json")
    
    logger.info(f"Uploaded ETL metadata to gs://{bucket_name}/{blob_name}")
    