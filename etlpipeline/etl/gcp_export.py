"""
Day 32: Export Airflow SQLite -> GCS -> BigQuery. 

Purpose: 
    Cloud migration prep. 

Inputs: 
    Local hn_posts.db -> gs://czelaya-hn-data/ 

Output: 
    BigQuery table ready for Cloud Compose DAG
"""

import logging 
from pathlib import Path 
import pandas as pd 
from etl.db_config import get_engine
import os 

logger = logging.getLogger(__name__)

def export_to_gcs(days_back: int = 30) -> None: 
    """Export ETL output -> GCS CSV (free tier)."""
    try: 
        engine = get_engine()
        df = pd.read_sql(f"""
            SELECT * FROM hn_posts
            WHERE created_at >= CURRENT_DATE - INTERVAL '{days_back}' DAY
        """, engine)
        
        csv_path = Path("/tmp/hn_posts.csv")
        df.to_csv(csv_path, index=False)

        bucket = "czelaya-hn-data"
        os.system(f"gsutil cp {csv_path} gs://{bucket}/hn_posts_{days_back}d.csv")

        logger.info("✅ Exported %s rows to gs://%s", len(df), bucket)

    except Exception as exc: 
        logger.error("GCP export failed: %s", exc)
        raise

if __name__ == "__main__": 
    export_to_gcs()