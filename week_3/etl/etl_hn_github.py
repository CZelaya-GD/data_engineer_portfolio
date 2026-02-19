"""
ETL Pipeline 1: Github HN Issues -> SQLite -> hnanalysis.sql 
Purpose: Extract HN discussions from Github -> clean -> load 
Inputs: Github API (public)
Outputs: hn_posts.db (feed hnanalysis.sql)
Raises: requests.RequestException, 
        sqlite3.Error
Usage: python etl_hn_github.py
"""

import requests 
import pandas as pd 
import sqlite3
from pathlib import Path 
from typing import List, Dict 
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_github_hn(limit: int = 1000) -> pd.DataFrame: 
    """Extract HN discussions from Github issues."""
    url = "https://api.github.com/search/issues"
    params = {
        "q": "hackernews", 
        "sort": "created", 
        "order": "desc",
        "per_page": 100
    }

    all_data = []
    for page in range(1, (limit // 100) + 2):
        params["page"] = page
        resp = requests.get(url, params=params)
        resp.raise_for_status()

        issues = resp.json().get("items", [])
        all_data.extend(issues)
        logger.info(f"Extracted page {page}: {len(issues)} issues")

        if len(issues) < 100: 
            break

    df = pd.DataFrame(all_data)
    if df.empty: 
        raise ValueError("No data extracted from GitHub")
    
    # Extract user.login from nested JSON
    df['user'] = df['user'].apply(lambda x: x['login'] if isinstance(x, dict) else str(x))

    return df[["id", 'title', 'user','comments', 'created_at']]


def transform(df: pd.DataFrame) -> pd.DataFrame: 
    """Clean/transform for hnanalysis.sql"""

    df["created_at"] = pd.to_datetime(df["created_at"])
    df["score"] = df["comments"] * 0.5 # Proxy score (real HN has upvotes)
    df = df[["id", "title", 'user', "score", "comments", "created_at"]]

    # Validate for hnanalysis.sql
    if df["score"].min() < 0: 
        raise ValueError("Invalid scores after transform")
    
    logger.info(f"Transformed {len(df)} rows")
    return df


def load(df: pd.DataFrame, db_path: str = "hn_posts.db") -> None: 
    """Load to SQLite for hnanalysis.sql"""
    conn = sqlite3.connect(db_path)

    try: 
        df.to_sql("hn_posts", conn, if_exists="replace", index=False)
        logger.info(f"Loaded {len(df)} rows to {db_path}")

    finally: 
        conn.close()

def run_etl(limit: int = 1000) -> None: 
    """Full ETL pipeline."""

    df_raw = extract_github_hn(limit)
    df_clean = transform(df_raw)
    load(df_clean)

if __name__ == "__main__": 
    run_etl()