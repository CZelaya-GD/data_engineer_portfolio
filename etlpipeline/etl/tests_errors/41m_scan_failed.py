# Day 24
"""
etl_runner.py - Production HN ETL Pipeline Runner

Purpose: 
    Regenerate hn_posts.db warehouse from HN API

Inputs: 
    --days-back INT: Days of HN data (default=30)

Output: 
    /app/data/hn_posts.db (112k rows)

Raises: 
    requests.RequestException, sqlite3.Error

Usage: 
    python etl_runner.py --days-back 30 
"""
import argparse 
import logging 
import sys
from typing import Optional 
from pathlib import Path 

import requests
import pandas as pd 
import sqlite3

# Production logging 
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_inputs(days_back:int) -> None: 
    """Input validation per production standards."""

    if days_back < 1 or days_back > 365: 
        raise ValueError("days_back must be 1-365")
    
    
def fetch_hn_posts(days_back: int) -> pd.DataFrame: 
    """Fetch HN posts via HN API."""

    logger.info(f"Fetching {days_back} days HN data...")

    base_url = "https://hacker-news.firebaseio.com/v0"
    max_item = 40000000     # HN max item ID
    batch_size = 1000

    all_posts = []

    for item_id in range(0, max_item, batch_size):

        try: 
            # Single HN items (production uses real pagination)
            response = requests.get(f"{base_url}/item/{item_id}", 
                                         timeout=5)
            
            # Check HTTP status BEFORE .json()
            if response.ok: 
                item_response = response.json()

                if item_response and 'time' in item_response: 
                    # Calculate post age in days
                    days_old = (pd.Timestamp.now() - pd.Timestamp(item_response['time'], unit='s')).days

                    if days_old <= days_back: 
                        all_posts.append(item_response)

        except requests.RequestException as e: 
            logger.error(f"HN API skip {item_id}: {e}")
            continue

    # Progress logging
    logger.info(f"Scanned {item_id}/{max_item}, collected {len(all_posts)} recent posts.")


    df = pd.DataFrame(all_posts)
    logger.info(f"✅ Collected {len(df)} recent HN posts")
    return df 
    
def save_warehouse(df: pd.DataFrame, 
                   warehouse_path: Path) -> None: 
    """Save DataFrame -> SQLite warehouse."""

    conn = sqlite3.connect(warehouse_path)

    try: 
        df.to_sql('hn_posts', 
                  conn,
                  if_exists='replace', 
                  index=False)
        logger.info(f"Warehouse saved: {warehouse_path} ({len(df)} rows)")

    except sqlite3.Error as e: 
        logger.error(f"SQLite error: {e}")
        raise

    finally: 
        conn.close()


def main(days_back: int = 30) -> None: 
    """Production ETL runner."""

    try: 
        validate_inputs(days_back)

        warehouse_path = Path('/app/data/hn_posts.db')
        warehouse_path.parent.mkdir(parents=True,
                                    exist_ok=True)
        
        df = fetch_hn_posts(days_back)
        save_warehouse(df, warehouse_path)

        logger.info("✅ HN ETL pipeline complete")
        sys.exit(0)

    except (ValueError, 
            Exception) as e: 
        logger.error(f"ETL failed: {e}")
        sys.exit(1)


if __name__ == "__main__": 

    parser = argparse.ArgumentParser(description = "HN ETL Pipeline")
    parser.add_argument('--days-back', 
                        type=int,
                        default=30,
                        help="Days of HN data")
    args = parser.parse_args()
    main(args.days_back)