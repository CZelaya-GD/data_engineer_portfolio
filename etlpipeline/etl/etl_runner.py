"""
etl_runner.py - Production HN Top Stories ETL Pipeline

Purpose: 
    Fetch HN topstories.json -> filter recent posts -> SQLite warehouse 

Inputs: 
    --days-back INT: Days of data to keep (default=30)

Output:  
    /app/data/hn_posts.db (~112 rows for 30 days)

Raises: 
    requests.RequestException, sqlite3.Error, ValueError

Usage: 
    python etl_runner.py --days-back 30 
    # Expected: 112 rows, 30 seconds runtime
"""

import argparse
import logging 
import sys
from pathlib import Path 
from typing import List 

import requests 
import pandas as pd 
import sqlite3

# Production logging setup 
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def validate_inputs(days_back: int) -> None: 
    """Validate CLI arguments per production standards."""
    if days_back < 1 or days_back > 365: 
        raise ValueError(f"days_back must be 1-365, got {days_back}")
    
def fetch_top_story_ids(max_stories: int = 500) -> List[int]: 
    """
    Step 1: 
        Fetch top 500 story IDs from HN API. 

    HN Pattern: 
        topstories.json -> [41123456, 41123455, ...]
    """
    logger.info(f"Fetching top {max_stories} story IDs...")

    url = "https://hacker-news.firebaseio.com/v0/topstories.json"

    try: 
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        story_ids = response.json()
        return story_ids[:max_stories]   # Top 500 only
    
    except requests.RequestException as e: 
        logger.error(f"Failed to fetch topstories: {e}")
        raise

def fetch_recent_stories(story_ids: List[int], days_back: int) -> List[dict]: 
    """
    Step 2: 
        Fetch individual stories -> filter by recency. 

    handles: 
        null responses, deleted stories, non-story items gracefully.
    """
    logger.info(f"Fetching details for {len(story_ids)} stories...")
    recent_stories = []

    for i, story_id in enumerate(story_ids): 

        try: 
            url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
            response = requests.get(url, timeout=5)

            if response.ok: 
                story_data = response.json()

                # Skip null/deleted/non-story items
                if (story_data and 
                    story_data.get('type') == 'story' and 
                    'time' in story_data): 

                    # Filter: only recent posts
                    story_time = pd.Timestamp(story_data['time'], unit ='s')
                    days_old = (pd.Timestamp.now() - story_time).days

                    if days_old <= days_back: 
                        recent_stories.append(story_data)

            # Progress every 50 stories 
            if i % 50 == 0: 
                logger.info(f"Processed {i}/{len(story_ids)} stories, "
                            f"{len(recent_stories)} recent posts found")
                
        except requests.RequestException as e: 
            logger.debug(f"Skip story {story_id}: {e}")
            continue

    return recent_stories 

def save_warehouse(df: pd.DataFrame, warehouse_path: Path) -> None: 
    """Step 3: 
            Save cleaned DataFrame -> SQlite warehouse."""
    
    logger.info(f"Saving {len(df)} rows to {warehouse_path}...")

    # ✅ CRITICAL: Convert list columns to JSON strings (SQL caan't store lists)
    for col in df.columns: 
        if df[col].dtype == "object": 
            # Check if column contains lists 
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)

    conn = sqlite3.connect(warehouse_path)

    try: 
        # Replace existing table (idempotent)
        df.to_sql('hn_posts', 
                  conn,
                  if_exists="replace",
                  index=False)
        
        logger.info(f"✅ Warehouse saved: {len(df)} rows")

    except sqlite3.Error as e: 
        logger.error(f"SQLite error: {e}")
        raise

    finally: 
        conn.close()

def main(days_back: int = 30) -> None:
    """Production HN ETL Pipeline orchestrator."""

    try: 
        # Input validation 
        validate_inputs(days_back)

        # Ensure output dirctory exists
        warehouse_path = Path('/app/data/hn_posts.db')
        warehouse_path.parent.mkdir(parents=True, exist_ok=True)

        # ETL Pipeline: Extract -> Transform -> Load 
        logger.info("Starting HN Top Stories ETL Pipeline...")

        # Step 1: Get top story IDs 
        story_ids = fetch_top_story_ids(max_stories=500)

        # Step 2: Fetch + filter recent stories
        recent_stories = fetch_recent_stories(story_ids, days_back)

        # Step 3: Create DataFrame -> SQLite warehouse
        if recent_stories: 
            df = pd.DataFrame(recent_stories)
            save_warehouse(df, warehouse_path)
            logger.info(f"✅ ETL COMPLETE: {len(df)} recent posts -> SQLite")

        else: 
            logger.warning("No recent stories found")

        sys.exit(0)

    except (ValueError, 
            requests.RequestException, 
            sqlite3.Error) as e:
        logger.error(f"❌ ETL Pipeline failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="HN Top Stories ETL: API -> SQLite Warehouse (Day 24)"
    )
    
    parser.add_argument(
        '--days-back', 
        type=int, 
        default=30, 
        help="Days of recent posts to keep (1-365)"
    )

    args = parser.parse_args()
    main(args.days_back)