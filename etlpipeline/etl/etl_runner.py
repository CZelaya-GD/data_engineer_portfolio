#!/usr/bin/env python3
"""
etl_runner.py - Production HN Top Stories ETL Pipeline.

Purpose:
    Fetch HN topstories.json, filter recent posts, and load them into SQLite.

Inputs:
    --days-back INT: Days of data to keep (default=30).

Output:
    /app/data/hn_posts.db (or DATABASE_URL target when configured).

Raises:
    ValueError, requests.RequestException, sqlite3.Error.

Usage:
    python etl_runner.py --days-back 30
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any, List

import pandas as pd
import requests
import sqlite3

from etl.schema import to_hn_schema
from etlpipeline.config.db_config import get_engine, get_db_url

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HN_TOPSTORIES_URL = "https://hacker-news.firebaseio.com/v0/topstories.json"
HN_ITEM_URL = "https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
DEFAULT_WAREHOUSE_PATH = Path('/app/data/hn_posts.db')


def validate_inputs(days_back: int) -> None:
    """Validate CLI arguments per production standards."""
    if not isinstance(days_back, int):
        raise ValueError(f"days_back must be an int, got {type(days_back).__name__}")
    if days_back < 1 or days_back > 365:
        raise ValueError(f"days_back must be 1-365, got {days_back}")


def fetch_top_story_ids(max_stories: int = 500) -> List[int]:
    """Fetch top HN story IDs from the public API."""
    if not isinstance(max_stories, int) or max_stories < 1:
        raise ValueError(f"max_stories must be a positive int, got {max_stories}")

    logger.info("Fetching top %s story IDs...", max_stories)
    try:
        response = requests.get(HN_TOPSTORIES_URL, timeout=10)
        response.raise_for_status()
        story_ids = response.json()
        if not isinstance(story_ids, list):
            raise ValueError("topstories response was not a list")
        return [int(story_id) for story_id in story_ids[:max_stories]]
    except requests.RequestException:
        logger.error("Failed to fetch topstories", exc_info=True)
        raise
    except ValueError:
        logger.error("Invalid topstories payload", exc_info=True)
        raise


def fetch_recent_stories(story_ids: List[int], days_back: int) -> List[dict[str, Any]]:
    """Fetch individual stories and keep only recent story items."""
    if not isinstance(story_ids, list):
        raise ValueError("story_ids must be a list")
    validate_inputs(days_back)

    logger.info("Fetching details for %s stories...", len(story_ids))
    recent_stories: List[dict[str, Any]] = []

    for story_position, hn_story_id in enumerate(story_ids):
        try:
            response = requests.get(HN_ITEM_URL.format(story_id=hn_story_id), timeout=5)
            response.raise_for_status()
            story_data = response.json()

            if not story_data or story_data.get('type') != 'story' or 'time' not in story_data:
                continue

            story_time = pd.Timestamp(story_data['time'], unit='s')
            story_age_days = (pd.Timestamp.now(tz=story_time.tz) - story_time).days

            if story_age_days <= days_back:
                recent_stories.append(story_data)

            if story_position % 50 == 0:
                logger.info(
                    "Processed %s/%s stories, %s recent posts found",
                    story_position,
                    len(story_ids),
                    len(recent_stories),
                )
        except requests.RequestException:
            logger.debug("Skipping story_id=%s due to request error", hn_story_id, exc_info=True)
            continue
        except (TypeError, ValueError):
            logger.debug("Skipping story_id=%s due to invalid payload", hn_story_id, exc_info=True)
            continue

    return recent_stories


def _serialize_object_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Convert list/dict columns to strings so SQLite can store them safely."""
    cleaned_df = df.copy()
    for column_name in cleaned_df.columns:
        if cleaned_df[column_name].dtype == "object":
            if cleaned_df[column_name].apply(lambda value: isinstance(value, (list, dict))).any():
                cleaned_df[column_name] = cleaned_df[column_name].apply(
                    lambda value: json.dumps(value) if isinstance(value, (list, dict)) else value
                )
    return cleaned_df


def save_warehouse(df: pd.DataFrame, warehouse_path: Path = DEFAULT_WAREHOUSE_PATH) -> None:
    """Transform and load the DataFrame into the warehouse."""
    if not isinstance(df, pd.DataFrame):
        raise ValueError("df must be a pandas DataFrame")
    if df.empty:
        raise ValueError("df cannot be empty")
    if not isinstance(warehouse_path, Path):
        raise ValueError("warehouse_path must be a Path")

    logger.info("Saving %s rows to %s...", len(df), warehouse_path)
    warehouse_path.parent.mkdir(parents=True, exist_ok=True)

    engine = None
    try:
        df_ready = to_hn_schema(_serialize_object_columns(df))
        engine = get_engine()
        df_ready.to_sql('hn_posts', engine, if_exists='replace', index=False)
        logger.info("Warehouse saved: %s rows into hn_posts", len(df_ready))
    except sqlite3.Error:
        logger.error("SQLite/warehouse error", exc_info=True)
        raise
    except ValueError:
        logger.error("Schema/validation error", exc_info=True)
        raise
    finally:
        if engine is not None:
            engine.dispose()


def main(days_back: int = 30) -> None:
    """Run the full HN ETL pipeline."""
    try:
        validate_inputs(days_back)
        logger.info("Starting HN Top Stories ETL Pipeline (db=%s)", get_db_url())

        story_ids = fetch_top_story_ids(max_stories=500)
        recent_stories = fetch_recent_stories(story_ids, days_back)

        if not recent_stories:
            logger.warning("No recent stories found")
            sys.exit(0)

        df = pd.DataFrame(recent_stories)
        save_warehouse(df, DEFAULT_WAREHOUSE_PATH)
        logger.info("ETL COMPLETE: %s recent posts -> SQLite", len(df))
        sys.exit(0)
    except (ValueError, requests.RequestException, sqlite3.Error):
        logger.error("ETL Pipeline failed", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="HN Top Stories ETL: API -> SQLite Warehouse")
    parser.add_argument('--days-back', type=int, default=30, help="Days of recent posts to keep (1-365)")
    args = parser.parse_args()
    main(args.days_back)