"""
HN Dashboard API Server v3.0
Purpose: Production 4-endpoints dashboard serving hn_posts.db ETL data
Week 3 architecture: 
    data/hn_posts.db
    etl/queries.py
    etl/serve_hn.py

Inputs: ../data/hn_posts.db (existing data!) + queries.py
Outputs: JSON endpoints -> LIVE dashboard KPIs
Usage:
    cd week_3/etl
    python serve_hn.py
    curl http://127.0.0.1:5000/api/dashboard
"""

from flask import Flask, jsonify
import sqlite3
import pandas as pd 
import logging 
from pathlib import Path 
from typing import Dict, Any, List 
from queries import (
    DAILY_LEADERS, 
    TOP_USERS_LAST_7D, 
    TRENDING_TITLES_LAST_7D,
    ACTIVITY_LAST_24H
)

# ============================================================================
# Production Logging & Config
# ============================================================================

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

app = Flask(__name__)
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR.parent / 'data' / 'hn_posts.db'

# ============================================================================
# Production DB Connection
# ============================================================================

def get_database_connection() -> sqlite3.Connection: 
    """
    Validate ETL database exists -> production connection. 

    Raises: 
        FileNotFoundError: If hn_posts.db is missing
    """

    if not DB_PATH.exists(): 
        raise FileNotFoundError(
            f'ETL database missing. Run: python etl_hn_github.py'
            f"Expected: {DB_PATH}"
        )

    logger.info(f"✅ Connected to ETL database: {DB_PATH}")
    return sqlite3.connect(DB_PATH)

# ============================================================================
# DRY Query Executor (Single Responsibility)
# ============================================================================

def execute_query(query_name: str, sql_query: str) -> Dict[str, Any]: 
    """
    Execute SQL -> JSON response (reusable across endpoints). 

    Args: 
        Query_name: For logging (e.g. "DAILY_LEADERS")
        sql_query: From queries.py

    Raises: 
        pd.errors.DatabaseError: SQL syntax/schema issues
        sqlite3.Error: Connection failures
    """

    connection = get_database_connection()

    try: 
        dataframe = pd.read_sql_query(sql_query, connection)
        logger.info(f"✅ {query_name}: Served {len(dataframe)} rows")

        # Single row -> dict (activity summary)
        if len(dataframe) == 1: 
            return jsonify(dataframe.to_dict(orient='records')[0])

        # Multi-row -> list of dicts
        return jsonify(dataframe.to_dict(orient='records'))

    except (pd.errors.DatabaseError, sqlite3.Error) as error:
        logger.error(f"❌ {query_name} failed: {error}")
        return jsonify({"error": f"{query_name} query failed: {str(error)}"}), 500

    finally: 
        connection.close()

# ============================================================================
# HN Dashboard API Endpoints
# ============================================================================

@app.route("/api/dashboard")
def get_daily_leaders() -> Dict[str, Any]: 
    """Daily top commenters (week 3 flagship). """
    return execute_query("DAILY_LEADERS", DAILY_LEADERS)

@app.route("/api/users")
def get_top_users() -> Dict[str, Any]: 
    """Top users by engagement (last 7 days)"""
    return execute_query("TOP_USERS_LAST_7D", TOP_USERS_LAST_7D)

@app.route("/api/trending")
def get_trending_topics() -> Dict[str, Any]: 
    """Most discussed HN titles (last 7 days)."""
    return execute_query("TRENDING_TITLES_LAST_7D", TRENDING_TITLES_LAST_7D)

@app.route("/api/activity")
def get_recent_activity() -> Dict[str, Any]: 
    """24hr pipeline health summary."""
    return execute_query("ACTIVITY_LAST_24H", ACTIVITY_LAST_24H)

@app.route("/health")
def health_check() -> Dict[str, Any]: 
    """Production health endpoints."""
    try: 
        connection = get_database_connection()
        connection.execute("SELECT 1")
        connection.close()
        return jsonify({"status": "healthy", "database": str(DB_PATH)})

    except Exception as error: 
        logger.error(f"Health check failed: {error}")
        return jsonify({"status": unhealthy}), 503

# ============================================================================
# Production Server
# ============================================================================

if __name__ == "__main__": 
    logger.info("=" * 60)
    logger.info("🚀 HN DASHBOARD API v3.0")
    logger.info("📊 Database: {DB_PATH}")
    logger.info("🔗Endpoints: ")
    logger.info(" GET /api/dashboard   -> Daily leaders")
    logger.info(" GET /api/users       -> Top users (7D)")
    logger.info(" GET /api/trending     -> Hot topics (7D)")
    logger.info(" GET /api/activity     -> 24hr summary")
    logger.info(" GET /health     -> Production health")
    logger.info("=" * 60)
    app.run(host="127.0.0.1", debug=False, port=5000)