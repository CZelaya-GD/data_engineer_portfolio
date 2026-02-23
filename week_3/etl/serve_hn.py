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
from typing import Dict, Any, Tuple

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
DATABASE_NAME = "hn_posts"
DB_PATH = BASE_DIR.parent / 'data' / f'{DATABASE_NAME}.db'

# ============================================================================
# Production DB Connection
# ============================================================================

def get_database_connection() -> sqlite3.Connection: 
    """
    Open a connection to the ETL SQLite database. 

    Raises: 
        FileNotFoundError: If the expected database file does not exist.
    """

    if not DB_PATH.exists(): 

        logger.error("Database file not found at %s", {DB_PATH})
        raise FileNotFoundError(
            f'ETL database missing. Expected at: {DB_PATH}.'
            f"Run the Day 15 ETL script to create it."
        )

    logger.info(f"✅ Connected to ETL database: {DB_PATH}")
    return sqlite3.connect(DB_PATH)

# ============================================================================
# VALIDATE SCHEMA 
# ============================================================================

def validate_table_schema(connection: sqlite3.Connection) -> Tuple[bool, str]:
    """"
    Validate that the hn_posts table exists with required columns. 

    Returns: 
        (is_valid, message): tuple with validation result and explanation.
    """
    required_columns = {'id', 'title', 'user', 'score', 'comments', 'created_at'}
    table_query = f"PRAGMA table_info({DATABASE_NAME})"

    try: 
        cursor = connection.execute(table_query)
        columns = {row[1] for row in cursor.fetchall()}
        missing = required_columns - columns

        if missing: 
            return False, f"{DATABASE_NAME} missing columns: {sorted(missing)}"
        return True, f"{DATABASE_NAME} schema valid"
    
    except sqlite3.Error as error: 
        return False, f"Schema validation error: {error}"


# ============================================================================
# DRY Query Executor (Single Responsibility)
# ============================================================================

def execute_query(query_name: str, sql_query: str) -> Any: 
    """
    Execute SQL -> JSON response (reusable across endpoints). 

    Args: 
        Query_name: For logging (e.g. "DAILY_LEADERS")
        sql_query: From queries.py

    Returns: 
        - List of records for multi-row queries. 
        - Single record for one-row queries. 
        - Error JSON with 500 status for execution failures. 

    Raises: 
        pd.errors.DatabaseError: SQL syntax/schema issues
        sqlite3.Error: Connection failures
    """

    connection = get_database_connection()

    is_valid, validation_message = validate_table_schema(connection)
    if not is_valid: 
        logger.error("%s: %s", query_name, validation_message)
        connection.close()
        return jsonify({"error": validation_message}), 500


    try: 
        dataframe = pd.read_sql_query(sql_query, connection)
        row_count = len(dataframe)
        logger.info(f"✅ %s returned %d rows", query_name, row_count)

        records = dataframe.to_dict(orient="records")

        if row_count == 0: 
            # Explicit, non-error empty response
            return jsonify({"records": [], 'row_count': 0, 'query': query_name)})

        # Single row -> dict (activity summary)
        if row_count == 1: 
            return jsonify(records[0])

        # Multi-row -> list of dicts
        return jsonify(records)

    except (pd.errors.DatabaseError, sqlite3.Error) as error:
        logger.error(f"❌ %s query failed: %s", query_name, error)
        return jsonify({"error": f"{query_name} query failed: {error}"}), 500

    finally: 
        connection.close()

# ============================================================================
# HN Dashboard API Endpoints
# ============================================================================

@app.route("/api/dashboard")
def get_daily_leaders() -> Any: 
    """Return daily leaders by comment volume. """
    return execute_query("DAILY_LEADERS", DAILY_LEADERS)

@app.route("/api/users")
def get_top_users() -> Any: 
    """Return the most active users for the last seven days."""
    return execute_query("TOP_USERS_LAST_7D", TOP_USERS_LAST_7D)

@app.route("/api/trending")
def get_trending_topics() -> Any: 
    """Return trending titles for the last seven days."""
    return execute_query("TRENDING_TITLES_LAST_7D", TRENDING_TITLES_LAST_7D)

@app.route("/api/activity")
def get_recent_activity() -> Any: 
    """Return a summary of activity in the last 24 hours."""
    return execute_query("ACTIVITY_LAST_24H", ACTIVITY_LAST_24H)

@app.route("/health")
def health_check() -> Dict[str, Any]: 
    """Health-check endpoint, including basic schema validation."""
    try: 
        connection = get_database_connection()
        is_valid, message = validate_table_schema(connection)
        connection.close()

        status = "healthy" if is_valid else "degraded"
        code = 200 if is_valid else 500

        return jsonify({"status": status, 
                        "database": str(DB_PATH), 
                        "details": message,}
                        , code)

    except Exception as error: 
        logger.error(f"Health check failed: %s",error)
        return jsonify({"status": "unhealthy", "error": str(error)}), 503

# ============================================================================
# Production Server
# ============================================================================

if __name__ == "__main__": 
    logger.info("=" * 60)
    logger.info("🚀 HN DASHBOARD API v3.0")
    logger.info("📊 Database: {DB_PATH}")
    logger.info("🔑 http://127.0.0.1:5000")
    logger.info("🔗Endpoints: ")
    logger.info(" GET /api/dashboard   -> Daily leaders")
    logger.info(" GET /api/users       -> Top users (7D)")
    logger.info(" GET /api/trending     -> Hot topics (7D)")
    logger.info(" GET /api/activity     -> 24hr summary")
    logger.info(" GET /health     -> Production health")
    logger.info("=" * 60)
    app.run(host="127.0.0.1", debug=False, port=5000)