"""
HN Dashboard API Server 
Purpose: Serve week_3/data/hn_posts.db -> JSON KPIs
Inputs: ../data/hn_posts.db (existing data!)
Outputs: /api/dashboard -> LIVE dashboard KPIs
UsageL cd week_3/etl && python serve_hn.py
"""

from flask import Flask, jsonify
import sqlite3
import pandas as pd 
import logging 
import os 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route("/api/dashboard")
def dashboard_kpis(): 
    """
    Serve hn_posts.db -> JSON dashboard KPIs. 
    """
    db_path = os.path.join(os.path.dirname(__file__), '../data/hn_posts.db')

    if not os.path.exists(db_path): 
        return jsonify({"error": f"Database not found: {db_path}"}), 404

    conn = sqlite3.connect(db_path)

    try: 
        query = """
            -- hnanalysis.sql final query here
            WITH daily_leaders AS (
                SELECT 'DAILY_LEADER' as metric,
                    DATE(created_at) as event_date,
                    user as user,
                    COUNT(*) as total_comments
                FROM hn_posts 
                WHERE user IS NOT NULL
                GROUP BY DATE(created_at), user
                HAVING COUNT(*) = (
                    SELECT MAX(c.cnt) FROM (
                        SELECT COUNT(*) cnt 
                        FROM hn_posts 
                        WHERE user IS NOT NULL
                        GROUP BY DATE(created_at), user
                    ) c
                )
            )
            SELECT * FROM daily_leaders 
            ORDER BY event_date DESC, total_comments DESC 
            LIMIT 20

        """

        df = pd.read_sql_query(query, conn)
        logger.info(f"✅ Served {len(df)} KPI records from {db_path}")

        return jsonify(df.to_dict(orient="records"))

    except Exception as e: 
        logger.error(f"API error: {e}")
        return jsonify({"error": str(e)}), 500
    
    finally: 
        conn.close()

if __name__ == "__main__": 
    
    logger.info("🚀 HN Dashboard API starting on http://127.0.0.1:5000")
    logger.info("📊 Serving Day 15 hn_posts.db → LIVE JSON dashboard!")
    app.run(debug=False, port=5000)   # Production: debug=False