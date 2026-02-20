from flask import Flask, jsonify
import sqlite3
import pandas as pd 

app = Flask(__name__)

@app.route("/hn/dashboard")
def dashboard_kpis(): 
    """
    Serve hnanalysis.sql results as JSON
    """

    conn = sqlite3.connect("hn_posts.db")

    try: 
        df = pd.read_sql_query("""
            -- hnanalysis.sql final union query here
            SELECT * FROM daily_leaders
            UNION ALL SELECT * FROM consistent_users
            UNION ALL SELECT * FROM top_posts_30d
            ORDER BY event_date DESC, total_comments DESC
            LIMIT 20
        """, conn)

        return jsonify(df.to_dict(orient="records"))
    
    finally: 
        conn.close()

if __name__ == "__main__": 
    run_etl()
    app.run(debug=False, port=5000)   # Production: debug=False