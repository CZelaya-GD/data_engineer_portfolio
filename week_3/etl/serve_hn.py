from flask import Flask, jsonify
import sqlite3
import pandas as pd 

app = Flask(__name__)

@app.route("/hn/dashboard")
def hn_dashboard(): 

    conn = sqlite3.connect("hn_posts.db")
    df = pd.read_sql_query("""
        --hnanalysis.sql here
    """, conn)

    return jsonify(df.to_dict(orient="records"))

if __name__ == "__main__": 
    app.run(debug=True)