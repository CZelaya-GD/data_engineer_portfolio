import sqlite3 
from pathlib import Path 
import pandas as pd 

def run_hn_dashboard(db_path: str) -> pd.DataFrame: 
    """
    Docstring for run_hn_dashboard
    
    Execute complete HN analysis -> executive dashboard. 

    Args: 
        db_path: Path to hn_posts SQLite

    Returns: 
        Multi-metric dashboard DataFrame

    Raises: 
        FileNotFoundError: Database missing 
    """

    if not Path(db_path).exists(): 
        raise FileNotFoundError(f"HN database not found: {db_path}")
    
    query = """
    -- Day 13 SQL above (paste complete query at a later date)
    """

    with sqlite3.connect(db_path) as conn: 
        df = pd.read_sql_query(query, conn)

    return df 