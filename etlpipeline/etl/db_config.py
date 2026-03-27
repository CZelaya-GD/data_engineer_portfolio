import os 
from pathlib import Path 
from sqlalchemy import create_engine
from typing import str, Engine

def get_db_url() -> str: 
    """Return DATABASE_URL or SQLite fallback."""
    if db_url := os.getenv("DATABASE_URL"):
        return db_url    # Prod: postgresql://postgres:password@postgres:5432/hn_warehouse
    
    # Dev fallback: SQLite file 
    db_path = Path("/app/data/hn_posts.db")
    db_path.parent.mkdir(exist_ok=True, parents=True)

    return f"sqlite:///{db_path}"


def get_engine():
    """Returns SQLAlchemy engine (handles connection pooling)."""
    return create_engine(get_db_url(), 
                         pool_pre_ping=True)