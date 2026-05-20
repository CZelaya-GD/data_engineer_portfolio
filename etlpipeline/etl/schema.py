"""
Week 3 API <- Day 2 ETL: EXPLICIT SCHEMA MAPPING 
No patching. ETL delivers EXACTLY what API expects.
"""

from typing import Dict, Any, List
import pandas as pd 

HN_POSTS_SCHEMA = {
    'id': "INTEGER PRIMARY KEY", 
    'title': 'TEXT', 
    'user': 'TEXT',
    'score': 'INTEGER', 
    'comments': 'INTEGER', 
    'created_at': 'TEXT'    # ISO datetime
}

def validate_schema(df: pd.DataFrame) -> bool: 
    """Fail-fast schema validation."""
    required = list(HN_POSTS_SCHEMA.keys())
    missing = set(required) - set(df.columns)

    if missing:
        raise ValueError(f"Missing schema columns: {missing}")
    
    return True

def to_hn_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Run HN API -> Production Schema."""

    df_clean = pd.DataFrame({
        'id': df['id'].astype(int),
        'title': df['title'].fillna('').astype(str),
        'user': df.get('by', '').fillna(''),
        'score': df.get('score', 0).fillna(0).astype(int),
        'comments': df['kids'].apply(lambda x: len(x) if isinstance(x, list) else 0),
        'created_at': pd.to_datetime(df['time'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    })

    validate_schema(df_clean)
    return df_clean