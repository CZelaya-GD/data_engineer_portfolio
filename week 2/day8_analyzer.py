import logging 
from pathlib import Path 
from typing import List
import pandas as pd 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_top_posts(csv_path: Path) -> pd.DataFrame: 
    """
    Docstring for analyze_top_posts
    
    Analyze HackerNews CSV: top users by comments, avg score by domain. 

    Args: 
        csv_path: Path to cleaned HackerNews CSV 

    Returns: 
        DataFrame: Top 1- users (comments), avg score by domain 

    Raises: 
        FileNotFoundError: CSV missing 
        ValueError: Invalid CSV structure
    """
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    
    try: 
        df = pd.read_csv(csv_path)
        if "user" not in df.columns or "comments" not in df.columns:
            raise ValueError("Missing required columns user, comments")
        
        # Top 10 users by comments 
        top_users = (df.groupby('user')['comments']
                     .sum()
                     .sort_values(ascending=False)
                     .head(10)
                     .reset_index())
        
        logger.info(f"Top users analysis complete: {len(top_users)} records")
        return top_users
    
    except pd.errors.EmptyDataError:
        logger.error("Empty CSV file")
        raise ValueError("CSV is empty")
    

# Test
if __name__ == "__main__":
    result = analyze_top_posts(Path("data/output/hn_test.csv"))
    print(result)