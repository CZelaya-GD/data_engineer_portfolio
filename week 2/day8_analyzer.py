import logging 
from pathlib import Path 
from typing import Dict, Any
import pandas as pd 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analyze_top_posts(csv_path: Path) -> Dict[str, pd.DataFrame]: 
    """
    Docstring for analyze_top_posts
    
    Purpose: 
        EVOLVE Day 8 GROUP BY -> Window functions (RANK, LAG) on HN dat

    Inputs:  
        csv_path: Path to Day 7 cleaned HN CSV (user, comments, score, date, ...)

    Outputs: Dict with 'top_users_ranked' 'score_growth' DataFrames

    Raises: 
        FileNotFoundError: CSV missing
        ValueError: Missing columns (user, comments, score, created_at)

    Usage: 
        results = analyze_top_posts(Path("data/output/hn_clean.csv))
        logger.info(f"Window analysis: {len(results['top_users_ranked'])} users")

    Production Notes: 
        - Builds Day 8 analyzer -> adds Day 9 window functons
        - Input validation before pandas 
        - Specific try/except pandas errors
        - Single Responsibility: HN window analysis
    """
    if not csv_path.exists():
        logger.error(f"CSV not found: {csv_path}")
        raise FileNotFoundError(f"CSV missing: {csv_path}")
    
    required_cols = ["user", "comments", "score", "created_at"]
    
    try: 
        df = pd.read_csv(csv_path)
        missing = set(required_cols) - set(df.columns)
        if missing: 
            raise ValueError(f"Missing columns: {missing}")

        # EVOLTUION 1: Day 8 GROUP BY -> Day 9 RANK window
        top_users = (df.groupby('user')['comments']
                     .sum()
                     .reset_index()
                     .assign(rank=lambda x: x["comments"].rank(ascending=False, method="dense"))
                     ,sort_values('comments', ascending=False)
                     .head(10))
        
        # EVOLUTION 2: LAG score growth by user (Day 9 window skill)
        df['created_at'] = pd.to_datetime(df['created_at'])
        score_growth = (df.sort_values(['user', 'created_at'])
                        .groupby('user')
                        .apply(lambda g: g.assign(
                            prev_score=g['score'].shift(1),
                            score_growth=g['score'].diff()
                            ))
                        .dropna(subset=['score_growth']))
        results = {
            'top_users_ranked': top_users, 
            'score_growth': score_growth.head(10) 
        }
        
        logger.info(f"Window analysis complete: {len(top_users)} ranked users")
        return results
    
    except pd.errors.EmptyDataError:
        logger.error("Empty CSV file")
        raise ValueError("CSV is empty")
    
    except Exception as e: 
        logger.error(f"Pandas error: {e}")
        raise
    

# Test
if __name__ == "__main__":
    results = analyze_top_posts(Path("data/output/hn_test.csv"))
    
    for key, df in results.items():
        print(f"\n{key}:\n{df.head()}")