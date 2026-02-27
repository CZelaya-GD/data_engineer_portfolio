import pandas as pd 
import logging 
from pathlib import Path 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def day4_clean_sales(input_file: str, output_file: str) -> None:
    """
    Day 4: Pandas class -> production function. 
    """
    path = Path(input_file)
    if not path.exists():
        raise FileNotFoundError(f"{input_file} missing")
    
    df = pd.read_csv(input_file)
    df = df.dropna()
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df.to_csv(output_file, index=False)
    logger.info(f"Day 4 cleaned: {input_file} -> {output_file}")