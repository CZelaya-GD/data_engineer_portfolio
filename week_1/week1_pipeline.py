"""
Docstring for week1_pipeline

Week 1 Portfolio: Production CSV ETL Pipeline
Chains: Day3_csv_cleaner -> Day4_cleaner -> Day5_safe_int -> Day6_validate -> Output

"""
import logging
from pathlib import Path 
from typing import List, Dict
import pandas as pd 
import csv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from week_1.row_validation import validate_headers, read_and_validate_csv

from week_1.safe_integer_converter import safe_int

def week1_master_pipeline(
        raw_csv: str, 
        cleaned_csv: str, 
        required_headers: List[str] = ["id", "price"], 
        int_fields: List[str] = ["price"]
) -> None:
    """
    Week 1: Full production ETL using Days 1-6 functions. 
    """

    valid_rows, errors = read_and_validate_csv(
        Path(raw_csv), required_headers, required_headers, int_fields
    )

    if errors: 
        logger.warning(f"Skipped {len(errors)} invalid rows.")

    df = pd.DataFrame(valid_rows)
    df["price"] = pd.to_numeric(df["price"], errors ="coerce")
    df = df.dropna(subset=["price"])

    df["price_int"] = df["price"].apply(
        lambda x: safe_int(x, default=0, transform=lambda v: max(0, v))
    )

    Path(cleaned_csv). parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cleaned_csv, index=False)
    logger.info(f"✅ Week 1 Pipeline: [raw_csv] -> {cleaned_csv} ({len(df)} rows.")

    if __name__ == "__main__": 
        week1_master_pipeline(
            "data/input/raw_sales.csv",
            "data/output/week1_cleaned.csv"
        )
        