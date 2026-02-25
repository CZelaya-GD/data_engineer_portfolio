"""
Week 1 Capstone: Production CSV ETL Pipeline (Day 7 Deliverable)
Chains: Vlaidation -> safe_int -> cleaning -> HN dataset generation
"""

import logging 
from pathlib import Path 
from typing import List, Dict
import pandas as pd 
from row_validation import read_and_validate_csv
from safe_integer_converter import safe_int

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def production_csv_pipeline(
        raw_csv: str,
        cleaned_csv: str,
        hn_output: str,
        required_headers: List[str] = ["id", "price"]
) -> None: 
    """
    Week 1 Production ETL: Raw CSV -> Validated -> HN Dataset (112k pipeline start)
    """
    # Production Validation 
    valid_rows, errors = read_and_validate_csv(
        Path(raw_csv), 
        required_headers=["id", "price"], 
        required_non_empty=["id"], 
        int_fields=["price"]
    )

    if errors: 
        logger.warning(f"Skipped {len(errors)} invalid rows")

    # Safe integer conversion 
    df = pd.DataFrame(valid_rows)
    df["price_int"] = df["price"].apply(
        lambda x: safe_int(x, default=0, transform= lambda v: max(0,v))
    )

    # Save Cleaned + Generate HN data
    df.to_csv(cleaned_csv, index=False)
    logger.info(f"✅ Week 1 Prototype: {len(df)} valid rows -> {cleaned_csv}")

    logger.info(f" Ready for hnanalysis_dashboard.sql -> etlpipeline Docker API")

if __name__ == "__main__":

    production_csv_pipeline(
        "data/input/raw_sales.csv",
        "data/output/week1_cleaned.csv",
        "data/input/hn_data.csv"
    )