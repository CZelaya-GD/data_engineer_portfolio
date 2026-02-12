"""
Docstring for week_1.generate_hn_data

Day 8: HN Dataset Gnerator using Day 3 CSV pipeline + synthetic HN data. 
Production Upgrade: logging, pathlib, specific exceptions, type hints. 
"""


import logging 
import csv 
from pathlib import Path 
from typing import List, Dict
import numpy as np 

# for use throughout the code ✅ ❌

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_csv(input_file: str) -> List[Dict[str, str]]:
    """
    Docstring for extract_csv
    
    Extract CSV rows (Day 3 - upgraded with pathlib/logging).
    """

    if not input_file.exists():
        raise FileNotFoundError(f"Week 1 pipeline missing: {input_file}")

    try:

        with open(input_file, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
        
        logger.info(f"Extracted {len(rows)} rows from {input_file}")
        return rows
        
    except csv.Error as e: 
        logger.error(f"CSV format error in {input_file}: {e}")
        raise

    except UnicodeDecodeError as e: 
        logger.error(f"Encoding error in {input_file}: {e}")
        raise


def generate_rows(rows: List[Dict[str, str]], target_rows: int = 1000) -> List[Dict[str, str]]:
    """
    Docstring for generate_rows
    
    Transform sales data -> realistic HN dataset (Day 8 Deliverable)
    """
    cleaned = []

    for i in range(min(target_rows, len(rows))):
        row = rows[i]

         
        new_row = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

        hn_row = {
            'id': str(i + 1), 
            'user': f"user_{np.random.randint(0, 50)}",   # 50 Active users
            'comments': str(np.random.poission(3) + 1),   # HN comment distribution
            'score': str(int(np.random.exponential(20) + 1)),    # HN score curve
            'domain': np.random.choice(['hn.com', 'github.com', 'reddit.com'])

        }

        if hn_row.get("id"):
            cleaned.append(hn_row)

    logger.info(f"Generated {len(cleaned)} HN rows from {len(rows)} sales rows")
    return cleaned

def save_csv(rows: List[Dict[str, str]], output_file: str) -> None: 
    """
    Docstring for save_csv
    
    Save CSV with schema validation (Day 3 pathlib upgrade)
    """

    
    if not rows: 
        logger.warning("Now rows to save")
        return
    
    # Production ready - handles shema 
    fieldnames = list(rows[0].keys())

    # Validate ALL rows have same schema 
    for i, row in enumerate(rows[1:], 1):
        if set(row.keys()) != set(fieldnames):
            logger.error(f"Schema mismatch in row {i+1}")
            return
        
    output_file.parent.mkdir(parents=True, exist_ok=True)

    try: 
        with open(output_file, mode="w", newline="", encoding="utf-8") as f: 
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        logger.info(f"✅ Saved {len(rows)} rows to {output_file}")

    except Exception as e:
        print(f"Error while saving CSV: {e}")

def generate_dataset(input_file: Path, output_file: Path, target_rows: int = 1000) -> None:
    """
    Docstring for generate_hn_dataset
    
    Day 8 pipeline: Day 3 CSV -> HN Dataset. 
    """
    rows = extract_csv(input_file)
    cleaned_rows = generate_rows(rows, target_rows)
    save_csv(cleaned_rows, output_file)


if __name__ == "__main__": 
    generate_dataset(
        input_file = Path("week_1/data/input/week1_cleaned.csv"),
        output_file = Path("data/input/hackernew.csv"),
        target_rows=1000
    )