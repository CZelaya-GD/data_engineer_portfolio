import csv 
from typing import List, Dict

# for use throughout the code ✅ ❌

def extract_csv(input_file: str) -> List[Dict[str, str]]:

    try:

        with open(input_file, mode="r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
        
    except FileNotFoundError:
        print(f"Error: file not found: {input_file}")
        return[]
    
    except Exception as e:
        print(f"Unexpected error while extracting CSV: {input_file}")


def clean_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:

    cleaned = []
    for row in rows: 
        new_row = {k: (v.strip() if isinstance(v, str) else v) for k, v in row.items()}

        # Example rule: drop rows missing a value in the "id" column

        if new_row.get("id"):
            cleaned.append(new_row)

    return cleaned

def save_csv(rows: List[Dict[str, str]], output_file: str) -> None: 
    
    if not rows: 
        print("No rows to save, \nExiting wihtout writing file")
        return
    
    # Production ready - handles shema 
    fieldnames = list(rows[0].keys()) if rows else []

    # Validate ALL rows have same schema 
    for i, row in enumerate(rows[1:], 1):
        if set(row.keys()) != set(fieldnames):
            print(f"Schema mismatch in row {i+1}")
            return

    try: 
        with open(output_file, mode="w", newline="", encoding="utf-8") as f: 
            writer = csv.DictWriter(f, fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        print(f"Saved {len(rows)} rows to {output_file}")

    except Exception as e:
        print(f"Error while saving CSV: {e}")

def clean_csv(input_file: str, output_file:str) -> None:

    rows = extract_csv(input_file)
    cleaned = clean_rows(rows)
    save_csv(cleaned, output_file)


if __name__ == "__main__":
    # Adjust file names as needed
    clean_csv("data/input/raw_data.csv", "data/output/cleaned_data.csv")