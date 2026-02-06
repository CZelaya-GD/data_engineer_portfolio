import os 

# Key Takeaway: Separating ETL into functions makes debugging 10x easier because I can test transform logic without file I/O.

def read_csv_rows(input_file): 
    """Extract: Read raw CSV lines into list of lists."""

    rows = []
    try:
        with open(input_file, "r", encoding="utf-8") as f:

            for line in f: 
                stripped = line.strip()
                if not stripped: # Skip empty lines
                    continue

                rows.append(stripped.split(","))

        return rows
    
    except FileNotFoundError:
        print(f"Input file not found: {input_file}")
        return []

def clean_rows(rows):
    """Transform: Strip whitespace from all fields."""

    cleaned = []
    for row in rows: 
        cleaned.append([col.strip() for col in row])

    return cleaned


def write_csv_rows(output_file, rows):
    """Load: Write cleaned rows to output CSV.""" 

    os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)
    
    with open(output_file, "w", encoding="utf-8") as f: 
        for row in rows: 
            f.write(",".join(row) + "\n")


def clean_csv(input_file, output_file):
    """Main ETL orchestrator."""
    
    raw_rows = read_csv_rows(input_file)
    
    if not raw_rows:
        return
    
    cleaned_rows = clean_rows(raw_rows)
    write_csv_rows(output_file, cleaned_rows)
    print(f"Cleaned {len(cleaned_rows)} rows: {output_file}")