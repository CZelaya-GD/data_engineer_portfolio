from pathlib import Path 
from day6_row_validation import read_and_validate_csv

valid_rows, errors = read_and_validate_csv(
    Path("data/input/test_input.csv"), 
    required_headers=["id", "name", "age"],
    required_non_empty=["id", "name"], 
    int_fields=["age"]
)

print("Valid rows: ", valid_rows)
print("Errors: ", errors)