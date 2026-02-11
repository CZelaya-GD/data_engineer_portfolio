import csv
import logging
from pathlib import Path
from typing import Dict, List, Any, Iterable, Tuple, Optional 

logging.basicConfig(level=logging.INFO)

class RowValidationError(ValueError):
    """Custom error for invalid row validation"""

def validate_headers(required_headers: Iterable[str], actual_headers: Iterable[str]) -> None:
    """
    Validate that all required headers are present in the CSV file. 
    
    Purpose:
        Ensure the the input CSV contains the minimum set of columns required 
        for downstream processing. This prevents subtle bugs later in the 
        pipeline when columns are missing or renamed.

    Inputs:
        required_headers: Iterable[str]
            List or other iterabke of header names that must exist. 

        Actual headers: Iterable[str]
            Headers read from the CSV file. 

    Output: 
        None

    Raises: 
        ValueError: 
            If any required header is missing from the actual headers. 

    Usage: 
        validate_headers(["id", "name"], csv_headers)
    """

    required_set = set(required_headers)
    actual_set = set(actual_headers)

    missing = required_set - actual_set

    if missing: 
        message = f"Missing required headers: {", ".join(sorted(missing))}"
        logging.error(message)
        raise ValueError(message)
    
    logging.info("All required headers are present.")

def parse_int_field(value: str, field_name: str) -> int:
    """
    Safely parse a string value into an integer.

    Purpose: 
        Convert string fields from CSV (e.g. age, quantity) into integers while
        providing clear error messages for invalid inputs. 

    Inputs: 
        value: str
            The raw string read from the CSV field. 

        field_name: str
            Name of the field being parsed, used for logging and errors.

    Outputs: 
        Parsed integer value. 

    Raises: 
        RowValidationError:
            If the value cannot be converted to an integer. 

    Usage: 
        age = parse_int_field(row["age"], "age") 
    """

    if value is None: 
        message = f"field '{field_name}' is None and cannot be converted to int."
        logging.error(message)
        raise RowValidationError(message)
    
    value_stripped = value.strip()
    
    if value_stripped == "": 
        message = f"Field '{field_name}' is empty and cannot be converted to int."
        logging.error(message)
        raise RowValidationError(message)
    
    try: 
        parsed = int(value_stripped)
        logging.info("Parsed field '%s' with value '%s' into int: %d", field_name, value, parsed)
        return parsed
    
    except ValueError as exc: 
        message = f"Field '{field_name}' has invalid integer value: '{value}'."
        logging.error(message)
        raise RowValidationError(message)
    

def validate_row(
        row: Dict[str, str],
        required_non_empty: Iterable[str], 
        int_fields: Iterable[str]
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Docstring for validate_row
    
    Validate a single CSV row and perform basic type conversions.

    Purpose: 
        Centralize row-level validation logic, checking for empty required fields
        and converting selectedd field to integers. This repares data for 
        fruther cleaning steps in later Day 7 script. 

    Inputs: 
        row: Dict[str, str]
            Raw row dictionary from csv.DictReader. 

        required_non_empty: Iterable[str]
            Columns that must not be empty after stripping whitespace. 

        int_fields: Iterble[str]
            Columns that should be parsed as integers if present. 

    Output: 
        Tuple[bool, Optional[Dict[str, Any]], Optional[str]]
            - first element: true if row is valid, False otherwise
            - second element: cleaned/typed row dict if valid, else None
            - third element: error message if invalid, else None

    Raises: 
        None directly. Errors are captured and returned via the tuple. 

    Usage: 
        is_valid, cleaned, error = validate_row(row, ["id", "name"], ["age"])
    """

    cleaned_row: Dict[str, Any] = dict(row)

    for field in required_non_empty: 
        raw_value = cleaned_row.get(field, "")
        if raw_value is None or raw_value.strip() == "": 
            error_msg = f"Required field '{field}' is empty."
            logging.error(error_msg)
            return False, None, error_msg
        
    for field in int_fields: 
        if field in cleaned_row:
            try:
                cleaned_row[field] = parse_int_field(str(cleaned_row[field]), field)

            except RowValidationError as exc: 
                return False, None, str(exc)
            
    logging.info("Row validated successfully: %s", cleaned_row)
    return True, cleaned_row, None

def read_and_validate_csv(
        input_path: Path,  
        required_headers: Iterable[str],
        required_non_empty: Iterable[str], 
        int_fields: Iterable[str], 
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Docstring for read_vallidate_csv
    
    Purpose: 
        Provide a reusable building block for a CLI data cleaner that can
        be extended. This function on safe file handling, structured 
        validation, and clear logging.

    Inputs: 
        input_path: Path 
            Path object pointing to the CSV file to process. 

        required_headers: Iterable[str]
            Columns that must exist in the CSV header.

        required_non_empty: Iterable[str]
            Columns that must be non-empty for a row to be considered valid.

        int_fields: Iterable[str]
            Columns that should be converted t integers. 

    Output: 
        Tuple[List[Dict[str, Any]], List[str]]
            - first element: list of validated and cleaned row dictionaries
            - second element: list of error messages for invalid rows

    Raises: 
        FileNotFoundError: 
            If the input CSV file does not exist. 

        ValueError: 
            If required headers are missing. 

    Usage: 
        valid_rows, errors = read_and_validate_csv(
        Path("input.csv"),
        ["id", "name", "age"],
        ["id", "name"], 
        ["age"]
        )
    """

    if not isinstance(input_path, Path): 
        message = "input_path must be a pathlib.Path instance."
        logging.error(message)
        raise TypeError(message)
    
    if not input_path.exists():
        message = f"Input file does not exist: {input_path}"
        logging.error(message)
        raise FileNotFoundError(message)
    

    valid_rows: List[Dict[str, Any]] = []
    errors: List[str] = []


    try: 
        with input_path.open(mode="r", encoding="utf-8", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            
            if reader.fieldnames is None:
                message = "CSV file has no header row."
                logging.error(message)
                raise ValueError(message)
            
            validate_headers(required_headers, reader.fieldnames)

            for index, row in enumerate(reader, start=1):
                is_valid, cleaned_row, error = validate_row(
                    row=row,
                    required_non_empty=required_non_empty, 
                    int_fields = int_fields,
                )

                if is_valid and cleaned_row is not None: 
                    valid_rows.append(cleaned_row)

                else: 
                    error_message = f"Row {index}: {error}"
                    errors.append(error_message)
                    logging.error(error_message)


    except FileNotFoundError:
        raise
    
    except ValueError:
        raise

    except csv.Error as exc:
        message = f"CSV parsing error: {exc}"
        logging.error(message)
        raise
    logging.info(
        "Finished validation. Valid rows: %d, Errors: %d", 
        len(valid_rows), 
        len(errors),
    )
    return valid_rows, errors 


