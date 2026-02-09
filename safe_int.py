from typing import Optional, Callable, Any
import logging 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def safe_int(
        value: Any, 
        default: Optional[int] = None,
        transform: Optional[Callable[[int], int]] = None 
) -> Optional[int]:
    
    """
    Docstring for safe_int
    Convert an arbitrary value to an integer with robust error handling.

    Purpose: 
        Safely parse values (e.g., from CSVs, APIs, or user input) into integers
        without crashing the application, while providing conistent logging
        and an oprional transformation step. 

    Inputs: 
        Value: 
            Any incoming value to convert (str, float, int, etc). 

        default: 
            Fallback value returned when conversion fails or value is None. 
            If default is None and conversion fails, the function returns None. 

        transform: 
            Optional callable applied to the successfully parsed integer 
            (e.g., to clamp to a range or apply business rules). 


    Output:
        Optional[int]:
            Parsed integer after optionnal transsformation, the provided default, 
            or None when parsing fails and no default is given. 

    Raises:
        ValueError: 
            Only raised if parsing succeeds but the transform function itself 
            raises a ValueError. Conversion errors are handled internally. 

    Usage:
        - Use in data cleaning pipelines when reading numeric columns from CSVs. 
        - Centralize integer parsing logic to avoid repeated try/except blocks. 
        - Combine with higher-level ETL functions for robust data ingestion. 

    Examples: 
        safe_int("42") -> 42
        safe_int("not_a_number", default=0) -> 0
        safe_int("10", transform=lambda x: x if x >= 0 else 0) -> 10
    """

    if value is None:
        logger.info("safe_int received None, returning default.", extra={"default": default})
        return default 
    
    try: 

        # Handle float explicitly for truncation
        if isinstance(value, (float, str)):
            parsed = int(float(str(value.strip())))

        else:
            parsed = int(str(value).strip())

        logger.info("Successfully parsed value to int.", extra={"raw_value": value, "parsed": parsed})

    except (TypeError, ValueError) as exc:
        logger.error("Failed to parse value to int.", extra={"raw_value": value, "error_type": type(exc).__name__})

        return default
    
    if transform is None:
        return parsed
    
    try:
        transformed = transform(parsed)
        logger.info(
            "Successfully transformed parsed int.",
            extra={"parsed": parsed, "transformed": transformed}
        )

    except ValueError as exc:
        logger.error(
            "Transform function raised ValueError.",
            extra={"parsed": parsed, "error_type": type(exc).__name__}
        )

        raise


if __name__ == "__main__":
    # Happy paths
    assert safe_int("42") == 42
    assert safe_int("3.14", default=0) == 3 # Truncates float
    assert safe_int("abc", default=0) == 0

    # Edge cases
    assert safe_int(None) is None
    assert safe_int("", default=-1) == -1

    # Transform 
    assert safe_int("4", transform=lambda x: x * x) == 16

    print("All tests passed! ✅")