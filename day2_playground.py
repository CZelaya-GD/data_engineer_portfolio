# summed total of numbers in a list
def sum_list(values):
    """summed total of all numbers inside a list.
    arg: list of numbers"""
    total = 0

    for v in values:
        total += v

    return total 


# Created to see if a file exists
def safe_read_first_line(path): 
    """Reads the lines inside of a file.
    arg: file path"""
    try: 
        with open(path, "r", encoding="utf-8") as f:
            return f.readline().strip()
        
    except FileNotFoundError: 
        return "MISSING"  
    
    