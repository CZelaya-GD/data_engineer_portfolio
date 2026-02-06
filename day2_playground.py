# summed total of numbers in a list
def sum_list(values):
    total = 0

    for v in values:
        total += v

    return total 


# Created to see if a file exists
def safe_read_first_line(path): 
    
    try: 
        with open(path, "r", encoding="utf-8") as f:
            return f.readline().strip()
        
    except FileNotFoundError: 
        return "MISSING"  
    
    