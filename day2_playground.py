def sum_list(values):
    total = 0

    for v in values:
        total += v

    return total 

def safe_read_first_line(path): 
    
    try: 
        with open(path, "r", encoding="utf-8") as f:
            return f.readline().strip()
        
    except FileNotFoundError: 
        return "MISSING"  
    
    