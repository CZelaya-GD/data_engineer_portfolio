import pandas as pd

# First ETL pipeline

def clean_csv(input_file, output_file):
    # Extract: Read raw data
    df = pd.read_csv(input_file)
    print(f"Raw: {len(df)} rows")

    #Transform: Drop bad data + convert age to INT
    df = df.dropna().drop_duplicates()
    df["age"] = df["age"].astype(int) # 🔑 This fixes float → 30 (not 30.0)
    print(f"Clean: {len(df)} rows")

    #Load: Save clean data
    df.to_csv(output_file, index=False)
    print(f"Saved to {output_file}")
    return df


# Creating dummy test data
if __name__ == "__main__":

    # REMOVE for production
    print("Creating test data...")
    test_data = pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie","", "Bob"],
        "age": [22,None, 25, 35, 31]
    })

    test_data.to_csv("input.csv", index=False)

    # Run ETL
    clean_csv("input.csv", "output.csv")
