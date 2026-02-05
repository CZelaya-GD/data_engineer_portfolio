import pandas as pd

def clean_csv(input_file, output_file):

    df = pd.read_csv(input_file)
    df = df.dropna(), df.drop_duplicates()
    df.to_csv(output_file, index=False)

    print(f"Cleaned {len(df)} rows to {output_file}")

