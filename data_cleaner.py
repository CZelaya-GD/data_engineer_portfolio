import pandas as pd 
import csv

class DataCleaner: 

    def __init__(self, input_file):
        self.input_file = input_file
        self.data = None

        try: 
            self.data = pd.read_csv(input_file)

        except FileNotFoundError:
            print(f"Error: {input_file} not found.")

    def clean_data(self):
        if self.data is None:
            return None
        
        # Using Day 1 logic: drop nulls, fix types
        self.data = self.data.dropna()
        self.data["price"] = pd.to_numeric(self.data["price"], errors="coerce")
        return self.data
    
    def save_cleaned(self, output_file):
        if self.data is not None:
            self.data.to_csv(output_file, index=False)
            print(f"Saved data to {output_file}")

# test it 

cleaner = DataCleaner("data/input/sales_data.csv")
cleaner.clean_data()
cleaner.save_cleaned("data/output/day4_cleaned_sales.csv")