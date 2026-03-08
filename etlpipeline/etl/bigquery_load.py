import pandas as pd
from google.cloud import bigquery

client = bigquery.Client(project="czelaya-hn-sandbox")
df = pd.read_csv("data/hn_export.csv")

table_id = "czelaya-hn-sandbox.hn_warehouse.hn_posts"
job = client.load_table_from_dataframe(df, table_id)
job.result()
print(f"✅ Loaded {job.output_rows} rows to BigQuery sandbox!")
