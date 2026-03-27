"""
Unified CLI for all ETL (local + cloud).
"""

import click 
from etl.etl_runner import run_etl, save_warehouse
from etlpipeline.gcp.bigquery_sync import sync_hn_posts_to_bigquery

@click.command()
@click.option('--days-back', default=30, type=int)
@click.option('--target', default='local')

def main(days_back: int, target: str): 
    
    df = run_etl(days_back)

    if target == "local": 
        save_warehouse(df)

    elif target == "bigquery": 
        sync_hn_posts_to_bigquery

    else: 
        raise ValueError("target: 'local' or 'bigquery'")
    
if __name__ == "__main__": 
    main()
