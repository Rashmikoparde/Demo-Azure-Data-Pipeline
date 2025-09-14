# src/prefect_flows/tasks/load_data.py
import pandas as pd
from prefect import task

@task
def load_data(file_path: str):
    """Load data from CSV file into pandas DataFrame."""
    print(f"Loading data from: {file_path}")
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows with {len(df.columns)} columns")
    return df