import pandas as pd
from prefect import task
import sys
import os

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)


@task
def load_data(file_path: str):
    """Load data from CSV file into pandas DataFrame."""
    print(f"Loading data from: {file_path}")
    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} rows with {len(df.columns)} columns")
    return df