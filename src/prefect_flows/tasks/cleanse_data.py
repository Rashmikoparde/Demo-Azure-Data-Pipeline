# src/prefect_flows/tasks/cleanse_data.py

import sys
import os
from prefect import flow, get_run_logger, task

# Add the src directory to Python path
src_path = os.path.join(os.path.dirname(__file__), '..', '..')
if src_path not in sys.path:
    sys.path.append(src_path)
import pandas as pd

@task
def cleanse_data(df: pd.DataFrame):
    """Clean and preprocess the sensor data."""
    print("Starting data cleansing...")
    
    # Make a copy to avoid modifying the original
    df_clean = df.copy()
    
    # Handle missing values - fill with mean for numeric columns
    numeric_cols = df_clean.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        if df_clean[col].isnull().sum() > 0:
            mean_val = df_clean[col].mean()
            df_clean[col].fillna(mean_val, inplace=True)
            print(f"Filled missing values in {col} with mean: {mean_val:.2f}")
    
    # Ensure 'fail' column is integer (0 or 1)
    if 'fail' in df_clean.columns:
        df_clean['fail'] = df_clean['fail'].astype(int)
        print("Converted 'fail' column to integer")
    
    print("Data cleansing completed")
    return df_clean