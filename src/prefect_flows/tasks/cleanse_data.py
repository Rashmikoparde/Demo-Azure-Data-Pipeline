# src/prefect_flows/tasks/cleanse_data.py
import sys
import os
from prefect import task, get_run_logger
import pandas as pd
from datetime import datetime

#from src.prefect_flows.tasks.validate_data import validate_data_with_great_expectations


@task
def cleanse_data(csv_file_path: str,  config: dict):
    """Clean and preprocess the sensor data and save to cleansed folder."""
    print("Starting data cleansing...")
    logger = get_run_logger()
    try:
        # Read the CSV file
        df_clean = pd.read_csv(csv_file_path)

        # Strip column names (accidental spaces, etc.)
        df_clean.columns = df_clean.columns.str.strip()

        # Drop duplicates
        initial_len = len(df_clean)
        df_clean.drop_duplicates(inplace=True)
        print(f"Removed {initial_len - len(df_clean)} duplicate rows")

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

        
        # Return the path to the cleansed file
        return df_clean
        
    except Exception as e:
        print(f"Error during data cleansing: {str(e)}")
        raise