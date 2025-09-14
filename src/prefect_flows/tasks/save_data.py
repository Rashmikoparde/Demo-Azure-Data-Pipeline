# src/prefect_flows/tasks/save_data.py
from prefect import task
import pandas as pd
import os

@task
def save_data(df: pd.DataFrame, metadata: dict, output_dir: str = "./data/cleansed"):
    """Save processed data as Parquet file."""
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate output filename
    input_filename = metadata['file_name'].replace('.csv', '')
    output_filename = f"{input_filename}_processed.parquet"
    output_path = os.path.join(output_dir, output_filename)
    
    # Save as Parquet
    df.to_parquet(output_path, index=False)
    print(f"Data saved as Parquet: {output_path}")
    
    return output_path