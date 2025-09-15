# src/prefect_flows/tasks/extract_metadata.py
from prefect import task
import pandas as pd
import os

@task
def extract_metadata(df: pd.DataFrame, file_path: str):
    """Extract metadata from the DataFrame."""
    # Get just the filename without path
    file_name = os.path.basename(file_path)
    
    metadata = {
        "file_name": file_name,  # Store only filename, not full path
        "file_path": file_path,  # Store full path separately if needed
        "row_count": len(df),
        "column_count": len(df.columns),
        "column_names": list(df.columns),
        "data_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "missing_values": df.isnull().sum().to_dict(),
        "failure_rate": df['fail'].mean() if 'fail' in df.columns else 0,
        "unique_fail_counts": df['fail'].value_counts().to_dict() if 'fail' in df.columns else {}
    }
    print(f"Metadata extracted: {len(df)} rows, {len(df.columns)} columns")
    print(f"Failure rate: {metadata['failure_rate']:.2%}")
    return metadata