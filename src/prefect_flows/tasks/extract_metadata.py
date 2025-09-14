# src/prefect_flows/tasks/extract_metadata.py
from prefect import task
import pandas as pd

@task
def extract_metadata(df: pd.DataFrame, file_name: str):
    """Extract metadata from the DataFrame."""
    metadata = {
        "file_name": file_name,
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