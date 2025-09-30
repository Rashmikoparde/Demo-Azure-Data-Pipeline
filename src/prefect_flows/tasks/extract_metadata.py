# src/prefect_flows/tasks/extract_metadata.py
from prefect import task
import json
import pandas as pd
import os
import shutil
from datetime import datetime

@task
def extract_metadata(file_path, raw_folder="./data/raw"):
    """Extract metadata from the DataFrame."""
    os.makedirs(raw_folder, exist_ok=True)
    metadata_folder = os.path.join(raw_folder, "metadata")
    os.makedirs(metadata_folder, exist_ok=True)
    
    try:
        # Read CSV file
        df = pd.read_csv(file_path)
        file_name = os.path.basename(file_path)
        
        # Save raw CSV file to raw folder
        raw_file_path = os.path.join(raw_folder, file_name)
        shutil.copy2(file_path, raw_file_path)
        
        # Extract basic metadata
        metadata = {
            "file_name": file_name,
            "file_info": {
                "file_size_bytes": os.path.getsize(file_path),
                "saved_path": raw_file_path
            },
            "data_structure": {
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns)
            }
        }
        
        # Save metadata to metadata folder
        metadata_file = f"{os.path.splitext(file_name)[0]}_metadata.json"
        metadata_path = os.path.join(metadata_folder, metadata_file)
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"File saved to Raw Folder: {raw_file_path}")
        print(f"Metadata saved: {metadata_path}")
        
        return metadata, raw_file_path  # Always returns two values
        
    except Exception as e:
        print(f"Error in extract_metadata: {str(e)}")
        # Return two values even in error case to maintain consistency
        error_metadata = {
            "error": str(e),
            "file_path": file_path,
            "timestamp": datetime.now().isoformat()
        }
        return error_metadata, file_path  # Still returns two values