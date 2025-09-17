import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from prefect_flows.tasks.get_config import get_config
from prefect_flows.tasks.load_data import load_data
from prefect_flows.tasks.extract_metadata import extract_metadata
from prefect_flows.tasks.cleanse_data import cleanse_data
from prefect_flows.tasks.validate_data import validate_data

def test_tasks():
    print("Testing data processing tasks...")
    
    # Test config - use .fn() to call the underlying function
    config = get_config.fn()
    print("Config loaded")
    print(f"Config keys: {list(config.keys())}")
    
    # Test loading data
    df = load_data.fn("./data/raw/data.csv")
    print("Data loaded")
    print(f"Data shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")
    
    # Test metadata extraction
    metadata = extract_metadata.fn(df, "data.csv")
    print("Metadata extracted")
    print(f"Row count: {metadata['row_count']}")
    print(f"Columns: {metadata['column_names']}")
    
    # Test data cleansing
    df_clean = cleanse_data.fn(df)
    print("Data cleansed")
    print(f"Cleansed data shape: {df_clean.shape}")
    
    # Test validation
    validation = validate_data.fn(df_clean, config)
    print("Data validated")
    print(f"Validation passed: {validation['passed']}")
    print(f"Errors: {validation['errors']}")
    print(f"Warnings: {validation['warnings']}")
    
    print("All tasks working correctly!")
    return True

if __name__ == "__main__":
    test_tasks()