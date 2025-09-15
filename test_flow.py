# test_flow.py
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from prefect import flow
from prefect_flows.tasks.get_config import get_config
from prefect_flows.tasks.load_data import load_data
from prefect_flows.tasks.extract_metadata import extract_metadata
from prefect_flows.tasks.cleanse_data import cleanse_data
from prefect_flows.tasks.validate_data import validate_data

@flow
def test_flow():
    """Test flow to verify all tasks work together."""
    print("Testing data processing tasks within a flow...")
    
    # Test config
    config = get_config()
    print("✅ Config loaded")
    
    # Test loading data
    df = load_data("./data/raw/data.csv")
    print("✅ Data loaded")
    
    # Test metadata extraction
    metadata = extract_metadata(df, "data.csv")
    print("✅ Metadata extracted")
    
    # Test data cleansing
    df_clean = cleanse_data(df)
    print("✅ Data cleansed")
    
    # Test validation
    validation = validate_data(df_clean, config)
    print("✅ Data validated")
    
    print(f"Validation results: {validation}")
    print("All tasks working correctly within flow!")
    return validation

if __name__ == "__main__":
    # This will run the flow
    result = test_flow()
    print(f"Flow completed with result: {result}")