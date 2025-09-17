import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.prefect_flows.flows.data_ingestion_flow import data_ingestion_flow

def test_complete_flow():
    """Test the complete data ingestion flow."""
    print("Testing complete data ingestion flow...")
    
    # Test with our data
    result = data_ingestion_flow("./data/raw/data.csv")
    
    print(f"Flow completed with status: {result['status']}")
    if result['status'] == 'success':
        print(f"Output file: {result['output_path']}")
        print(f"Processed rows: {result['metadata']['row_count']}")
        print(f"Validation passed: {result['validation']['passed']}")
    else:
        print(f"Error: {result['error']}")
    
    return result

if __name__ == "__main__":
    test_complete_flow()