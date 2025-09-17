# test_flow_simple.py
import sys
import os

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)

try:
    # Import the flow directly
    from src.prefect_flows.flows.data_ingestion_flow import data_ingestion_flow
    print("Successfully imported flow")
    
    # Test the flow     
    result = data_ingestion_flow("./data/raw/data.csv")
    #print(f"Flow result: {result}")
    
except ImportError as e:
    print(f"Import failed: {e}")
    print("Trying alternative import method...")
    
    # Alternative: import by file path
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "data_ingestion_flow", 
        os.path.join(current_dir, "src", "prefect_flows", "flows", "data_ingestion_flow.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    result = module.data_ingestion_flow("./data/sample_data.csv")
    print(f"Flow result: {result}")