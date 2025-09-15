# src/prefect_flows/flows/data_ingestion_flow.py
import sys
import os
from prefect import flow, get_run_logger

# Add the src directory to Python path
src_path = os.path.join(os.path.dirname(__file__), '..', '..')
if src_path not in sys.path:
    sys.path.append(src_path)

# Now import using absolute paths from project root
from src.prefect_flows.tasks.get_config import get_config
from src.prefect_flows.tasks.load_data import load_data
from src.prefect_flows.tasks.extract_metadata import extract_metadata
from src.prefect_flows.tasks.cleanse_data import cleanse_data
from src.prefect_flows.tasks.validate_data import validate_data
from src.prefect_flows.tasks.save_data import save_data

@flow(name="sensor-data-ingestion-flow")
def data_ingestion_flow(file_path: str):
    """Main flow to orchestrate the entire data ingestion pipeline."""
    logger = get_run_logger()
    logger.info(f"Starting data ingestion flow for file: {file_path}")
    
    try:
        # 1. Get configuration
        logger.info("Step 1: Loading configuration...")
        config = get_config()
        
        # 2. Load raw data
        logger.info("Step 2: Loading raw data...")
        raw_df = load_data(file_path)
            
        # 3. Extract metadata
        logger.info("Step 3: Extracting metadata...")
        # In the data_ingestion_flow function, update the metadata extraction call:
        metadata = extract_metadata(raw_df, file_path)  # Pass the full file_path
        
        # 4. Cleanse data
        logger.info("Step 4: Cleansing data...")
        cleansed_df = cleanse_data(raw_df)
        
        # 5. Validate data
        logger.info("Step 5: Validating data...")
        validation_results = validate_data(cleansed_df, config)
        
        if not validation_results["passed"]:
            logger.error(f"Data validation failed: {validation_results['errors']}")
        
        # 6. Save processed data
        logger.info("Step 6: Saving processed data...")
        output_path = save_data(cleansed_df, metadata)
        
        logger.info(f"Data ingestion completed successfully! Output: {output_path}")
        logger.info(f"Validation warnings: {validation_results['warnings']}")
        
        return {
            "status": "success",
            "output_path": output_path,
            "metadata": metadata,
            "validation": validation_results
        }
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}")
        return {
            "status": "failed",
            "error": str(e)
        }