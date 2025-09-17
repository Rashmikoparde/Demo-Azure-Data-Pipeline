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
from src.prefect_flows.tasks.validate_data import validate_data_with_great_expectations
from src.prefect_flows.tasks.save_data import save_data

@flow(name="sensor-data-ingestion-flow")
def data_ingestion_flow(file_path: str):
    """Main flow to orchestrate the entire data ingestion pipeline."""
    logger = get_run_logger()
    logger.info(f"Starting data ingestion flow for file: {file_path}")
    
    try:
        # Get configuration
        logger.info("Step 1: Loading configuration...")
        config = get_config()
        
        # Load raw data
        logger.info("Step 2: Loading raw data...")
        raw_df = load_data(file_path)

        # Extract metadata
        logger.info("Step 3: Extracting metadata...")
        # In the data_ingestion_flow function, update the metadata extraction call:
        metadata = extract_metadata(raw_df, file_path)  # Pass the full file_path
        
        # Cleanse data
        logger.info("Step 4: Cleansing data...")
        cleansed_df = cleanse_data(raw_df)
        
        # Validate data
        logger.info("Step 5: Validating data...")
        validation_results = validate_data_with_great_expectations(cleansed_df, config)
        
        
        # Save processed data
        logger.info("Step 6: Saving processed data...")
        output_path = save_data(cleansed_df, metadata)
        
        logger.info(f"Data ingestion completed successfully! Output: {output_path}")
        
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