import sys
import os
from prefect import flow, get_run_logger
from src.prefect_flows.tasks.Validate import validate_sensor_data, save_validation_report
from src.prefect_flows.tasks.save_data import save_data

# Add the src directory to Python path
src_path = os.path.join(os.path.dirname(__file__), '..', '..')
if src_path not in sys.path:
    sys.path.append(src_path)

# Now import using absolute paths from project root
from src.prefect_flows.tasks.get_config import get_config
from src.prefect_flows.tasks.load_data import load_data
from src.prefect_flows.tasks.extract_metadata import extract_metadata
from src.prefect_flows.tasks.cleanse_data import cleanse_data
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
        
        # Extract metadata
        logger.info("Step 3: Saving raw data and metadata to raw folder...")
        # In the data_ingestion_flow function, update the metadata extraction call:
        metadata, raw_file_path = extract_metadata(file_path, raw_folder="./data/raw")  # Pass the full file_path
        
        # Cleanse data
        logger.info("Step 4: Cleansing data...")
        df = cleanse_data(raw_file_path, config)
                
        # Validate data

        logger.info("Step : Validating data...")
        validation_results = validate_sensor_data(df)
        
        logger.info("Step 4: Saving validation report...")
        report_path = save_validation_report(validation_results, file_path)
        logger.info(f"Validation successfully completed! Output: {report_path}")
        
        # 5. Save processed data only if validation passes
        if validation_results["success"]:
            logger.info("Step 5: Saving processed data...")
            processed_path = save_data(df, metadata)
        else:
            processed_path = None
            logger.warning("Validation failed - data not promoted to processed folder")

        logger.info(f"Data ingestion completed successfully! Output: {processed_path}")
        
        return {
            "status": "success",
            "output_path": processed_path,
            "metadata": metadata,
            "validation": validation_results
        }
        
    except Exception as e:
        logger.error(f"Data ingestion failed: {str(e)}")
        return {
            "status": "failed",
            "error": str(e)
        }