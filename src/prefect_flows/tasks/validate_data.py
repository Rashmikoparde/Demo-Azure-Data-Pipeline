# src/prefect_flows/tasks/validate_data.py
from prefect import task
import pandas as pd
import sys
import os

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.append(current_dir)


@task
def validate_data(df: pd.DataFrame, config: dict):
    """Validate data against configuration rules."""
    print("Starting data validation...")
    
    validation_results = {
        "passed": True,
        "errors": [],
        "warnings": []
    }
    
    # Check required columns
    required_cols = config.get('required_columns', [])
    missing_cols = [col for col in required_cols if col not in df.columns]
    
    if missing_cols:
        validation_results["passed"] = False
        validation_results["errors"].append(f"Missing required columns: {missing_cols}")
    
    # Validate value ranges
    valid_ranges = config.get('valid_ranges', {})
    for col, ranges in valid_ranges.items():
        if col in df.columns:
            min_val = ranges.get('min')
            max_val = ranges.get('max')
            
            out_of_range = df[(df[col] < min_val) | (df[col] > max_val)]
            if len(out_of_range) > 0:
                validation_results["warnings"].append(
                    f"{len(out_of_range)} rows in {col} out of range [{min_val}, {max_val}]"
                )
    
    print(f"Validation completed: {'PASSED' if validation_results['passed'] else 'FAILED'}")
    return validation_results