# src/prefect_flows/tasks/get_config.py
import sys
import os

# Add the src directory to Python path
src_path = os.path.join(os.path.dirname(__file__), '..', '..')
if src_path not in sys.path:
    sys.path.append(src_path)

from prefect import task

@task
def get_config():
    """Load configuration for data validation rules based on your actual dataset."""
    config = {
        "valid_ranges": {
            "footfall": {"min": 0, "max": 1000},
            "tempMode": {"min": 0, "max": 10},
            "AQ": {"min": 0, "max": 10},
            "USS": {"min": 0, "max": 10},
            "CS": {"min": 0, "max": 10},
            "VOC": {"min": 0, "max": 100},
            "RP": {"min": 0, "max": 100},
            "IP": {"min": 0, "max": 10},
            "Temperature": {"min": 0, "max": 50}
        },
        "required_columns": [
            "footfall", "tempMode", "AQ", "USS", "CS", 
            "VOC", "RP", "IP", "Temperature", "fail"
        ],
        "categorical_columns": {
            "tempMode": [0, 1, 2, 3, 4, 5, 6, 7],
            "fail": [0, 1]
        }
    }
    return config