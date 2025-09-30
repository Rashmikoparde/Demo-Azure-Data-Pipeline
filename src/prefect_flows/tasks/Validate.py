# src/prefect_flows/tasks/validate_sensor_data.py
import sys
import os
from prefect import task
import pandas as pd
import json
from datetime import datetime

@task
def validate_sensor_data(df):
    """Validate sensor data with specific rules for each column."""
    print("Starting sensor data validation...")
    #df = pd.read_csv(csv_path)
    validation_results = {
        "success": True,
        "errors": [],
        "warnings": [],
        "column_stats": {},
        "summary": {
            "total_rows": len(df),
            "valid_rows": 0,
            "invalid_rows": 0
        }
    }
    
    try:
        # Define expected columns and their validation rules
        expected_columns = [
            'footfall', 'tempMode', 'AQ', 'USS', 'CS', 'VOC', 'RP', 'IP', 'Temperature', 'fail'
        ]
        
        # Check if all expected columns exist
        missing_columns = [col for col in expected_columns if col not in df.columns]
        if missing_columns:
            validation_results["errors"].append(f"Missing required columns: {missing_columns}")
            validation_results["success"] = False
            return validation_results
        
        # Validate each column
        validation_rules = {
            'footfall': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 10000,
                'description': 'Number of people detected'
            },
            'tempMode': {
                'type': 'categorical', 
                'allowed_values': [1, 2, 3, 4, 5, 6, 7],
                'description': 'Temperature mode setting'
            },
            'AQ': {
                'type': 'numeric',
                'min_value': 1,
                'max_value': 10,
                'description': 'Air Quality index'
            },
            'USS': {
                'type': 'numeric', 
                'min_value': 1,
                'max_value': 10,
                'description': 'Ultrasonic sensor reading'
            },
            'CS': {
                'type': 'numeric',
                'min_value': 1, 
                'max_value': 10,
                'description': 'Current sensor reading'
            },
            'VOC': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 10, 
                'description': 'Volatile Organic Compounds level'
            },
            'RP': {
                'type': 'numeric',
                'min_value': 0,
                'max_value': 100,
                'description': 'Relative Pressure'
            },
            'IP': {
                'type': 'numeric',
                'min_value': 1,
                'max_value': 10,
                'description': 'Input Power'
            },
            'Temperature': {
                'type': 'numeric',
                'min_value': -50,
                'max_value': 100,
                'description': 'Temperature in Celsius'
            },
            'fail': {
                'type': 'binary',
                'allowed_values': [0, 1],
                'description': 'Failure indicator (0=normal, 1=failed)'
            }
        }
        
        # Perform column-wise validation
        valid_rows_mask = pd.Series([True] * len(df))
        
        for column, rules in validation_rules.items():
            col_validation = {
                'total_count': len(df[column]),
                'null_count': df[column].isnull().sum(),
                'invalid_count': 0,
                'min_value': None,
                'max_value': None,
                'unique_values': None
            }
            
            # Check for null values
            if col_validation['null_count'] > 0:
                validation_results["warnings"].append(f"Column '{column}' has {col_validation['null_count']} null values")
            
            # Type-specific validation
            if rules['type'] == 'numeric':
                col_validation['min_value'] = df[column].min()
                col_validation['max_value'] = df[column].max()
                
                # Check value ranges
                if 'min_value' in rules and df[column].min() < rules['min_value']:
                    invalid_mask = df[column] < rules['min_value']
                    col_validation['invalid_count'] += invalid_mask.sum()
                    valid_rows_mask &= ~invalid_mask
                    validation_results["errors"].append(
                        f"Column '{column}' has values below minimum {rules['min_value']}: min={df[column].min()}"
                    )
                
                if 'max_value' in rules and df[column].max() > rules['max_value']:
                    invalid_mask = df[column] > rules['max_value']
                    col_validation['invalid_count'] += invalid_mask.sum()
                    valid_rows_mask &= ~invalid_mask
                    validation_results["errors"].append(
                        f"Column '{column}' has values above maximum {rules['max_value']}: max={df[column].max()}"
                    )
            
            elif rules['type'] == 'categorical':
                col_validation['unique_values'] = df[column].unique().tolist()
                invalid_values = [val for val in df[column].unique() if val not in rules['allowed_values']]
                if invalid_values:
                    invalid_mask = df[column].isin(invalid_values)
                    col_validation['invalid_count'] += invalid_mask.sum()
                    valid_rows_mask &= ~invalid_mask
                    validation_results["errors"].append(
                        f"Column '{column}' has invalid values: {invalid_values}. Allowed: {rules['allowed_values']}"
                    )
            
            elif rules['type'] == 'binary':
                col_validation['unique_values'] = df[column].unique().tolist()
                invalid_values = [val for val in df[column].unique() if val not in rules['allowed_values']]
                if invalid_values:
                    invalid_mask = df[column].isin(invalid_values)
                    col_validation['invalid_count'] += invalid_mask.sum()
                    valid_rows_mask &= ~invalid_mask
                    validation_results["errors"].append(
                        f"Column '{column}' has invalid values: {invalid_values}. Must be {rules['allowed_values']}"
                    )
            
            validation_results["column_stats"][column] = col_validation
        
        # Update summary statistics
        validation_results["summary"]["valid_rows"] = valid_rows_mask.sum()
        validation_results["summary"]["invalid_rows"] = (~valid_rows_mask).sum()
        validation_results["summary"]["valid_percentage"] = (valid_rows_mask.sum() / len(df)) * 100
        
        # Determine overall success
        if validation_results["errors"]:
            validation_results["success"] = False
        elif validation_results["summary"]["valid_percentage"] < 95:  # Allow 5% invalid rows
            validation_results["success"] = False
            validation_results["errors"].append(f"Too many invalid rows: {validation_results['summary']['valid_percentage']:.1f}% valid")
        
        print(f"Validation completed: {validation_results['success']}")
        print(f"Valid rows: {validation_results['summary']['valid_rows']}/{validation_results['summary']['total_rows']} "
              f"({validation_results['summary']['valid_percentage']:.1f}%)")
        
        return validation_results
        
    except Exception as e:
        error_msg = f"Validation error: {str(e)}"
        print(error_msg)
        validation_results["success"] = False
        validation_results["errors"].append(error_msg)
        return validation_results

@task
def save_validation_report(validation_results, file_path):
    """Save validation results to a JSON report with proper type handling."""
    try:
        reports_folder = "./data/reports"
        os.makedirs(reports_folder, exist_ok=True)
        
        # Convert problematic numpy/pandas types to native Python types
        def clean_for_json(obj):
            if hasattr(obj, 'item'):  # numpy types
                return obj.item()
            elif hasattr(obj, 'tolist'):  # pandas Series, numpy arrays
                return obj.tolist()
            elif isinstance(obj, dict):
                return {k: clean_for_json(v) for k, v in obj.items()}
            elif isinstance(obj, (list, tuple)):
                return [clean_for_json(item) for item in obj]
            else:
                return obj
        
        cleaned_results = clean_for_json(validation_results)
        
        # Create report
        filename = os.path.basename(file_path)
        report = {
            "validation_report": {
                "timestamp": datetime.now().isoformat(),
                "file_validated": filename,
                "overall_status": "PASS" if cleaned_results.get("success") else "FAIL",
                "summary": cleaned_results.get("summary", {}),
                "errors": cleaned_results.get("errors", []),
                "warnings": cleaned_results.get("warnings", []),
                "column_statistics": cleaned_results.get("column_stats", {})
            }
        }
        
        # Save report
        report_filename = f"validation_{os.path.splitext(filename)[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_path = os.path.join(reports_folder, report_filename)
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"Validation report saved: {report_path}")
        return report_path
        
    except Exception as e:
        print(f"Error saving validation report: {e}")
        return None