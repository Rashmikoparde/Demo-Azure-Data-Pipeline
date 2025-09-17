# src/prefect_flows/tasks/validate_data.py
from prefect import task
import pandas as pd
import great_expectations as ge
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.core.expectation_suite import ExpectationSuite
import json
import os

@task
def validate_data(df: pd.DataFrame, config: dict):
    """Validate data using Great Expectations with comprehensive validation suite."""
    print("Starting Great Expectations validation...")
    
    # Convert DataFrame to Great Expectations DataFrame
    gdf = ge.from_pandas(df)
    
    # Create expectation suite
    expectation_suite = create_expectation_suite(config)
    
    # Run validation
    validation_results = gdf.validate(
        expectation_suite=expectation_suite,
        result_format="COMPLETE",
        only_return_failures=False
    )
    
    # Generate comprehensive validation report
    validation_report = generate_validation_report(validation_results, df)
    
    print("Validation completed")
    return validation_report

def create_expectation_suite(config: dict) -> ExpectationSuite:
    """Create a comprehensive expectation suite for sensor data."""
    
    expectations = []
    
    # 1. Column existence expectations
    for column in config.get('required_columns', []):
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_to_exist",
                kwargs={"column": column}
            )
        )
    
    # 2. Data type expectations
    type_expectations = {
        'footfall': 'int64',
        'tempMode': 'int64',
        'AQ': 'int64',
        'USS': 'int64',
        'CS': 'int64',
        'VOC': 'int64',
        'RP': 'int64',
        'IP': 'int64',
        'Temperature': 'float64',
        'fail': 'int64'
    }
    
    for column, dtype in type_expectations.items():
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_of_type",
                kwargs={"column": column, "type_": dtype}
            )
        )
    
    # 3. Value range expectations
    valid_ranges = config.get('valid_ranges', {})
    for column, ranges in valid_ranges.items():
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_between",
                kwargs={
                    "column": column,
                    "min_value": ranges.get('min'),
                    "max_value": ranges.get('max')
                }
            )
        )
    
    # 4. Non-null expectations
    for column in config.get('required_columns', []):
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={"column": column}
            )
        )
    
    # 5. Unique identifier expectations
    expectations.append(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={"column": "UDI"}
        )
    )
    
    expectations.append(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "UDI"}
        )
    )
    
    # 6. Categorical value expectations
    categorical_values = config.get('categorical_columns', {})
    for column, allowed_values in categorical_values.items():
        expectations.append(
            ExpectationConfiguration(
                expectation_type="expect_column_values_to_be_in_set",
                kwargs={"column": column, "value_set": allowed_values}
            )
        )
    
    # 7. Statistical expectations
    expectations.append(
        ExpectationConfiguration(
            expectation_type="expect_column_mean_to_be_between",
            kwargs={"column": "fail", "min_value": 0, "max_value": 1}
        )
    )
    
    # Create and return expectation suite
    suite = ExpectationSuite(
        expectation_suite_name="sensor_data_validation_suite",
        expectations=expectations,
        meta={
            "description": "Comprehensive validation suite for industrial sensor data",
            "config": config
        }
    )
    
    return suite

def generate_validation_report(validation_results: dict, df: pd.DataFrame) -> dict:
    """Generate comprehensive validation report."""
    
    # Basic success metrics
    success = validation_results["success"]
    successful_expectations = sum(1 for result in validation_results["results"] if result["success"])
    total_expectations = len(validation_results["results"])
    
    # Detailed failure analysis
    failed_expectations = []
    for result in validation_results["results"]:
        if not result["success"]:
            failed_expectations.append({
                "expectation_type": result["expectation_config"]["expectation_type"],
                "column": result["expectation_config"]["kwargs"].get("column", "N/A"),
                "failed_count": result["result"].get("unexpected_count", 0),
                "failure_percentage": result["result"].get("unexpected_percent", 0),
                "details": result["result"]
            })
    
    # Data quality score
    quality_score = calculate_data_quality_score(validation_results, df)
    
    # Generate HTML report (optional)
    generate_html_report(validation_results)
    
    return {
        "summary": {
            "success": success,
            "successful_expectations": successful_expectations,
            "total_expectations": total_expectations,
            "success_rate": (successful_expectations / total_expectations) * 100,
            "data_quality_score": quality_score,
            "failed_expectations_count": len(failed_expectations)
        },
        "failed_expectations": failed_expectations,
        "validation_results": validation_results,
        "recommendations": generate_recommendations(failed_expectations)
    }

def calculate_data_quality_score(validation_results: dict, df: pd.DataFrame) -> float:
    """Calculate comprehensive data quality score (0-100)."""
    
    base_score = (sum(1 for r in validation_results["results"] if r["success"]) / 
                 len(validation_results["results"])) * 100
    
    # Penalties for critical failures
    critical_penalty = 0
    for result in validation_results["results"]:
        if not result["success"]:
            expectation_type = result["expectation_config"]["expectation_type"]
            if "null" in expectation_type or "exist" in expectation_type:
                critical_penalty += 10  # Major penalty for nulls or missing columns
            elif "unique" in expectation_type:
                critical_penalty += 5   # Moderate penalty for uniqueness issues
    
    # Bonus for large datasets
    size_bonus = min(len(df) / 1000, 5)  # Up to 5 points for large datasets
    
    final_score = max(0, base_score - critical_penalty + size_bonus)
    return round(final_score, 2)

def generate_recommendations(failed_expectations: list) -> list:
    """Generate actionable recommendations based on validation failures."""
    
    recommendations = []
    
    for failure in failed_expectations:
        if failure["expectation_type"] == "expect_column_values_to_not_be_null":
            recommendations.append(
                f"Column '{failure['column']}' has {failure['failed_count']} null values. "
                f"Consider imputation or investigation."
            )
        
        elif failure["expectation_type"] == "expect_column_values_to_be_between":
            recommendations.append(
                f"Column '{failure['column']}' has {failure['failed_count']} values outside expected range. "
                f"Review data collection process."
            )
        
        elif failure["expectation_type"] == "expect_column_values_to_be_in_set":
            recommendations.append(
                f"Column '{failure['column']}' has {failure['failed_count']} invalid categorical values. "
                f"Update data entry validation rules."
            )
    
    return recommendations

def generate_html_report(validation_results: dict):
    """Generate HTML validation report for documentation."""
    
    report_dir = "validation_reports"
    os.makedirs(report_dir, exist_ok=True)
    
    # Simple HTML report
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Data Validation Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; }}
            .success {{ color: green; }}
            .failure {{ color: red; }}
            .warning {{ color: orange; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
        </style>
    </head>
    <body>
        <h1>Data Validation Report</h1>
        <p>Generated on: {pd.Timestamp.now()}</p>
        
        <h2>Summary</h2>
        <p>Overall Success: <span class="{'success' if validation_results['success'] else 'failure'}">
            {validation_results['success']}
        </span></p>
        <p>Successful Expectations: {sum(1 for r in validation_results['results'] if r['success'])} / {len(validation_results['results'])}</p>
        
        <h2>Detailed Results</h2>
        <table>
            <tr>
                <th>Expectation</th>
                <th>Status</th>
                <th>Details</th>
            </tr>
            {"".join([
                f"<tr><td>{r['expectation_config']['expectation_type']}</td>"
                f"<td class={'success' if r['success'] else 'failure'}>{r['success']}</td>"
                f"<td>{r['result'].get('observed_value', 'N/A')}</td></tr>"
                for r in validation_results['results']
            ])}
        </table>
    </body>
    </html>
    """
    
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    report_path = f"{report_dir}/validation_report_{timestamp}.html"
    
    with open(report_path, "w") as f:
        f.write(html_content)
    
    print(f"HTML validation report saved: {report_path}")

@task
def validate_data_with_great_expectations(df: pd.DataFrame, config: dict):
    """Alternative: Use Great Expectations with data context."""
    
    try:
        # Initialize Great Expectations context
        context = ge.data_context.DataContext()
        
        # Create validator
        validator = context.get_validator(
            batch_request={
                "datasource_name": "sensor_data_datasource",
                "data_connector_name": "default_inferred_data_connector_name",
                "data_asset_name": "sensor_data",
            },
            expectation_suite_name="sensor_data_validation_suite",
        )
        
        # Run validation
        validation_results = validator.validate(df)
        
        return generate_validation_report(validation_results, df)
        
    except Exception as e:
        print(f"Great Expectations context not available, using basic validation: {e}")
        return validate_data.fn(df, config)  # Fallback to basic validation