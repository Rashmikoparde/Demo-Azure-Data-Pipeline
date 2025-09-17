# setup_great_expectations.py
import os
import json
import shutil

def setup_great_expectations_programmatically():
    """Setup Great Expectations directories and config programmatically."""
    print("Setting up Great Expectations programmatically...")
    
    ge_dir = "great_expectations"
    if os.path.exists(ge_dir):
        print("Great Expectations directory already exists")
        return True
    
    try:
        # Create GE directory structure
        os.makedirs(ge_dir, exist_ok=True)
        os.makedirs(os.path.join(ge_dir, "expectations"), exist_ok=True)
        os.makedirs(os.path.join(ge_dir, "checkpoints"), exist_ok=True)
        os.makedirs(os.path.join(ge_dir, "plugins"), exist_ok=True)
        os.makedirs(os.path.join(ge_dir, "uncommitted"), exist_ok=True)
        os.makedirs(os.path.join(ge_dir, "uncommitted", "data_docs"), exist_ok=True)
        
        # Create main config file
        ge_config = {
            "config_version": 3,
            "datasources": {
                "sensor_data_datasource": {
                    "class_name": "PandasDatasource",
                    "module_name": "great_expectations.datasource",
                    "data_connectors": {
                        "default_runtime_data_connector_name": {
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": ["run_id"]
                        }
                    }
                }
            },
            "expectations_store_name": "expectations_store",
            "validations_store_name": "validations_store",
            "evaluation_parameter_store_name": "evaluation_parameter_store",
            "stores": {
                "expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "expectations/"
                    }
                },
                "validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "validations/"
                    }
                },
                "evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                }
            },
            "data_docs_sites": {
                "local_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleFilesystemStoreBackend",
                        "base_directory": "uncommitted/data_docs/local_site/"
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder"
                    }
                }
            },
            "anonymous_usage_statistics": {
                "enabled": False
            }
        }
        
        with open(os.path.join(ge_dir, "great_expectations.yml"), "w") as f:
            yaml.dump(ge_config, f)
        
        print("Great Expectations setup completed programmatically")
        return True
        
    except Exception as e:
        print(f"Error setting up Great Expectations: {e}")
        # Clean up on failure
        if os.path.exists(ge_dir):
            shutil.rmtree(ge_dir)
        return False

# Install pyyaml for YAML handling
try:
    import yaml
except ImportError:
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyyaml"])
    import yaml

if __name__ == "__main__":
    success = setup_great_expectations_programmatically()
    if success:
        print("Great Expectations setup complete!")
    else:
        print("Great Expectations setup failed!")
        print("Using fallback validation mode...")