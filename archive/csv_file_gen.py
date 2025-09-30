# generate_valid_sensor_data.py
import pandas as pd
import numpy as np
from datetime import datetime
import os

def generate_valid_sensor_data(num_rows=100, output_folder="./data/valid_samples"):
    """Generate valid sensor data that will pass all validation rules."""
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    # Generate valid data according to your validation rules
    data = {
        'footfall': np.random.randint(0, 1000, num_rows),  # 0-1000 people
        'tempMode': np.random.choice([1, 2, 3, 4, 5, 6, 7], num_rows),  # Only allowed values
        'AQ': np.random.randint(1, 11, num_rows),  # 1-10
        'USS': np.random.randint(1, 11, num_rows),  # 1-10
        'CS': np.random.randint(1, 11, num_rows),   # 1-10
        'VOC': np.random.randint(0, 11, num_rows),  # 0-10
        'RP': np.random.randint(0, 101, num_rows),  # 0-100
        'IP': np.random.randint(1, 11, num_rows),   # 1-10
        'Temperature': np.round(np.random.uniform(-10, 40, num_rows), 1),  # -10°C to 40°C
        'fail': np.random.choice([0, 1], num_rows, p=[0.9, 0.1])  # Mostly 0, some 1
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    # Add some realistic patterns (optional)
    # Make temperature correlate with tempMode
    df['Temperature'] = np.where(df['tempMode'] >= 5, 
                                df['Temperature'] + 5, 
                                df['Temperature'])
    
    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"valid_sensor_data_{timestamp}.csv"
    file_path = os.path.join(output_folder, filename)
    
    df.to_csv(file_path, index=False)
    print(f"✅ Generated valid CSV file: {file_path}")
    print(f"📊 Sample of generated data:")
    print(df.head(10))
    
    return file_path, df

def fix_existing_csv(input_file_path, output_folder="./data/fixed_files"):
    """Fix an existing CSV file to make it pass validation."""
    
    # Create output folder
    os.makedirs(output_folder, exist_ok=True)
    
    try:
        # Read the problematic CSV
        df = pd.read_csv(input_file_path)
        print(f"📖 Read input file: {input_file_path}")
        print(f"Original data shape: {df.shape}")
        
        # Fix common issues
        df_fixed = df.copy()
        
        # 1. Strip column names
        df_fixed.columns = df_fixed.columns.str.strip()
        
        # 2. Ensure all required columns exist
        required_columns = ['footfall', 'tempMode', 'AQ', 'USS', 'CS', 'VOC', 'RP', 'IP', 'Temperature', 'fail']
        for col in required_columns:
            if col not in df_fixed.columns:
                print(f"⚠️ Missing column '{col}', adding with default values")
                if col == 'fail':
                    df_fixed[col] = 0  # Default to no failure
                elif col == 'Temperature':
                    df_fixed[col] = 20.0  # Reasonable default temperature
                else:
                    df_fixed[col] = 1  # Default value for other numeric columns
        
        # 3. Fix data types and ranges
        fixes_applied = []
        
        # footfall: 0-10000
        if 'footfall' in df_fixed.columns:
            df_fixed['footfall'] = pd.to_numeric(df_fixed['footfall'], errors='coerce')
            df_fixed['footfall'] = df_fixed['footfall'].fillna(0).clip(0, 10000).astype(int)
            fixes_applied.append("Fixed footfall range (0-10000)")
        
        # tempMode: 1-7
        if 'tempMode' in df_fixed.columns:
            df_fixed['tempMode'] = pd.to_numeric(df_fixed['tempMode'], errors='coerce')
            df_fixed['tempMode'] = df_fixed['tempMode'].fillna(1).clip(1, 7).astype(int)
            fixes_applied.append("Fixed tempMode range (1-7)")
        
        # AQ: 1-10
        if 'AQ' in df_fixed.columns:
            df_fixed['AQ'] = pd.to_numeric(df_fixed['AQ'], errors='coerce')
            df_fixed['AQ'] = df_fixed['AQ'].fillna(5).clip(1, 10).astype(int)
            fixes_applied.append("Fixed AQ range (1-10)")
        
        # USS: 1-10
        if 'USS' in df_fixed.columns:
            df_fixed['USS'] = pd.to_numeric(df_fixed['USS'], errors='coerce')
            df_fixed['USS'] = df_fixed['USS'].fillna(5).clip(1, 10).astype(int)
            fixes_applied.append("Fixed USS range (1-10)")
        
        # CS: 1-10
        if 'CS' in df_fixed.columns:
            df_fixed['CS'] = pd.to_numeric(df_fixed['CS'], errors='coerce')
            df_fixed['CS'] = df_fixed['CS'].fillna(5).clip(1, 10).astype(int)
            fixes_applied.append("Fixed CS range (1-10)")
        
        # VOC: 0-10
        if 'VOC' in df_fixed.columns:
            df_fixed['VOC'] = pd.to_numeric(df_fixed['VOC'], errors='coerce')
            df_fixed['VOC'] = df_fixed['VOC'].fillna(0).clip(0, 10).astype(int)
            fixes_applied.append("Fixed VOC range (0-10)")
        
        # RP: 0-100
        if 'RP' in df_fixed.columns:
            df_fixed['RP'] = pd.to_numeric(df_fixed['RP'], errors='coerce')
            df_fixed['RP'] = df_fixed['RP'].fillna(50).clip(0, 100).astype(int)
            fixes_applied.append("Fixed RP range (0-100)")
        
        # IP: 1-10
        if 'IP' in df_fixed.columns:
            df_fixed['IP'] = pd.to_numeric(df_fixed['IP'], errors='coerce')
            df_fixed['IP'] = df_fixed['IP'].fillna(5).clip(1, 10).astype(int)
            fixes_applied.append("Fixed IP range (1-10)")
        
        # Temperature: -50 to 100
        if 'Temperature' in df_fixed.columns:
            df_fixed['Temperature'] = pd.to_numeric(df_fixed['Temperature'], errors='coerce')
            df_fixed['Temperature'] = df_fixed['Temperature'].fillna(20.0).clip(-50, 100).round(1)
            fixes_applied.append("Fixed Temperature range (-50 to 100)")
        
        # fail: 0 or 1
        if 'fail' in df_fixed.columns:
            df_fixed['fail'] = pd.to_numeric(df_fixed['fail'], errors='coerce')
            df_fixed['fail'] = df_fixed['fail'].fillna(0).clip(0, 1).astype(int)
            fixes_applied.append("Fixed fail values (0 or 1)")
        
        # 4. Remove duplicates
        initial_rows = len(df_fixed)
        df_fixed = df_fixed.drop_duplicates()
        duplicates_removed = initial_rows - len(df_fixed)
        if duplicates_removed > 0:
            fixes_applied.append(f"Removed {duplicates_removed} duplicate rows")
        
        # 5. Fill any remaining null values
        null_count_before = df_fixed.isnull().sum().sum()
        df_fixed = df_fixed.fillna(method='ffill').fillna(method='bfill')
        null_count_after = df_fixed.isnull().sum().sum()
        if null_count_before > 0:
            fixes_applied.append(f"Filled {null_count_before} null values")
        
        # Save fixed file
        input_filename = os.path.basename(input_file_path)
        filename_without_ext = os.path.splitext(input_filename)[0]
        output_filename = f"fixed_{filename_without_ext}.csv"
        output_path = os.path.join(output_folder, output_filename)
        
        df_fixed.to_csv(output_path, index=False)
        
        print(f"✅ Fixed CSV file saved: {output_path}")
        print(f"🔧 Fixes applied:")
        for fix in fixes_applied:
            print(f"   - {fix}")
        print(f"📊 Fixed data shape: {df_fixed.shape}")
        print(f"📋 Sample of fixed data:")
        print(df_fixed.head(10))
        
        return output_path, df_fixed
        
    except Exception as e:
        print(f"❌ Error fixing CSV file: {e}")
        return None, None

def validate_and_diagnose(csv_file_path):
    """Validate a CSV file and show what needs to be fixed."""
    from src.prefect_flows.tasks.validate_sensor_data import validate_sensor_data
    
    df = pd.read_csv(csv_file_path)
    results = validate_sensor_data.fn(df)
    
    print(f"🔍 Validation Results for: {csv_file_path}")
    print(f"✅ Overall Success: {results['success']}")
    print(f"📊 Summary: {results['summary']}")
    
    if results['errors']:
        print("❌ Errors found:")
        for error in results['errors']:
            print(f"   - {error}")
    
    if results['warnings']:
        print("⚠️ Warnings:")
        for warning in results['warnings']:
            print(f"   - {warning}")
    
    return results

if __name__ == "__main__":
    print("🚀 Sensor Data CSV Tools")
    print("=" * 50)
    
    # Option 1: Generate new valid data
    print("\n1. Generating new valid sensor data...")
    valid_file, valid_df = generate_valid_sensor_data(num_rows=50)
    
    # Option 2: If you have a problematic file, fix it
    # print("\n2. Fixing existing CSV file...")
    # problematic_file = "path/to/your/problematic_file.csv"
    # fixed_file, fixed_df = fix_existing_csv(problematic_file)
    
    # Option 3: Validate and diagnose
    # print("\n3. Validating file...")
    # validate_and_diagnose(valid_file)