"""
ETL Runner - Execute generated PySpark scripts with sample data

This script helps you run the generated PySpark ETL code with the sample data.
It automatically updates file paths in the generated code to point to sample data.

Usage:
    uv run run_etl.py <generated_etl_file>
    
Examples:
    uv run run_etl.py generated/pyspark/wf_m_PatternForChurnData_etl.py
    uv run run_etl.py generated/pyspark/wf_s_m_complex5_etl.py
"""

import sys
import re
from pathlib import Path
import tempfile
import subprocess


def update_file_paths_in_etl(etl_code: str, sample_data_dir: Path) -> str:
    """
    Update file paths in generated ETL code to point to sample data
    
    Replaces:
    - "path/to/file.csv" -> "generated/sample_data/file.csv"
    - JDBC connections with warnings
    """
    updated_code = etl_code
    
    # Update CSV file paths
    # Pattern: .csv("path/to/filename.csv")
    csv_pattern = r'\.csv\(["\']path/to/([^"\']+)["\']'
    
    def replace_csv_path(match):
        filename = match.group(1)
        # Convert to lowercase to match generated sample data naming
        base_name = Path(filename).stem.lower()
        new_path = sample_data_dir / f"{base_name}.csv"
        return f'.csv("{new_path}")'
    
    updated_code = re.sub(csv_pattern, replace_csv_path, updated_code)
    
    # Add warning comments for JDBC connections
    if 'jdbc' in updated_code.lower():
        warning = '''
# ⚠️  WARNING: This ETL contains JDBC database connections
# Sample data is only available for flat file sources
# Database sources will need actual connection details
# Consider commenting out database reads for testing with sample data only

'''
        # Insert warning after imports
        import_end = updated_code.find('spark = SparkSession')
        if import_end > 0:
            updated_code = updated_code[:import_end] + warning + updated_code[import_end:]
    
    return updated_code


def run_pyspark_script(script_path: Path, use_sample_data: bool = True):
    """
    Run a PySpark script, optionally updating paths to use sample data
    """
    print(f"{'='*80}")
    print(f"RUNNING PYSPARK ETL: {script_path.name}")
    print(f"{'='*80}\n")
    
    if not script_path.exists():
        print(f"❌ Error: Script not found: {script_path}")
        sys.exit(1)
    
    # Read the ETL script
    with open(script_path, 'r') as f:
        etl_code = f.read()
    
    # Update paths if using sample data
    if use_sample_data:
        sample_data_dir = Path("generated/sample_data")
        
        if not sample_data_dir.exists() or not list(sample_data_dir.glob("*.csv")):
            print("⚠️  Warning: No sample data found in generated/sample_data/")
            print("   Generate sample data first using:")
            print(f"   uv run generate_sample_data.py <metadata.json>\n")
            response = input("Continue anyway? (y/N): ")
            if response.lower() != 'y':
                sys.exit(0)
        else:
            print(f"📂 Using sample data from: {sample_data_dir.absolute()}")
            csv_files = list(sample_data_dir.glob("*.csv"))
            print(f"   Found {len(csv_files)} CSV file(s):")
            for csv_file in csv_files:
                print(f"   • {csv_file.name}")
            print()
        
        # Update file paths in the code
        etl_code = update_file_paths_in_etl(etl_code, sample_data_dir)
    
    # Create a temporary file with updated code
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as tmp:
        tmp.write(etl_code)
        tmp_path = tmp.name
    
    try:
        print(f"🚀 Executing PySpark script...\n")
        print(f"{'='*80}\n")
        
        # Run the PySpark script using spark-submit
        # First try with spark-submit, fall back to python if not available
        try:
            result = subprocess.run(
                ['spark-submit', '--master', 'local[*]', tmp_path],
                check=True,
                capture_output=False,
                text=True
            )
        except FileNotFoundError:
            # spark-submit not found, try running with python directly
            print("⚠️  spark-submit not found, running with python directly...")
            print("   (This may not work for all PySpark features)\n")
            result = subprocess.run(
                [sys.executable, tmp_path],
                check=True,
                capture_output=False,
                text=True
            )
        
        print(f"\n{'='*80}")
        print("✅ ETL execution completed successfully!")
        print(f"{'='*80}\n")
        
    except subprocess.CalledProcessError as e:
        print(f"\n{'='*80}")
        print(f"❌ ETL execution failed with error code: {e.returncode}")
        print(f"{'='*80}\n")
        sys.exit(1)
    except KeyboardInterrupt:
        print(f"\n\n{'='*80}")
        print("⚠️  ETL execution interrupted by user")
        print(f"{'='*80}\n")
        sys.exit(1)
    finally:
        # Clean up temporary file
        Path(tmp_path).unlink(missing_ok=True)


def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: uv run run_etl.py <generated_etl_file> [--no-sample-data]")
        print("\nExamples:")
        print("  uv run run_etl.py generated/pyspark/wf_m_PatternForChurnData_etl.py")
        print("  uv run run_etl.py generated/pyspark/wf_s_m_complex5_etl.py")
        print("\nOptions:")
        print("  --no-sample-data    Don't update paths to use sample data")
        print("\nDescription:")
        print("  Runs the generated PySpark ETL script.")
        print("  By default, updates file paths to use generated sample data.")
        sys.exit(1)
    
    script_path = Path(sys.argv[1])
    use_sample_data = '--no-sample-data' not in sys.argv
    
    run_pyspark_script(script_path, use_sample_data)


if __name__ == "__main__":
    main()
