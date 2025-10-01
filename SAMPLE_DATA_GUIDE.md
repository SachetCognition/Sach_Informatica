# Sample Data Generation Guide

## Overview

The `generate_sample_data.py` script creates realistic CSV files for all flat file sources defined in your Informatica mappings. This is essential for testing your converted PySpark code without needing access to production data.

## How It Works

```
Informatica XML → Parser → Metadata JSON → Sample Data Generator → CSV Files
```

1. **Parse** the Informatica XML to extract source definitions
2. **Export** metadata to JSON format
3. **Generate** realistic CSV data based on field types and names
4. **Test** your PySpark code with the generated data

## Usage

### Basic Usage
```bash
uv run generate_sample_data.py metadata.json
```

### Specify Number of Rows
```bash
uv run generate_sample_data.py metadata.json 500
```

### Specify Output Directory
```bash
uv run generate_sample_data.py metadata.json 500 my_test_data
```

## Complete Example

Let's generate test data for the churn analysis mapping:

```bash
# Step 1: Parse Informatica XML and export metadata
uv run infa_to_pyspark_parser.py "Informatica PC Samples/wf_m_PatternForChurnData.xml" churn_metadata.json

# Step 2: Generate 200 rows of sample data
uv run generate_sample_data.py churn_metadata.json 200 sample_data

# Output:
# ✅ Created: sample_data/gender.csv (2 columns × 200 rows)
# ✅ Created: sample_data/churndata.csv (21 columns × 200 rows)
```

## Generated Data Quality

### Intelligent Field Recognition

The generator recognizes common field patterns and generates appropriate data:

| Field Pattern | Generated Data | Example |
|--------------|----------------|---------|
| **EMPNO, EMPID** | Sequential employee IDs | 1000, 1001, 1002... |
| **FIRSTNAME** | Common first names | John, Sarah, Michael |
| **LASTNAME** | Common last names | Smith, Johnson, Williams |
| **EMAIL** | Valid email addresses | john.smith@company.com |
| **PHONE** | Phone numbers | 555-123-4567 |
| **SALARY, SAL** | Realistic salaries | 45000.00, 87500.50 |
| **GENDER** | Gender values | Male, Female (or M, F) |
| **DEPTNO** | Department numbers | 10, 20, 30, 40, 50 |
| **LOCATION, LOC** | City names | New York, Chicago, Seattle |
| **CONTRACT** | Contract types | Month-to-month, One year |
| **CHURN** | Yes/No values | Yes, No |

### Telecom-Specific Fields

For telecom/customer data (like churn analysis):

| Field | Generated Values |
|-------|-----------------|
| **SeniorCitizen** | 0 or 1 |
| **Partner** | Yes, No |
| **Tenure** | 0-72 months |
| **InternetService** | DSL, Fiber optic, No |
| **PhoneService** | Yes, No |
| **MultipleLines** | Yes, No, No phone service |
| **OnlineSecurity** | Yes, No, No internet service |
| **PaymentMethod** | Electronic check, Credit card, etc. |
| **MonthlyCharges** | 18.95 - 118.95 |
| **TotalCharges** | Calculated based on tenure |

### Type-Based Generation

When field names don't match patterns, the generator uses data types:

| Data Type | Generated Data |
|-----------|---------------|
| **string/varchar** | "Value_0", "Value_1", etc. |
| **number/decimal** | Random numbers within precision/scale |
| **integer** | Random integers |
| **date** | Dates between 2020-2024 |
| **timestamp** | Timestamps with date and time |

## Sample Output

### Gender.csv
```csv
Gender,Type
Female,A
Male,B
Female,C
Male,D
```

### Churndata.csv (first few columns)
```csv
customerID,gender,SeniorCitizen,Partner,Dependents,tenure,PhoneService
1000-ABCDE,Male,0,Yes,No,24,Yes
1001-ABCDE,Female,1,No,Yes,12,No
1002-ABCDE,Male,0,Yes,Yes,48,Yes
```

### Employee_Details_Flat.csv
```csv
EMPID,FIRSTNAME,LASTNAME,EMAIL,PHONENO,SALARY,DEPID
1000,John,Smith,john.smith@company.com,555-123-4567,75000.00,10
1001,Sarah,Johnson,sarah.johnson@company.com,555-234-5678,82000.00,20
1002,Michael,Williams,michael.williams@company.com,555-345-6789,68000.00,30
```

## Testing Your PySpark Code

Once you've generated sample data, test your converted PySpark code:

```bash
# 1. Generate PySpark code
uv run generate_pyspark_code.py "Informatica PC Samples/wf_m_PatternForChurnData.xml" churn_etl.py

# 2. Generate sample data
uv run generate_sample_data.py churn_metadata.json 1000 sample_data

# 3. Update file paths in churn_etl.py
# Change: .csv("path/to/churndata.csv")
# To:     .csv("sample_data/churndata.csv")

# 4. Run your PySpark code
spark-submit churn_etl.py
```

## Customization

### Add Custom Field Patterns

Edit `generate_sample_data.py` and add your patterns in the `_generate_value()` method:

```python
# Add custom field recognition
if 'CUSTOM_FIELD' in field_name:
    return "Custom Value"
```

### Add Custom Data Pools

Extend the data pools in the `__init__()` method:

```python
self.custom_values = ["Value1", "Value2", "Value3"]
```

### Adjust Randomization

Control randomness for specific fields:

```python
# Always generate specific value for testing
if 'TEST_FIELD' in field_name:
    return "FIXED_VALUE"

# Generate with specific probability
if random.random() > 0.7:  # 30% chance
    return "Special Value"
```

## Best Practices

### 1. Start Small
Generate a small dataset first to verify data quality:
```bash
uv run generate_sample_data.py metadata.json 10 test_data
```

### 2. Review Generated Data
Check the first few rows to ensure data makes sense:
```bash
head -20 sample_data/churndata.csv
```

### 3. Scale Up for Testing
Once verified, generate larger datasets:
```bash
uv run generate_sample_data.py metadata.json 10000 sample_data
```

### 4. Version Control
Keep sample data separate from code:
```bash
# Add to .gitignore
echo "sample_data/" >> .gitignore
```

### 5. Document Data Assumptions
Note any special data generation rules for your team:
```bash
# Create a data dictionary
cat > sample_data/README.md << EOF
# Sample Data Documentation
- Employee IDs: 1000-1999
- Departments: 10, 20, 30, 40, 50
- Date range: 2020-2024
EOF
```

## Limitations

### Database Sources
The generator only creates CSV files for **flat file sources**. Database sources are skipped because:
- They're typically read directly from databases in production
- Connection details would be needed
- You can use database export tools if needed

### Complex Data Relationships
The generator creates independent rows. It doesn't maintain:
- Foreign key relationships between files
- Sequential dependencies
- Complex business rules

For these cases, consider:
- Manually adjusting generated data
- Creating custom generation logic
- Using production data samples (anonymized)

### Data Volume
For very large datasets (millions of rows):
- Generation may take time
- Consider generating in batches
- Use appropriate hardware

## Troubleshooting

### Issue: No CSV files generated
**Cause**: Metadata contains only database sources
**Solution**: Check if your mapping has flat file sources

### Issue: Data doesn't match expected format
**Cause**: Field name pattern not recognized
**Solution**: Add custom pattern in `_generate_value()` method

### Issue: Wrong data types
**Cause**: Metadata precision/scale not set correctly
**Solution**: Verify source definitions in Informatica XML

### Issue: File encoding errors
**Cause**: Special characters in generated data
**Solution**: Script uses UTF-8 encoding by default

## Advanced Usage

### Generate Data for Specific Sources Only

Modify the metadata JSON to include only desired sources:

```python
import json

# Load metadata
with open('metadata.json', 'r') as f:
    data = json.load(f)

# Filter to specific sources
data['sources'] = [s for s in data['sources'] if s['name'] == 'Churndata']

# Save filtered metadata
with open('filtered_metadata.json', 'w') as f:
    json.dump(data, f)

# Generate data
# uv run generate_sample_data.py filtered_metadata.json
```

### Programmatic Usage

Use the generator in your own Python scripts:

```python
from generate_sample_data import SampleDataGenerator
import json

# Load metadata
with open('metadata.json', 'r') as f:
    metadata = json.load(f)

# Create generator
generator = SampleDataGenerator(num_rows=500)

# Generate data
files = generator.generate_all_sources(metadata, 'output_dir')

print(f"Generated {len(files)} files")
```

## Integration with CI/CD

### Automated Testing Pipeline

```yaml
# .github/workflows/test.yml
name: Test PySpark ETL

on: [push]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Generate sample data
        run: |
          uv run generate_sample_data.py metadata.json 100 test_data
      
      - name: Run PySpark tests
        run: |
          spark-submit --master local[*] my_etl.py
```

## Summary

The sample data generator provides:
- ✅ **Realistic test data** based on field definitions
- ✅ **Fast generation** of any size dataset
- ✅ **Intelligent field recognition** for common patterns
- ✅ **Easy customization** for specific needs
- ✅ **Integration ready** for testing pipelines

Use it to test your converted PySpark code without needing production data access!
