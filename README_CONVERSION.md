# Informatica PowerCenter to PySpark Conversion

This toolkit helps you convert Informatica PowerCenter XML exports to PySpark ETL code.

## Overview

The conversion process has two main steps:

1. **Parse** the Informatica XML to extract metadata (sources, targets, transformations, data flow)
2. **Generate** PySpark code that replicates the ETL logic

## Files

- `infa_to_pyspark_parser.py` - Parses Informatica XML and extracts metadata
- `generate_pyspark_code.py` - Generates PySpark code from parsed metadata
- `Informatica PC Samples/` - Sample Informatica XML files

## Installation

```bash
# No special dependencies needed for parsing
# For running generated PySpark code, you'll need:
pip install pyspark
```

## Usage

### Step 1: Analyze an Informatica XML File

```bash
python infa_to_pyspark_parser.py "Informatica PC Samples/wf_m_PatternForChurnData.xml"
```

This will print a detailed summary:
- Sources (tables, flat files)
- Targets (output destinations)
- Mappings and their transformations
- Data flow paths
- Unconnected lookup calls

**Export to JSON for further processing:**

```bash
python infa_to_pyspark_parser.py "Informatica PC Samples/wf_m_PatternForChurnData.xml" metadata.json
```

### Step 2: Generate PySpark Code

```bash
python generate_pyspark_code.py "Informatica PC Samples/wf_m_PatternForChurnData.xml" output_etl.py
```

This generates a complete PySpark script with:
- Source reading (CSV/JDBC)
- All transformations converted to PySpark operations
- Target writing

**View output directly (without saving):**

```bash
python generate_pyspark_code.py "Informatica PC Samples/wf_m_PatternForChurnData.xml"
```

## Supported Transformations

| Informatica | PySpark Equivalent | Status |
|-------------|-------------------|--------|
| **Source Qualifier** | `spark.read.csv()` / `spark.read.jdbc()` | ✅ Supported |
| **Expression** | `.withColumn()` with expressions | ✅ Supported |
| **Filter** | `.filter()` / `.where()` | ✅ Supported |
| **Aggregator** | `.groupBy().agg()` | ✅ Supported |
| **Joiner** | `.join()` | ✅ Supported |
| **Sorter** | `.orderBy()` | ✅ Supported |
| **Lookup (connected)** | `.join()` with broadcast | ⚠️ Partial |
| **Lookup (unconnected)** | UDF or join | ⚠️ Partial |
| **Router** | Multiple `.filter()` | ⚠️ Manual |
| **Union** | `.union()` | ⚠️ Manual |
| **Rank** | `.withColumn()` + Window | ⚠️ Manual |

## Expression Conversion

The tool automatically converts Informatica expressions to PySpark:

| Informatica | PySpark |
|-------------|---------|
| `IIF(condition, true, false)` | `F.when(condition, true).otherwise(false)` |
| `CONCAT(field1, field2)` | `F.concat(field1, field2)` |
| `LTRIM(field)` | `F.ltrim(field)` |
| `RTRIM(field)` | `F.rtrim(field)` |
| `SUM(field)` | `F.sum(field)` |
| `MAX(field)` | `F.max(field)` |
| `MIN(field)` | `F.min(field)` |

## Example Workflow

Let's convert the churn data mapping:

```bash
# 1. Analyze the XML
python infa_to_pyspark_parser.py "Informatica PC Samples/wf_m_PatternForChurnData.xml"
```

Output:
```
================================================================================
INFORMATICA XML ANALYSIS: wf_m_PatternForChurnData.xml
================================================================================

📥 SOURCES (2):
  • Gender (Flat File - Flat File)
    Fields: 2
  • Churndata (Flat File - Flat File)
    Fields: 21

📤 TARGETS (3):
  • CHURNRATIO (Flat File - Flat File)
    Fields: 7
  • CHURNDIM (Flat File - Flat File)
    Fields: 17
  • ChurnDT (Flat File - Flat File)
    Fields: 3

🗺️  MAPPINGS (1):
  • m_PatternForChurnData
    Transformations: 5
    Transformation breakdown:
      - Aggregator: 1
      - Expression: 1
      - Joiner: 1
      - Source Qualifier: 2
      - Sorter: 1
```

```bash
# 2. Generate PySpark code
python generate_pyspark_code.py "Informatica PC Samples/wf_m_PatternForChurnData.xml" churn_etl.py
```

This creates `churn_etl.py` with complete PySpark code!

## Customization

### Modify Data Types

Edit the `_map_datatype()` method in `generate_pyspark_code.py`:

```python
def _map_datatype(self, infa_type: str) -> str:
    type_mapping = {
        'string': 'StringType()',
        'decimal': 'DecimalType(10, 2)',  # Customize precision
        # Add your custom mappings
    }
    return type_mapping.get(infa_type.lower(), 'StringType()')
```

### Add Custom Transformations

Extend the `PySparkCodeGenerator` class:

```python
def generate_custom_transform(self, trans: Transformation, input_df: str):
    # Your custom logic here
    pass
```

### Handle Unconnected Lookups

For complex lookup logic, you may need to:

1. Convert to broadcast joins
2. Create UDFs for lookup logic
3. Use window functions

Example:

```python
# Instead of :LKP.LKPTRANS(DEPTNO)
# Use a broadcast join:
lookup_df = spark.read.table("DDEPT").alias("lookup")
result_df = main_df.join(
    F.broadcast(lookup_df),
    F.col("DEPTNO") == F.col("lookup.D_DEPTNO"),
    "left"
)
```

## Programmatic Usage

Use the parser in your own Python scripts:

```python
from infa_to_pyspark_parser import InformaticaXMLParser

# Parse XML
parser = InformaticaXMLParser("path/to/informatica.xml")
parser.parse()

# Access parsed objects
for source in parser.sources:
    print(f"Source: {source.name}")
    for field in source.fields:
        print(f"  - {field.name}: {field.datatype}")

for mapping in parser.mappings:
    print(f"Mapping: {mapping.name}")
    paths = parser.get_data_flow(mapping.name)
    for path in paths:
        print(f"  Flow: {' → '.join(path)}")

# Export to dict/JSON
metadata = parser.to_dict()
```

## Limitations & Manual Steps

Some transformations require manual intervention:

1. **Unconnected Lookups** - May need refactoring to joins or UDFs
2. **Router Transformations** - Split into multiple filter operations
3. **Sequence Generators** - Use `monotonically_increasing_id()` or window functions
4. **Stored Procedures** - Rewrite logic in PySpark or call via JDBC
5. **Complex Expressions** - May need manual review and adjustment
6. **Connection Strings** - Update JDBC URLs, credentials, file paths

## Tips for Production

1. **Test incrementally** - Convert and test one mapping at a time
2. **Review expressions** - Complex Informatica expressions may need manual tuning
3. **Optimize joins** - Use broadcast joins for small lookup tables
4. **Partition data** - Add `.repartition()` for large datasets
5. **Add logging** - Insert logging statements for debugging
6. **Handle nulls** - PySpark null handling differs from Informatica

## Next Steps

After generating PySpark code:

1. ✅ Review and test the generated code
2. ✅ Update file paths and connection strings
3. ✅ Add error handling and logging
4. ✅ Optimize for your Spark cluster
5. ✅ Add unit tests
6. ✅ Set up CI/CD pipeline

## Support

For issues or questions:
- Review the generated code comments
- Check Informatica XML structure with the parser
- Consult PySpark documentation for specific transformations

## License

MIT License - Feel free to modify and extend!
