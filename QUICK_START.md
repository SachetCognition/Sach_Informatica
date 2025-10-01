# Quick Start Guide: Informatica to PySpark Conversion

## 📦 Installation

```bash
# 1. Install dependencies (PySpark, etc.)
uv sync

# 2. Install Java 17 (required for PySpark)
brew install openjdk@17
sudo ln -sfn /opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk /Library/Java/JavaVirtualMachines/openjdk-17.jdk

# 3. Verify installation
java -version
uv run python -c "import pyspark; print('PySpark installed!')"
```

**Note:** Java is required to run actual PySpark code. For testing without Java, use `test_without_java.py`.

## 🚀 Quick Commands

### Analyze any Informatica XML file
```bash
uv run python infa_to_pyspark_parser.py "Informatica PC Samples/<your_file>.xml"
# Output: generated/metadata/<your_file>_metadata.json
```

### Generate PySpark code
```bash
uv run python generate_pyspark_code.py "Informatica PC Samples/<your_file>.xml"
# Output: generated/pyspark/<your_file>_etl.py
```

### Generate sample CSV data
```bash
uv run python generate_sample_data.py generated/metadata/<your_file>_metadata.json
# Output: generated/sample_data/*.csv
```

### Test without Java (simulated ETL)
```bash
uv run python test_without_java.py
```

### Test with PySpark (requires Java)
```bash
# Option 1: Use helper script (sets JAVA_HOME automatically)
./run_with_java.sh uv run python test_etl_simple.py

# Option 2: Set JAVA_HOME manually
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
uv run python test_etl_simple.py
```

### Run the full ETL (requires Java)
```bash
./run_with_java.sh uv run python run_etl.py generated/pyspark/<your_file>_etl.py
```

### Custom output paths (optional)
```bash
# Specify custom output locations
uv run python infa_to_pyspark_parser.py "file.xml" custom_metadata.json
uv run python generate_pyspark_code.py "file.xml" custom_etl.py
uv run python generate_sample_data.py metadata.json 100 custom_data_dir
```

## 📋 What You Get

### From the Parser
- Complete source/target inventory
- Transformation breakdown by type
- Data flow visualization
- Unconnected lookup identification
- Field-level metadata

### From the Code Generator
- Ready-to-run PySpark script
- Source reading logic (CSV/JDBC)
- Converted transformations
- Target writing logic
- Proper imports and session setup

### From the Sample Data Generator
- Realistic CSV files for all flat file sources
- Proper field types and formats
- Configurable number of rows
- Ready for testing PySpark code

## 🎯 Sample Files Included

| File | Description | Complexity |
|------|-------------|------------|
| `wf_m_PatternForChurnData.xml` | Customer churn analysis | Medium |
| `wf_m_mapplet_multiple_unconnected.XML` | Unconnected lookups | Medium |
| `wf_s_m_complex5.XML` | Multi-source with router | **High** |
| `wf_s_m_multiple_mapplet.XML` | Chained mapplets | Medium |
| `wf_s_m_multiple_unc11.xml` | Aggregator with lookups | Medium |

## 🔍 Understanding the Output

### Parser Output Structure
```
📥 SOURCES - Where data comes from
📤 TARGETS - Where data goes
🔧 MAPPLETS - Reusable transformation logic
🗺️  MAPPINGS - Complete ETL workflows
    ├── Transformations - Processing steps
    ├── Data flow paths - Source → Target routes
    └── Unconnected lookups - Special handling needed
```

### Generated PySpark Code Structure
```python
# 1. IMPORTS & SETUP
from pyspark.sql import SparkSession, functions as F

# 2. READ SOURCES
df_source = spark.read.csv(...)

# 3. TRANSFORMATIONS
df_transformed = df_source.withColumn(...)

# 4. WRITE TARGETS
df_transformed.write.csv(...)
```

## ⚡ Common Workflows

### Workflow 1: Quick Analysis
```bash
# See what's in the file
uv run infa_to_pyspark_parser.py "Informatica PC Samples/wf_s_m_complex5.XML"
```

### Workflow 2: Full Conversion with Testing
```bash
# 1. Analyze and export metadata (auto-saves to generated/metadata/)
uv run infa_to_pyspark_parser.py "Informatica PC Samples/wf_s_m_complex5.XML"

# 2. Generate PySpark code (auto-saves to generated/pyspark/)
uv run generate_pyspark_code.py "Informatica PC Samples/wf_s_m_complex5.XML"

# 3. Generate sample data for testing (auto-saves to generated/sample_data/)
uv run generate_sample_data.py generated/metadata/wf_s_m_complex5_metadata.json 100

# 4. Review and customize generated/pyspark/wf_s_m_complex5_etl.py
# 5. Test with generated sample data
```

### Workflow 3: Batch Processing
```bash
# Process all XML files
for file in "Informatica PC Samples"/*.xml; do
    echo "Processing: $file"
    uv run generate_pyspark_code.py "$file" "output/$(basename "$file" .xml).py"
done
```

## 🛠️ Customization Points

### 1. Update Connection Strings
```python
# In generated code, replace:
.option("url", "jdbc:oracle:thin:@//host:port/service")
.option("user", "username")
.option("password", "password")

# With your actual values:
.option("url", "jdbc:oracle:thin:@//prod-db:1521/PROD")
.option("user", os.getenv("DB_USER"))
.option("password", os.getenv("DB_PASSWORD"))
```

### 2. Update File Paths
```python
# Replace:
.csv("path/to/file.csv")

# With:
.csv("s3://my-bucket/data/file.csv")
# or
.csv("/mnt/data/file.csv")
```

### 3. Add Error Handling
```python
try:
    df = spark.read.csv("path/to/file.csv")
except Exception as e:
    logger.error(f"Failed to read source: {e}")
    raise
```

## 📊 Interpreting Results

### High Path Count (>100)
- Indicates complex routing (Router transformation)
- Multiple targets from same source
- Review generated code carefully

### Unconnected Lookups
- Requires manual conversion to joins
- Consider using broadcast joins for small lookup tables
- May need UDF for complex lookup logic

### Multiple Mapplets
- Check for nested reusable logic
- May need to flatten transformation chain
- Verify field mappings between mapplets

## 🐛 Troubleshooting

### Issue: "KeyboardInterrupt" or infinite loop
**Solution**: Fixed in latest version with path limits

### Issue: Generated code missing transformations
**Solution**: Check if transformation type is supported (see README_CONVERSION.md)

### Issue: Expression conversion incorrect
**Solution**: Manually review and adjust complex expressions

### Issue: Lookup not working
**Solution**: Convert unconnected lookups to broadcast joins:
```python
lookup_df = spark.read.table("LOOKUP_TABLE")
result = main_df.join(
    F.broadcast(lookup_df),
    main_df.key == lookup_df.key,
    "left"
)
```

## 📚 Additional Resources

- **README_CONVERSION.md** - Detailed documentation
- **CONVERSION_SUMMARY.md** - Example analysis and results
- **complex5_metadata.json** - Sample metadata export
- **complex5_etl.py** - Sample generated PySpark code

## 💡 Pro Tips

1. **Start Simple**: Begin with simple mappings before tackling complex ones
2. **Test Incrementally**: Test each transformation step with sample data
3. **Use Broadcast**: For small lookup tables, use `F.broadcast()` for performance
4. **Cache Wisely**: Cache DataFrames that are reused multiple times
5. **Monitor Performance**: Use Spark UI to identify bottlenecks
6. **Version Control**: Keep both Informatica XML and generated PySpark code in Git

## 🎓 Learning Path

1. ✅ Run parser on sample files
2. ✅ Review generated PySpark code
3. ✅ Understand transformation mappings
4. ✅ Customize for your environment
5. ✅ Test with sample data
6. ✅ Deploy to production

## 🤝 Contributing

Found a bug or want to add features?
1. Review the parser logic in `infa_to_pyspark_parser.py`
2. Extend the code generator in `generate_pyspark_code.py`
3. Add new transformation handlers
4. Improve expression conversion

---

**Ready to convert?** Start with: `uv run infa_to_pyspark_parser.py "Informatica PC Samples/wf_s_m_complex5.XML"`
