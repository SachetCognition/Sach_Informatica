# Informatica to PySpark Conversion Summary

## Scripts Created

### 1. `infa_to_pyspark_parser.py`
**Purpose**: Parses Informatica PowerCenter XML files and extracts metadata

**Key Features**:
- Extracts sources (tables, flat files) with field definitions
- Extracts targets (output destinations)
- Parses transformations (Expression, Filter, Aggregator, Joiner, Sorter, Lookup, etc.)
- Maps data flow between transformations
- Identifies unconnected lookup calls
- Exports metadata to JSON for further processing

**Usage**:
```bash
# Analyze and print summary
uv run infa_to_pyspark_parser.py "Informatica PC Samples/wf_s_m_complex5.XML"

# Export metadata to JSON
uv run infa_to_pyspark_parser.py "Informatica PC Samples/wf_s_m_complex5.XML" output.json
```

### 2. `generate_pyspark_code.py`
**Purpose**: Generates executable PySpark code from parsed Informatica metadata

**Key Features**:
- Converts Informatica expressions to PySpark syntax
- Generates source reading code (CSV, JDBC)
- Converts transformations to PySpark operations
- Generates target writing code
- Handles common patterns (filters, joins, aggregations, sorting)

**Usage**:
```bash
# Generate PySpark code
uv run generate_pyspark_code.py "Informatica PC Samples/wf_s_m_complex5.XML" output_etl.py

# View generated code without saving
uv run generate_pyspark_code.py "Informatica PC Samples/wf_s_m_complex5.XML"
```

## Example: wf_s_m_complex5.XML Analysis

### Sources (5)
1. **EMP_DEPT** - Oracle database table (3 fields)
2. **EMPLOYEE_DETAILS1** - Oracle database table (7 fields)
3. **EMPLOYEE_DETAILS2** - Oracle database table (7 fields)
4. **EMPLOYEEDETAILS** - Oracle database table (7 fields)
5. **EMPLOYEE_DETAILS_FLAT** - Flat file with tab delimiter (7 fields)

### Targets (5)
1. **TARGET_UPDATE** - Flat file (8 fields)
2. **TARGET_INSERT** - Flat file (8 fields)
3. **T_DEFAULT** - Oracle table (7 fields)
4. **T_EMP_DEP10** - Oracle table (7 fields)
5. **EMP_MAX_MIN** - Oracle table with aggregated data (9 fields)

### Transformations (18)
- **Aggregator**: 1 (calculates MAX/MIN salary)
- **Custom Transformation**: 1 (Union)
- **Expression**: 2 (field calculations)
- **Filter**: 2 (row filtering)
- **Joiner**: 1 (joins employee and department data)
- **Lookup Procedure**: 2 (reference data lookups)
- **Router**: 1 (conditional routing to multiple targets)
- **Sorter**: 2 (data sorting)
- **Source Qualifier**: 5 (one per source)
- **Update Strategy**: 1 (insert/update logic)

### Mapplets (1)
- **mp_name**: Reusable logic to concatenate first and last names

### Data Flow
The mapping has **101 data flow paths** from sources to targets, with the main flow:
```
SQ_EMPLOYEE_DETAILS_FLAT → Union → SRTTRANS → JNRTRANS → mp_name → EXPTRANS → AGGTRANS → EMP_MAX_MIN
```

### Unconnected Lookups
- **EXPTRANS**: Contains 1 unconnected lookup call (needs special handling in PySpark)

## Generated PySpark Code

The script generated `complex5_etl.py` with:

### 1. Source Reading
```python
# Oracle database sources
df_emp_dept = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMP_DEPT") \
    .load()

# Flat file source
df_employee_details_flat = spark.read \
    .option("header", "true") \
    .option("delimiter", "\011") \
    .csv("path/to/employee_details_flat.csv")
```

### 2. Transformations
```python
# Sorter → orderBy
df_srttrans = df_employee_details_flat.orderBy("EMPID", "FIRSTNAME", "LASTNAME")

# Joiner → join
df_jnrtrans = df_srttrans.join(
    df_srttrans1,
    F.col("DEPTNO") == F.col("DEPID"),
    "inner"
)
```

### 3. Target Writing
```python
# Flat file target
df_result.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output/target_update")

# Oracle database target
df_result.write \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMP_MAX_MIN") \
    .mode("overwrite") \
    .save()
```

## Transformation Mapping

| Informatica | PySpark | Example |
|-------------|---------|---------|
| **Expression** | `.withColumn()` | `df.withColumn("FULL_NAME", F.concat("FIRST", "LAST"))` |
| **Filter** | `.filter()` / `.where()` | `df.filter(F.col("SALARY") > 50000)` |
| **Aggregator** | `.groupBy().agg()` | `df.groupBy("DEPT").agg(F.max("SALARY"))` |
| **Joiner** | `.join()` | `df1.join(df2, "key", "inner")` |
| **Sorter** | `.orderBy()` | `df.orderBy("EMPID", "NAME")` |
| **Router** | Multiple `.filter()` | `df_route1 = df.filter(condition1)` |
| **Union** | `.union()` | `df1.union(df2)` |
| **Lookup** | `.join()` with broadcast | `df.join(F.broadcast(lookup_df), "key")` |

## Expression Conversion Examples

| Informatica Expression | PySpark Equivalent |
|------------------------|-------------------|
| `IIF(SAL > 5000, 'High', 'Low')` | `F.when(F.col("SAL") > 5000, "High").otherwise("Low")` |
| `CONCAT(FIRSTNAME, LASTNAME)` | `F.concat(F.col("FIRSTNAME"), F.col("LASTNAME"))` |
| `LTRIM(RTRIM(NAME))` | `F.ltrim(F.rtrim(F.col("NAME")))` |
| `SUM(SALARY)` | `F.sum("SALARY")` |
| `MAX(SALARY)` | `F.max("SALARY")` |

## Next Steps for Production Use

1. **Review Generated Code**
   - Check all transformations are correctly converted
   - Verify expression logic matches Informatica
   - Test with sample data

2. **Update Configuration**
   - Replace JDBC URLs with actual connection strings
   - Update file paths for CSV sources/targets
   - Add credentials management (use secrets/environment variables)

3. **Handle Special Cases**
   - **Unconnected Lookups**: Convert to broadcast joins or UDFs
   - **Router**: Implement multiple filter branches
   - **Update Strategy**: Use `.mode("append")` or custom logic
   - **Sequence Generators**: Use `monotonically_increasing_id()`

4. **Optimize Performance**
   - Add `.repartition()` for large datasets
   - Use `.broadcast()` for small lookup tables
   - Enable adaptive query execution
   - Add caching for reused DataFrames

5. **Add Production Features**
   - Error handling and logging
   - Data quality checks
   - Monitoring and metrics
   - Unit tests
   - CI/CD pipeline integration

## Files Generated

- ✅ `complex5_etl.py` - PySpark ETL code
- ✅ `complex5_metadata.json` - Extracted metadata in JSON format
- ✅ `infa_to_pyspark_parser.py` - Parser script
- ✅ `generate_pyspark_code.py` - Code generator script
- ✅ `README_CONVERSION.md` - Detailed documentation

## Known Limitations

1. **Complex Expressions**: Some Informatica-specific functions may need manual conversion
2. **Unconnected Lookups**: Require refactoring to joins or UDFs
3. **Stored Procedures**: Cannot be automatically converted
4. **Custom Transformations**: Need manual implementation
5. **Session-level Properties**: Not captured in XML export

## Support & Troubleshooting

If you encounter issues:
1. Check the metadata JSON to verify parsing
2. Review transformation types in the summary
3. Manually adjust complex expressions
4. Test incrementally with small datasets
5. Refer to PySpark documentation for specific operations
