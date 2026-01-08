# Informatica to PySpark Migration Report

## Overview

This report documents the migration of 5 Informatica PowerCenter XML workflow files to PySpark. The migration was performed using a combination of automated parsing tools and manual conversion for complex transformations.

## Files Migrated

| # | Original File | PySpark Output | Complexity | Status |
|---|---------------|----------------|------------|--------|
| 1 | wf_m_PatternForChurnData.xml | wf_m_PatternForChurnData_etl.py | Medium | Complete |
| 2 | wf_s_m_multiple_unc11.xml | wf_s_m_multiple_unc11_etl.py | Medium | Complete |
| 3 | wf_s_m_multiple_mapplet.XML | wf_s_m_multiple_mapplet_etl.py | Medium | Complete |
| 4 | wf_m_mapplet_multiple_unconnected.XML | wf_m_mapplet_multiple_unconnected_etl.py | Medium | Complete |
| 5 | wf_s_m_complex5.XML | wf_s_m_complex5_etl.py | High | Complete |

## Transformation Conversions

### 1. wf_m_PatternForChurnData.xml

**Original Mapping:** m_PatternForChurnData

**Sources:**
- Churndata (Oracle) - Customer churn data with 21 fields
- Gender (Oracle) - Gender lookup table

**Targets:**
- CHURNRATIO - Churn ratio calculations
- CHURNDIM - Churn dimension data
- ChurnDT - Churn detail data

**Transformations Converted:**
| Transformation | Type | PySpark Implementation |
|----------------|------|------------------------|
| SQ_Churndata | Source Qualifier | spark.read.csv() |
| SQ_Gender | Source Qualifier | spark.read.csv() |
| EXPTRANS | Expression | withColumn() with IIF logic for ContVar1, TotChargesVar |
| JNRTRANS1 | Joiner | .join() on gender field |
| SRTTRANS | Sorter | .orderBy() |
| EXPTRANS1 | Expression | withColumn() for additional calculations |
| AGGTRANS | Aggregator | .groupBy().agg() with MAX/MIN |

**Manual Conversions:**
- IIF(Contract = "One year", 23, 12) converted to F.when().otherwise()
- Joiner converted to inner join with proper column selection
- Aggregator converted to groupBy with aggregate functions

### 2. wf_s_m_multiple_unc11.xml

**Original Mapping:** m_multiple_unc11

**Sources:**
- EMPLOYEE (Oracle) - Employee data with 7 fields

**Targets:**
- TGT_FLT - Filtered employee data
- TARGET_AGG - Aggregated salary data
- UL_TGT_EMP - Unconnected lookup target

**Transformations Converted:**
| Transformation | Type | PySpark Implementation |
|----------------|------|------------------------|
| SQ_EMPLOYEE | Source Qualifier | spark.read.csv() |
| LKPTRANS | Unconnected Lookup | Broadcast join on DEPTNO |
| LKPTRANS1 | Unconnected Lookup | Broadcast join on EMPNO |
| EXPTRANS | Expression | withColumn() |
| FILTRANS | Filter | .filter() with lookup condition |
| EXPTRANS1 | Expression | withColumn() |
| AGGTRANS | Aggregator | .groupBy().agg() with MAX/MIN |

**Manual Conversions:**
- Unconnected lookups `:LKP.LKPTRANS(DEPTNO)` converted to broadcast joins
- Filter with lookup condition converted to join-based filtering
- Aggregator with lookup values converted to join + groupBy

### 3. wf_s_m_multiple_mapplet.XML

**Original Mapping:** m_multiple_mapplet

**Sources:**
- EMPLOYEEDETAILS (Oracle) - Employee details with 7 fields

**Targets:**
- TARGET_EMP - Employee output with department and location

**Mapplets Converted:**
| Mapplet | Transformations | PySpark Implementation |
|---------|-----------------|------------------------|
| mp_emp | EXPTRANS, SRTTRANS, Lookup | Inlined: concat(FIRSTNAME, LASTNAME), broadcast join for DNAME |
| mp_lkp_loc | LKPTRANS | Inlined: broadcast join for LOC |

**Transformations Converted:**
| Transformation | Type | PySpark Implementation |
|----------------|------|------------------------|
| SQ_EMPLOYEEDETAILS | Source Qualifier | spark.read.csv() |
| EXPTRANS_1 | Expression | withColumn() with LTRIM/RTRIM |
| COM_EXPTRANS | Expression | Pass-through |

**Manual Conversions:**
- Mapplet mp_emp inlined with concat() for NAME field
- Mapplet mp_lkp_loc inlined with broadcast join for LOC lookup
- LTRIM(RTRIM(PHONENO)) converted to F.trim()

### 4. wf_m_mapplet_multiple_unconnected.XML

**Original Mapping:** m_mapplet_multiple_unconnected

**Sources:**
- EMPLOYEE (Oracle) - Employee data with 7 fields

**Targets:**
- TGT_FLT - Filtered employee data with department name and location

**Mapplet Converted:**
| Mapplet | Transformations | PySpark Implementation |
|---------|-----------------|------------------------|
| mp_mutiple_unc | LKPTRANS, LKPTRANS1, FILTRANS, MP_EXPTRANS | Inlined with broadcast joins and filter |

**Transformations Converted:**
| Transformation | Type | PySpark Implementation |
|----------------|------|------------------------|
| SQ_EMPLOYEE | Source Qualifier | spark.read.csv() |
| EXPTRANS | Expression | .select() pass-through |
| mp_mutiple_unc | Mapplet | Inlined transformations |
| EXPTRANS1 | Expression | .select() with all fields |

**Manual Conversions:**
- Mapplet with unconnected lookups inlined
- LKPTRANS (DEPTNO -> DEPTNAME) converted to broadcast join
- LKPTRANS1 (DEPTNO -> LOC) converted to broadcast join
- FILTRANS converted to .filter() on lookup results

### 5. wf_s_m_complex5.XML (High Complexity)

**Original Mapping:** m_complex3

**Sources (5):**
- EMP_DEPT (Oracle) - Department reference data
- EMPLOYEE_DETAILS1 (Oracle) - Employee source 1
- EMPLOYEE_DETAILS2 (Oracle) - Employee source 2
- EMPLOYEEDETAILS (Oracle) - Employee source 3
- EMPLOYEE_DETAILS_FLAT (Flat File) - Employee source 4

**Targets (5):**
- TARGET_UPDATE - Records for update (DEPID=30)
- TARGET_INSERT - Records for insert (DEPID=20)
- T_DEFAULT - Default router output
- T_EMP_DEP10 - Router output for DEPID=10
- EMP_MAX_MIN - Aggregator output with MAX/MIN salary

**Transformations Converted:**
| Transformation | Type | PySpark Implementation |
|----------------|------|------------------------|
| Union | Custom | .union() with schema alignment |
| SRTTRANS | Sorter | .orderBy() |
| SRTTRANS1 | Sorter | .orderBy() |
| JNRTRANS | Joiner | .join() on DEPTNO = DEPID |
| mp_name | Mapplet | Inlined: concat(FIRSTNAME, LASTNAME) |
| EXPTRANS | Expression | withColumn() with trim() and lookup |
| AGGTRANS | Aggregator | .groupBy().agg() with MAX/MIN |
| RTRTRANS | Router | Multiple .filter() branches |
| LKPTRANS | Lookup | Broadcast join for update strategy |
| LKPTRANS1 | Unconnected Lookup | Broadcast join for ELOC |
| EXPTRANS2 | Expression | withColumn() for NEWRECORD_FLAG, UPDATERECORD_FLAG |
| FILTRANS_INSERT | Filter | .filter(DEPID == 20) |
| FILTRANS_UPDATE | Filter | .filter(DEPID == 30) |
| UPDTRANS | Update Strategy | Flag-based filtering |

**Manual Conversions:**
- Union of 4 sources with schema alignment
- Router with 2 groups (DEPID=10, DEFAULT) converted to multiple filters
- Update strategy converted to lookup + flag-based filtering
- Unconnected lookup for ELOC converted to broadcast join
- Mapplet mp_name inlined

## Conversion Patterns Applied

### 1. Unconnected Lookups
Informatica unconnected lookups (`:LKP.LKPTRANS(field)`) were converted to broadcast joins for efficiency:
```python
df_lookup_broadcast = F.broadcast(df_lookup)
df_result = df_source.join(
    df_lookup_broadcast,
    df_source["key"] == df_lookup["lookup_key"],
    "left"
)
```

### 2. Router Transformations
Router transformations with multiple output groups were converted to multiple filter operations:
```python
df_group1 = df_input.filter(F.col("field") == value1)
df_default = df_input.filter(F.col("field") != value1)
```

### 3. Union Transformations
Union transformations were converted using PySpark's union() with schema alignment:
```python
df_union = df_src1.union(df_src2).union(df_src3).union(df_src4)
```

### 4. Mapplets
Mapplets were inlined into the main mapping, with their internal transformations converted to equivalent PySpark operations.

### 5. Update Strategy
Update strategy transformations were converted to flag-based filtering using lookup results:
```python
df_with_flags = df_input.withColumn(
    "NEWRECORD_FLAG",
    F.when(F.col("lookup_key").isNull(), "TRUE").otherwise("FALSE")
)
```

### 6. Expression Transformations
Informatica expressions were converted to PySpark equivalents:
- `IIF(condition, true_val, false_val)` -> `F.when(condition, true_val).otherwise(false_val)`
- `LTRIM(RTRIM(field))` -> `F.trim(F.col("field"))`
- `field1 || field2` -> `F.concat(F.col("field1"), F.col("field2"))`

## Known Limitations

1. **Database Connections**: All database connections have been replaced with CSV file reads for testing purposes. Production deployment would require updating connection strings.

2. **Session Properties**: Informatica session properties (commit intervals, error handling) are not directly translated. PySpark uses different mechanisms for these.

3. **Data Types**: Some Informatica-specific data types may require additional casting in production.

4. **Error Handling**: Informatica's row-level error handling is not directly replicated. PySpark uses different error handling patterns.

## File Structure

```
generated/
├── pyspark/
│   ├── wf_m_PatternForChurnData_etl.py
│   ├── wf_m_mapplet_multiple_unconnected_etl.py
│   ├── wf_s_m_complex5_etl.py
│   ├── wf_s_m_multiple_mapplet_etl.py
│   └── wf_s_m_multiple_unc11_etl.py
├── sample_data/
│   ├── churndata.csv
│   ├── ddept.csv
│   ├── dept.csv
│   ├── emp_dept.csv
│   ├── emp_loc.csv
│   ├── emp_loc_dept.csv
│   ├── emp_loc_empid.csv
│   ├── employee.csv
│   ├── employee_details1.csv
│   ├── employee_details2.csv
│   ├── employee_details_flat.csv
│   ├── employeedetails.csv
│   ├── gender.csv
│   └── target_lookup.csv
└── metadata/
    └── [parsed metadata JSON files]
```

## Recommendations for Production

1. **Connection Management**: Replace CSV file reads with proper JDBC connections or cloud storage reads.

2. **Error Handling**: Implement try-catch blocks and logging for production error handling.

3. **Performance Tuning**: Adjust Spark configurations (executor memory, partitions) based on data volumes.

4. **Testing**: Run parallel tests comparing Informatica and PySpark outputs before full migration.

5. **Monitoring**: Implement Spark UI monitoring and alerting for production jobs.
