"""
PySpark ETL: wf_s_m_complex5
Migrated from Informatica PowerCenter XML

Original Mapping: m_complex3
Sources (5):
- EMP_DEPT (Oracle) - Department reference data
- EMPLOYEE_DETAILS1 (Oracle) - Employee data source 1
- EMPLOYEE_DETAILS2 (Oracle) - Employee data source 2
- EMPLOYEEDETAILS (Oracle) - Employee data source 3
- EMPLOYEE_DETAILS_FLAT (Flat File) - Employee data source 4

Targets (5):
- TARGET_UPDATE (Flat File) - Records for update (DEPID=30)
- TARGET_INSERT (Flat File) - Records for insert (DEPID=20)
- T_DEFAULT (Oracle - simulated as CSV) - Default router output
- T_EMP_DEP10 (Oracle - simulated as CSV) - Router output for DEPID=10
- EMP_MAX_MIN (Oracle - simulated as CSV) - Aggregator output with MAX/MIN salary

Key Transformations:
- Union: Combines 4 employee sources
- Joiner: Joins employee with department on DEPTNO = DEPID
- Aggregator: Calculates MAX/MIN salary grouped by employee
- Router: Routes records based on DEPID (10 vs DEFAULT)
- Update Strategy: Determines INSERT vs UPDATE based on lookup match
- Unconnected Lookup: LKPTRANS1 for ELOC lookup

Manual Conversions Applied:
- Union transformation converted to .union() with schema alignment
- Router transformation converted to multiple .filter() branches
- Unconnected lookup converted to broadcast join
- Update strategy converted to flag-based filtering
- Mapplet mp_name inlined (FIRSTNAME + LASTNAME -> NAME)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("wf_s_m_complex5_ETL") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_complex3
################################################################################

# ============================================================================
# STEP 1: READ SOURCES
# ============================================================================

# Read source: EMP_DEPT (simulated with CSV for testing)
df_emp_dept = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/emp_dept.csv")

# Read source: EMPLOYEE_DETAILS1 (simulated with CSV for testing)
df_employee_details1 = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employee_details1.csv")

# Read source: EMPLOYEE_DETAILS2 (simulated with CSV for testing)
df_employee_details2 = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employee_details2.csv")

# Read source: EMPLOYEEDETAILS (simulated with CSV for testing)
df_employeedetails = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employeedetails.csv")

# Read source: EMPLOYEE_DETAILS_FLAT (Flat File)
df_employee_details_flat = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employee_details_flat.csv")

# Read lookup tables
# EMP_LOC table for LKPTRANS1 (EMPID -> LOC)
df_emp_loc = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/emp_loc_empid.csv")

# TARGET_UPDATE for lookup to determine INSERT vs UPDATE
df_target_lookup = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/target_lookup.csv")

# ============================================================================
# STEP 2: TRANSFORMATIONS
# ============================================================================

# Broadcast lookup tables for efficient joins
df_emp_loc_broadcast = F.broadcast(df_emp_loc)
df_target_lookup_broadcast = F.broadcast(df_target_lookup)

# ---- UNION TRANSFORMATION ----
# Combine all 4 employee sources with schema alignment
# All sources have: EMPID, FIRSTNAME, LASTNAME, EMAIL, PHONENO, SALARY, DEPID

# Standardize column names and select common columns
common_cols = ["EMPID", "FIRSTNAME", "LASTNAME", "EMAIL", "PHONENO", "SALARY", "DEPID"]

df_src1 = df_employee_details1.select(*common_cols)
df_src2 = df_employee_details2.select(*common_cols)
df_src3 = df_employeedetails.select(*common_cols)
df_src4 = df_employee_details_flat.select(*common_cols)

# Union all sources
df_union = df_src1.union(df_src2).union(df_src3).union(df_src4)

# ---- SORTER: SRTTRANS ----
# Sort employee data by EMPID
df_srttrans = df_union.orderBy("EMPID")

# ---- SORTER: SRTTRANS1 ----
# Sort department data by DEPTNO
df_srttrans1 = df_emp_dept.orderBy("DEPTNO")

# ---- JOINER: JNRTRANS ----
# Join employee with department on DEPTNO = DEPID
df_jnrtrans = df_srttrans.join(
    df_srttrans1,
    df_srttrans["DEPID"] == df_srttrans1["DEPTNO"],
    "inner"
).select(
    df_srttrans["EMPID"],
    df_srttrans["FIRSTNAME"],
    df_srttrans["LASTNAME"],
    df_srttrans["EMAIL"],
    df_srttrans["PHONENO"],
    df_srttrans["SALARY"],
    df_srttrans["DEPID"],
    df_srttrans1["DNAME"],
    df_srttrans1["LOC"]
)

# ---- MAPPLET: mp_name (inlined) ----
# Concatenate FIRSTNAME + LASTNAME -> NAME
df_mp_name = df_jnrtrans.withColumn(
    "NAME", F.concat(F.col("FIRSTNAME"), F.lit(" "), F.col("LASTNAME"))
)

# ---- EXPRESSION: EXPTRANS ----
# Apply LTRIM(RTRIM(NAME)) -> ENAME
# Unconnected lookup LKPTRANS1 for ELOC (converted to broadcast join)
df_exptrans = df_mp_name.join(
    df_emp_loc_broadcast,
    df_mp_name["EMPID"] == df_emp_loc["EMPID"],
    "left"
).select(
    df_mp_name["EMPID"],
    df_mp_name["EMAIL"],
    df_mp_name["PHONENO"],
    df_mp_name["SALARY"],
    df_mp_name["DEPID"],
    df_mp_name["DNAME"],
    F.trim(df_mp_name["NAME"]).alias("ENAME"),
    df_emp_loc["LOC"].alias("ELOC")
)

# ---- AGGREGATOR: AGGTRANS ----
# Calculate MAX and MIN salary grouped by employee
df_aggtrans = df_exptrans.groupBy(
    "EMPID", "EMAIL", "PHONENO", "SALARY", "DEPID", "DNAME", "ENAME"
).agg(
    F.max("SALARY").alias("MAXSAL"),
    F.min("SALARY").alias("MINSAL")
)

# ---- ROUTER: RTRTRANS ----
# Route records based on DEPID
# Group 1: DEPID = 10 -> T_EMP_DEP10
# Default: All other records -> T_DEFAULT

df_router_dep10 = df_exptrans.filter(F.col("DEPID") == 10).select(
    F.col("EMPID").alias("EMPID1"),
    F.col("EMAIL").alias("EMAIL1"),
    F.col("PHONENO").alias("PHONENO1"),
    F.col("SALARY").alias("SALARY1"),
    F.col("DEPID").alias("DEPID1"),
    F.col("DNAME").alias("DNAME1"),
    F.col("ENAME").alias("NAME1")
)

df_router_default = df_exptrans.filter(F.col("DEPID") != 10).select(
    F.col("EMPID").alias("EMPID2"),
    F.col("EMAIL").alias("EMAIL2"),
    F.col("PHONENO").alias("PHONENO2"),
    F.col("SALARY").alias("SALARY2"),
    F.col("DEPID").alias("DEPID2"),
    F.col("DNAME").alias("DNAME2"),
    F.col("ENAME").alias("NAME2")
)

# ---- LOOKUP: LKPTRANS ----
# Lookup to determine if record exists in target (for INSERT vs UPDATE)
df_lkptrans = df_exptrans.join(
    df_target_lookup_broadcast,
    df_exptrans["EMPID"] == df_target_lookup["T_EMPID"],
    "left"
).select(
    df_exptrans["EMPID"],
    df_exptrans["EMAIL"],
    df_exptrans["PHONENO"],
    df_exptrans["SALARY"],
    df_exptrans["DEPID"],
    df_exptrans["DNAME"],
    df_exptrans["ENAME"],
    df_exptrans["ELOC"],
    df_target_lookup["T_EMPID"]
)

# ---- EXPRESSION: EXPTRANS2 ----
# Determine NEWRECORD_FLAG and UPDATERECORD_FLAG based on lookup result
df_exptrans2 = df_lkptrans.withColumn(
    "NEWRECORD_FLAG",
    F.when(F.col("T_EMPID").isNull(), "TRUE").otherwise("FALSE")
).withColumn(
    "UPDATERECORD_FLAG",
    F.when(F.col("T_EMPID").isNull(), "FALSE").otherwise("TRUE")
)

# ---- FILTER: FILTRANS_INSERT ----
# Filter for INSERT records (DEPID = 20)
df_filtrans_insert = df_exptrans2.filter(F.col("DEPID") == 20)

# ---- FILTER: FILTRANS_UPDATE ----
# Filter for UPDATE records (DEPID = 30)
df_filtrans_update = df_exptrans2.filter(F.col("DEPID") == 30)

# ============================================================================
# STEP 3: WRITE TO TARGETS
# ============================================================================

# Write to target: EMP_MAX_MIN (Oracle - simulated as CSV)
# Columns: EMPID, EMAIL, PHONENO, SALARY, DEPID, DNAME, NAME, MAXSAL, MINSAL
# Source: AGGTRANS (NAME comes from ENAME)
df_emp_max_min = df_aggtrans.select(
    "EMPID", "EMAIL", "PHONENO", "SALARY", "DEPID", "DNAME", 
    F.col("ENAME").alias("NAME"), "MAXSAL", "MINSAL"
)
df_emp_max_min.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/EMP_MAX_MIN")

# Write to target: T_EMP_DEP10 (Oracle - simulated as CSV)
# Columns: EMPID1, EMAIL1, PHONENO1, SALARY1, DEPID1, DNAME1, NAME1
# Source: RTRTRANS (Router output for DEPID=10)
df_t_emp_dep10 = df_router_dep10.select(
    "EMPID1", "EMAIL1", "PHONENO1", "SALARY1", "DEPID1", "DNAME1", "NAME1"
)
df_t_emp_dep10.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/T_EMP_DEP10")

# Write to target: T_DEFAULT (Oracle - simulated as CSV)
# Columns: EMPID2, EMAIL2, PHONENO2, SALARY2, DEPID2, DNAME2, NAME2
# Source: RTRTRANS (Router default output)
df_t_default = df_router_default.select(
    "EMPID2", "EMAIL2", "PHONENO2", "SALARY2", "DEPID2", "DNAME2", "NAME2"
)
df_t_default.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/T_DEFAULT")

# Write to target: TARGET_INSERT (Flat File)
# Columns: T_EMPID, EMAIL, PHONENO, SALARY, DEPID, DNAME, ENAME, ELOC
# Source: FILTRANS_INSERT (Insert records with DEPID=20)
df_target_insert = df_filtrans_insert.select(
    F.col("EMPID").alias("T_EMPID"),
    "EMAIL", "PHONENO", "SALARY", "DEPID", "DNAME", "ENAME", "ELOC"
)
df_target_insert.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/TARGET_INSERT")

# Write to target: TARGET_UPDATE (Flat File)
# Columns: T_EMPID, EMAIL, PHONENO, SALARY, DEPID, DNAME, ENAME, ELOC
# Source: UPDTRANS (Update records with DEPID=30)
df_target_update = df_filtrans_update.select(
    F.col("EMPID").alias("T_EMPID"),
    "EMAIL", "PHONENO", "SALARY", "DEPID", "DNAME", "ENAME", "ELOC"
)
df_target_update.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/TARGET_UPDATE")

print("ETL completed successfully!")
print(f"Records from Union: {df_union.count()}")
print(f"Records after Join: {df_jnrtrans.count()}")
print(f"Records to EMP_MAX_MIN: {df_emp_max_min.count()}")
print(f"Records to T_EMP_DEP10: {df_t_emp_dep10.count()}")
print(f"Records to T_DEFAULT: {df_t_default.count()}")
print(f"Records to TARGET_INSERT: {df_target_insert.count()}")
print(f"Records to TARGET_UPDATE: {df_target_update.count()}")

# Stop Spark session
spark.stop()
