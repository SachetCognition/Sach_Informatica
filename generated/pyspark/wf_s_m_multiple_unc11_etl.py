"""
PySpark ETL Script - Migrated from Informatica PowerCenter
Source: wf_s_m_multiple_unc11.xml
Mapping: m_multiple_unc

This ETL includes:
- Aggregator transformation with SUM calculations
- Multiple unconnected lookups (LKPTRANS, LKPTRANS1)
- Filter transformation with lookup condition
- Unconnected lookups implemented as broadcast joins
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Informatica_to_PySpark_m_multiple_unc") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.broadcastTimeout", "600") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_multiple_unc
# Description: ETL with aggregator and multiple unconnected lookups
################################################################################

# ============================================================================
# STEP 1: READ SOURCES
# ============================================================================

# Read source: EMPLOYEE (Database: Oracle)
# TODO: Update connection string with actual database credentials
df_employee = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMPLOYEE") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# ============================================================================
# STEP 2: LOOKUP TABLES (for unconnected lookups)
# ============================================================================

# Lookup table: LKPTRANS - Department lookup for DEPTNAME
# Unconnected lookup: :LKP.LKPTRANS(DEPTNO)
df_lkp_dept = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "DEPT") \
    .option("user", "username") \
    .option("password", "password") \
    .load() \
    .select(
        F.col("DEPTNO").alias("LKP_DEPTNO"),
        F.col("DNAME").alias("DEPTNAME")
    )

# Broadcast the lookup table for efficient joins
df_lkp_dept_broadcast = F.broadcast(df_lkp_dept)

# Lookup table: LKPTRANS1 - Employee location lookup
# Unconnected lookup: :LKP.LKPTRANS1(EMPNO)
df_lkp_emp_loc = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMP_LOC") \
    .option("user", "username") \
    .option("password", "password") \
    .load() \
    .select(
        F.col("EMPNO").alias("LKP_EMPNO"),
        F.col("LOC")
    )

# Broadcast the location lookup
df_lkp_emp_loc_broadcast = F.broadcast(df_lkp_emp_loc)

# ============================================================================
# STEP 3: SOURCE QUALIFIER
# ============================================================================

# Source Qualifier: SQ_EMPLOYEE
df_sq_employee = df_employee.select(
    F.col("EMPNO"),
    F.col("ENAME"),
    F.col("JOB"),
    F.col("MGR"),
    F.col("SAL"),
    F.col("COMM"),
    F.col("DEPTNO")
)

# ============================================================================
# STEP 4: EXPRESSION TRANSFORMATION - EXPTRANS
# Includes unconnected lookup call: :LKP.LKPTRANS(DEPTNO) for DEPTNAME
# Implemented as broadcast join
# ============================================================================

df_exptrans = df_sq_employee.alias("emp").join(
    df_lkp_dept_broadcast.alias("dept"),
    F.col("emp.DEPTNO") == F.col("dept.LKP_DEPTNO"),
    "left"
).select(
    F.col("emp.EMPNO"),
    F.col("emp.ENAME"),
    F.col("emp.JOB"),
    F.col("emp.MGR"),
    F.col("emp.SAL"),
    F.col("emp.COMM"),
    F.col("emp.DEPTNO"),
    F.coalesce(F.col("dept.DEPTNAME"), F.lit("")).alias("DEPTNAME")
)

# ============================================================================
# STEP 5: AGGREGATOR TRANSFORMATION - AGGTRANS
# Calculate SUM of salary by department
# ============================================================================

df_aggtrans = df_exptrans.groupBy(
    "DEPTNO",
    "DEPTNAME"
).agg(
    F.sum("SAL").alias("TOTAL_SAL"),
    F.count("*").alias("EMP_COUNT"),
    F.avg("SAL").alias("AVG_SAL")
)

# ============================================================================
# STEP 6: FILTER TRANSFORMATION - FILTRANS
# Filter based on lookup condition
# Original: IIF(ISNULL(:LKP.LKPTRANS(DEPTNO)), FALSE, TRUE)
# Implemented: Filter records where DEPTNAME is not null (lookup found)
# ============================================================================

df_filtrans = df_exptrans.filter(
    F.col("DEPTNAME") != ""
)

# ============================================================================
# STEP 7: EXPRESSION TRANSFORMATION - EXPTRANS1
# Includes unconnected lookup call: :LKP.LKPTRANS1(EMPNO) for LOC
# Implemented as broadcast join
# ============================================================================

df_exptrans1 = df_filtrans.alias("emp").join(
    df_lkp_emp_loc_broadcast.alias("loc"),
    F.col("emp.EMPNO") == F.col("loc.LKP_EMPNO"),
    "left"
).select(
    F.col("emp.EMPNO"),
    F.col("emp.ENAME"),
    F.col("emp.JOB"),
    F.col("emp.MGR"),
    F.col("emp.SAL"),
    F.col("emp.COMM"),
    F.col("emp.DEPTNO"),
    F.col("emp.DEPTNAME"),
    F.coalesce(F.col("loc.LOC"), F.lit("")).alias("LOC")
)

# ============================================================================
# STEP 8: WRITE TO TARGETS
# ============================================================================

# Write to target: TGT_FLT (Flat File) - Filtered employee data with lookups
df_exptrans1.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/tgt_flt_unc11")

# Write to target: UL_TGT_EMP (Database: Oracle) - Aggregated data
# For testing, writing to CSV instead:
df_aggtrans.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/ul_tgt_emp")

# Uncomment below for actual JDBC write:
# df_aggtrans.write \
#     .format("jdbc") \
#     .option("url", "jdbc:oracle:thin:@//host:port/service") \
#     .option("dbtable", "UL_TGT_EMP") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .mode("overwrite") \
#     .save()

print("ETL completed successfully!")
print("Output files written to: generated/outputs/")
print("  - tgt_flt_unc11: Filtered employee data with department and location lookups")
print("  - ul_tgt_emp: Aggregated salary data by department")

# Stop Spark session
spark.stop()
