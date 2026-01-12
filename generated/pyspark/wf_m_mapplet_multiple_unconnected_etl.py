"""
PySpark ETL Script - Migrated from Informatica PowerCenter
Source: wf_m_mapplet_multiple_unconnected.XML
Mapping: m_mapplet_multiple_unconnected

This ETL includes:
- Mapplet with multiple unconnected lookups (mp_mutiple_unc)
- Unconnected lookups implemented as broadcast joins
- Department name and location lookups
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Informatica_to_PySpark_m_mapplet_multiple_unconnected") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.broadcastTimeout", "600") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_mapplet_multiple_unconnected
# Description: ETL with mapplet containing multiple unconnected lookups
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
# STEP 2: LOOKUP TABLES (for unconnected lookups in mapplet)
# ============================================================================

# Lookup table: LKP_DEPT - Department lookup for DEPTNAME
# Unconnected lookup: :LKP.LKP_DEPT(DEPTNO)
df_lkp_dept = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "DEPT") \
    .option("user", "username") \
    .option("password", "password") \
    .load() \
    .select(
        F.col("DEPTNO").alias("LKP_DEPTNO"),
        F.col("DNAME").alias("DEPTNAME"),
        F.col("LOC")
    )

# Broadcast the lookup table for efficient joins
df_lkp_dept_broadcast = F.broadcast(df_lkp_dept)

# ============================================================================
# STEP 3: TRANSFORMATIONS
# ============================================================================

# Expression transformation: EXPTRANS (pass-through)
df_exptrans = df_employee.select(
    F.col("EMPNO"),
    F.col("ENAME"),
    F.col("JOB"),
    F.col("MGR"),
    F.col("SAL"),
    F.col("COMM"),
    F.col("DEPTNO")
)

# ============================================================================
# STEP 4: MAPPLET - mp_mutiple_unc
# Contains multiple unconnected lookups for DEPTNAME and LOC
# Implemented as broadcast joins
# ============================================================================

# Join with department lookup to get DEPTNAME and LOC
# This replaces the unconnected lookup calls:
# - :LKP.LKP_DEPT(DEPTNO) for DEPTNAME
# - :LKP.LKP_LOC(DEPTNO) for LOC
df_with_lookups = df_exptrans.alias("emp").join(
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
    F.coalesce(F.col("dept.DEPTNAME"), F.lit("")).alias("DEPTNAME"),
    F.coalesce(F.col("dept.LOC"), F.lit("")).alias("LOC")
)

# ============================================================================
# STEP 5: EXPRESSION TRANSFORMATION - EXPTRANS1
# Final expression transformation before target
# ============================================================================

df_exptrans1 = df_with_lookups.select(
    F.col("EMPNO"),
    F.col("ENAME"),
    F.col("JOB"),
    F.col("MGR"),
    F.col("SAL"),
    F.col("COMM"),
    F.col("DEPTNO"),
    F.col("DEPTNAME"),
    F.col("LOC")
)

# ============================================================================
# STEP 6: WRITE TO TARGETS
# ============================================================================

# Write to target: TGT_FLT (Flat File)
df_exptrans1.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/tgt_flt_mapplet_unconnected")

print("ETL completed successfully!")
print("Output files written to: generated/outputs/tgt_flt_mapplet_unconnected")

# Stop Spark session
spark.stop()
