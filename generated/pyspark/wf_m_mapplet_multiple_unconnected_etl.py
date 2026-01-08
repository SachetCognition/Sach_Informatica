"""
PySpark ETL: wf_m_mapplet_multiple_unconnected
Migrated from Informatica PowerCenter XML

Original Mapping: m_mapplet_multiple_unconnected
Sources: EMPLOYEE (Oracle Database)
Targets: TGT_FLT (Flat File - simulated as CSV)

Mapplet: mp_mutiple_unc
- Contains unconnected lookups: LKPTRANS (DEPTNO -> DEPTNAME), LKPTRANS1 (DEPTNO -> LOC)
- Contains filter transformation: FILTRANS
- Contains expression transformations: MP_EXPTRANS, MP_EXPTRANS1

Data Flow: SQ_EMPLOYEE -> EXPTRANS -> mp_mutiple_unc -> EXPTRANS1 -> TGT_FLT

Manual Conversions Applied:
- Mapplet mp_mutiple_unc inlined into main mapping
- Unconnected lookups converted to broadcast joins
- Filter transformation converted to .filter() operation
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("wf_m_mapplet_multiple_unconnected_ETL") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_mapplet_multiple_unconnected
################################################################################

# ============================================================================
# STEP 1: READ SOURCES
# ============================================================================

# Read source: EMPLOYEE (simulated with CSV for testing)
df_employee = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employee.csv")

# Read lookup tables for unconnected lookups in mapplet
# DDEPT table for LKPTRANS (DEPTNO -> DEPTNAME)
df_ddept = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/ddept.csv")

# EMP_LOC table for LKPTRANS1 (DEPTNO -> LOC)
df_emp_loc = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/emp_loc.csv")

# ============================================================================
# STEP 2: TRANSFORMATIONS
# ============================================================================

# Broadcast lookup tables for efficient joins
df_ddept_broadcast = F.broadcast(df_ddept)
df_emp_loc_broadcast = F.broadcast(df_emp_loc)

# ---- EXPRESSION: EXPTRANS ----
# Pass-through expression (no transformations, just passes all fields)
df_exptrans = df_employee.select(
    "EMPNO", "ENAME", "JOB", "MGR", "SAL", "COMM", "DEPTNO"
)

# ---- MAPPLET: mp_mutiple_unc (inlined) ----
# Input: EMPNO, ENAME, JOB, MGR, SAL, COMM, DEPTNO
# Output: EMPNO, ENAME, JOB, MGR, SAL, COMM, DEPTNO, DEPTNAME, LOC
# Transformations: LKPTRANS (lookup DEPTNAME), LKPTRANS1 (lookup LOC), FILTRANS (filter)

# MP_EXPTRANS - pass-through expression inside mapplet
df_mp_exptrans = df_exptrans

# LKPTRANS - Unconnected lookup for DEPTNAME (converted to broadcast join)
df_with_deptname = df_mp_exptrans.join(
    df_ddept_broadcast,
    df_mp_exptrans["DEPTNO"] == df_ddept["D_DEPTNO"],
    "left"
).select(
    df_mp_exptrans["EMPNO"],
    df_mp_exptrans["ENAME"],
    df_mp_exptrans["JOB"],
    df_mp_exptrans["MGR"],
    df_mp_exptrans["SAL"],
    df_mp_exptrans["COMM"],
    df_mp_exptrans["DEPTNO"],
    df_ddept["DNAME"].alias("DEPTNAME")
)

# LKPTRANS1 - Unconnected lookup for LOC (converted to broadcast join)
df_with_loc = df_with_deptname.join(
    df_emp_loc_broadcast,
    df_with_deptname["DEPTNO"] == df_emp_loc["DEPID"],
    "left"
).select(
    df_with_deptname["EMPNO"],
    df_with_deptname["ENAME"],
    df_with_deptname["JOB"],
    df_with_deptname["MGR"],
    df_with_deptname["SAL"],
    df_with_deptname["COMM"],
    df_with_deptname["DEPTNO"],
    df_with_deptname["DEPTNAME"],
    df_emp_loc["LOC"]
)

# FILTRANS - Filter transformation (filter records where lookup returned values)
# In Informatica, unconnected lookups typically filter out records where lookup fails
df_filtrans = df_with_loc.filter(
    (F.col("DEPTNAME").isNotNull()) & (F.col("LOC").isNotNull())
)

# MP_EXPTRANS1 - pass-through expression inside mapplet
df_mp_exptrans1 = df_filtrans

# ---- EXPRESSION: EXPTRANS1 ----
# Pass-through expression (passes all fields including DEPTNAME and LOC from mapplet)
df_exptrans1 = df_mp_exptrans1.select(
    "EMPNO", "ENAME", "JOB", "MGR", "SAL", "COMM", "DEPTNO", "DEPTNAME", "LOC"
)

# ============================================================================
# STEP 3: WRITE TO TARGETS
# ============================================================================

# Write to target: TGT_FLT (Flat File - simulated as CSV for testing)
# Columns: EMPNO, ENAME, JOB, MGR, SAL, COMM, DEPTNO, DEPTNAME, LOC
# Source: EXPTRANS1
df_tgt_flt = df_exptrans1.select(
    "EMPNO", "ENAME", "JOB", "MGR", "SAL", "COMM", "DEPTNO", "DEPTNAME", "LOC"
)
df_tgt_flt.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/TGT_FLT_MAPPLET")

print("ETL completed successfully!")
print(f"Records processed from EMPLOYEE: {df_employee.count()}")
print(f"Records written to TGT_FLT: {df_tgt_flt.count()}")

# Stop Spark session
spark.stop()
