"""
PySpark ETL: wf_s_m_multiple_unc11
Migrated from Informatica PowerCenter XML

Original Mapping: m_multiple_unc
Sources: EMPLOYEE (Oracle Database)
Targets: TGT_FLT (Flat File), TARGET_AGG (Flat File), UL_TGT_EMP (Oracle)

Transformations:
- SQ_EMPLOYEE: Source Qualifier
- LKPTRANS: Unconnected Lookup to DDEPT table (returns DNAME based on DEPTNO)
- LKPTRANS1: Unconnected Lookup to EMP_LOC table (returns LOC based on EMPNO)
- EXPTRANS: Expression with lookup call for DEPTNAME
- FILTRANS: Filter with lookup condition
- EXPTRANS1: Expression with lookup call for LOC
- AGGTRANS: Aggregator with MAX/MIN salary calculations

Manual Conversions Applied:
- Unconnected lookups converted to broadcast joins
- Filter with lookup converted to join-based filter
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("wf_s_m_multiple_unc11_ETL") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_multiple_unc
################################################################################

# ============================================================================
# STEP 1: READ SOURCES
# ============================================================================

# Read source: EMPLOYEE (simulated with CSV for testing)
df_employee = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employee.csv")

# Read lookup tables (simulated with CSV for testing)
# LKPTRANS: Lookup to DDEPT table (D_DEPTNO -> DNAME)
df_ddept = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/ddept.csv")

# LKPTRANS1: Lookup to EMP_LOC table (EMPID -> LOC)
df_emp_loc = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/emp_loc_empid.csv")

# ============================================================================
# STEP 2: TRANSFORMATIONS
# ============================================================================

# Broadcast lookup tables for efficient joins (simulating unconnected lookups)
df_ddept_broadcast = F.broadcast(df_ddept)
df_emp_loc_broadcast = F.broadcast(df_emp_loc)

# Expression transformation: EXPTRANS
# Unconnected lookup :LKP.LKPTRANS(DEPTNO) converted to left join
df_exptrans = df_employee.join(
    df_ddept_broadcast,
    df_employee["DEPTNO"] == df_ddept["D_DEPTNO"],
    "left"
).select(
    df_employee["EMPNO"],
    df_employee["ENAME"],
    df_employee["JOB"],
    df_employee["MGR"],
    df_employee["SAL"],
    df_employee["COMM"],
    df_employee["DEPTNO"],
    df_ddept["DNAME"].alias("DEPTNAME")
)

# Filter transformation: FILTRANS
# Filter condition: DEPTNAME = :LKP.LKPTRANS(DEPTNO)
# This is a self-referential filter - keep rows where DEPTNAME matches lookup result
# Since we already joined, we filter where DEPTNAME is not null (successful lookup)
df_filtrans = df_exptrans.filter(F.col("DEPTNAME").isNotNull())

# Expression transformation: EXPTRANS1
# Unconnected lookup :LKP.LKPTRANS1(EMPNO) converted to left join for LOC
df_exptrans1 = df_filtrans.join(
    df_emp_loc_broadcast,
    df_filtrans["EMPNO"] == df_emp_loc["EMPID"],
    "left"
).select(
    df_filtrans["EMPNO"],
    df_filtrans["ENAME"],
    df_filtrans["JOB"],
    df_filtrans["MGR"],
    df_filtrans["SAL"],
    df_filtrans["COMM"],
    df_filtrans["DEPTNO"],
    df_filtrans["DEPTNAME"],
    df_emp_loc["LOC"]
)

# Aggregator transformation: AGGTRANS
# Group by employee fields, calculate MAX/MIN salary
# Unconnected lookup FIRST(:LKP.LKPTRANS1(EMPNO)) for LOC
df_aggtrans = df_exptrans.join(
    df_emp_loc_broadcast,
    df_exptrans["EMPNO"] == df_emp_loc["EMPID"],
    "left"
).groupBy("EMPNO", "ENAME", "JOB", "MGR") \
    .agg(
        F.first("SAL").alias("SAL"),
        F.max("SAL").alias("MAXSAL"),
        F.min("SAL").alias("MINSAL"),
        F.first("LOC").alias("LOC")
    )

# ============================================================================
# STEP 3: WRITE TO TARGETS
# ============================================================================

# Write to target: TGT_FLT (Flat File)
# Columns: EMPNO, ENAME, JOB, MGR, SAL, COMM, DEPTNO, DEPTNAME, LOC
# Source: EXPTRANS1
df_tgt_flt = df_exptrans1.select(
    "EMPNO", "ENAME", "JOB", "MGR", "SAL", "COMM", "DEPTNO", "DEPTNAME", "LOC"
)
df_tgt_flt.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/TGT_FLT")

# Write to target: TARGET_AGG (Flat File)
# Columns: EMPNO, ENAME, JOB, MGR, SAL, MAXSAL, MINSAL, LOC
# Source: AGGTRANS
df_target_agg = df_aggtrans.select(
    "EMPNO", "ENAME", "JOB", "MGR", "SAL", "MAXSAL", "MINSAL", "LOC"
)
df_target_agg.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/TARGET_AGG")

# Write to target: UL_TGT_EMP (Oracle - simulated as CSV for testing)
# Columns: EMPNO, ENAME, JOB, MGR, SAL, COMM, DEPTNO, DEPTNAME
# Source: EXPTRANS
df_ul_tgt_emp = df_exptrans.select(
    "EMPNO", "ENAME", "JOB", "MGR", "SAL", "COMM", "DEPTNO", "DEPTNAME"
)
df_ul_tgt_emp.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/UL_TGT_EMP")

print("ETL completed successfully!")
print(f"Records processed from EMPLOYEE: {df_employee.count()}")
print(f"Records written to TGT_FLT: {df_tgt_flt.count()}")
print(f"Records written to TARGET_AGG: {df_target_agg.count()}")
print(f"Records written to UL_TGT_EMP: {df_ul_tgt_emp.count()}")

# Stop Spark session
spark.stop()
