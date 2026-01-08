"""
PySpark ETL: wf_s_m_multiple_mapplet
Migrated from Informatica PowerCenter XML

Original Mapping: m_multiple_mapplet
Sources: EMPLOYEEDETAILS (Oracle Database)
Targets: TARGET_EMP (Oracle Database - simulated as CSV)

Mapplets:
- mp_emp: Concatenates FIRSTNAME and LASTNAME into NAME, looks up DNAME from DEPT table
- mp_lkp_loc: Looks up LOC from EMP_LOC table based on DEPID

Data Flow: SQ_EMPLOYEEDETAILS -> mp_emp -> EXPTRANS_1 -> mp_lkp_loc -> COM_EXPTRANS -> TARGET_EMP

Manual Conversions Applied:
- Mapplets converted to inline PySpark transformations
- Lookup transformations converted to broadcast joins
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("wf_s_m_multiple_mapplet_ETL") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_multiple_mapplet
################################################################################

# ============================================================================
# STEP 1: READ SOURCES
# ============================================================================

# Read source: EMPLOYEEDETAILS (simulated with CSV for testing)
df_employeedetails = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employeedetails.csv")

# Read lookup tables for mapplets
# DEPT table for mp_emp mapplet (DEPID -> DNAME)
df_dept = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/dept.csv")

# EMP_LOC table for mp_lkp_loc mapplet (DEPID -> LOC)
df_emp_loc = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/emp_loc_dept.csv")

# ============================================================================
# STEP 2: TRANSFORMATIONS
# ============================================================================

# Broadcast lookup tables for efficient joins
df_dept_broadcast = F.broadcast(df_dept)
df_emp_loc_broadcast = F.broadcast(df_emp_loc)

# ---- MAPPLET: mp_emp ----
# Input: EMPID, FIRSTNAME, LASTNAME, EMAIL, PHONENO, SALARY, DEPID
# Output: EMPID, EMAIL, PHONENO, SALARY, DEPID, DNAME, NAME
# Transformations: Concatenate FIRSTNAME + LASTNAME -> NAME, Lookup DNAME from DEPT

df_mp_emp = df_employeedetails \
    .withColumn("NAME", F.concat(F.col("FIRSTNAME"), F.lit(" "), F.col("LASTNAME"))) \
    .join(
        df_dept_broadcast,
        df_employeedetails["DEPID"] == df_dept["DEPTNO"],
        "left"
    ).select(
        df_employeedetails["EMPID"],
        df_employeedetails["EMAIL"],
        df_employeedetails["PHONENO"],
        df_employeedetails["SALARY"],
        df_employeedetails["DEPID"],
        df_dept["DNAME"],
        F.concat(F.col("FIRSTNAME"), F.lit(" "), F.col("LASTNAME")).alias("NAME")
    )

# ---- EXPRESSION: EXPTRANS_1 ----
# Apply LTRIM(RTRIM(PHONENO)) transformation
df_exptrans_1 = df_mp_emp \
    .withColumn("O_PHONENO", F.trim(F.col("PHONENO")))

# ---- MAPPLET: mp_lkp_loc ----
# Input: EMPID, EMAIL, SALARY, DEPID, DNAME, NAME, PHONENO
# Output: EMPID, EMAIL, SALARY, DEPID, NAME, PHONENO, DNAME, LOC
# Transformation: Lookup LOC from EMP_LOC table based on DEPID

df_mp_lkp_loc = df_exptrans_1.join(
    df_emp_loc_broadcast,
    df_exptrans_1["DEPID"] == df_emp_loc["DEPID"],
    "left"
).select(
    df_exptrans_1["EMPID"],
    df_exptrans_1["EMAIL"],
    df_exptrans_1["SALARY"],
    df_exptrans_1["DEPID"],
    df_exptrans_1["NAME"],
    df_exptrans_1["O_PHONENO"].alias("PHONENO"),
    df_exptrans_1["DNAME"],
    df_emp_loc["LOC"]
)

# ---- EXPRESSION: COM_EXPTRANS ----
# Pass-through expression (no additional transformations)
df_com_exptrans = df_mp_lkp_loc

# ============================================================================
# STEP 3: WRITE TO TARGETS
# ============================================================================

# Write to target: TARGET_EMP (Oracle - simulated as CSV for testing)
# Columns: NAME, DNAME, EMPID, EMAIL, PHONENO, SALARY, DEPID, LOC
# Source: COM_EXPTRANS (PHONENO comes from O_PHONENO)
df_target_emp = df_com_exptrans.select(
    "NAME", "DNAME", "EMPID", "EMAIL", "PHONENO", "SALARY", "DEPID", "LOC"
)
df_target_emp.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/TARGET_EMP")

print("ETL completed successfully!")
print(f"Records processed from EMPLOYEEDETAILS: {df_employeedetails.count()}")
print(f"Records written to TARGET_EMP: {df_target_emp.count()}")

# Stop Spark session
spark.stop()
