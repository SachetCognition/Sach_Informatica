"""
PySpark ETL Script - Migrated from Informatica PowerCenter
Source: wf_s_m_complex5.XML
Mapping: m_complex3

This is a complex ETL with:
- Union transformation (5 sources combined)
- Router transformation (multiple filter branches)
- Unconnected lookups (implemented as broadcast joins)
- Update Strategy (insert/update logic)
- Aggregator with MAX/MIN calculations
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Informatica_to_PySpark_m_complex3") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.broadcastTimeout", "600") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_complex3
# Description: Complex ETL with Union, Router, Lookups, and Update Strategy
################################################################################

# ============================================================================
# STEP 1: READ SOURCES
# ============================================================================

# Read source: EMP_DEPT (Database: Oracle)
# TODO: Update connection string with actual database credentials
df_emp_dept = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMP_DEPT") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# Read source: EMPLOYEE_DETAILS1 (Database: Oracle)
df_employee_details1 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMPLOYEE_DETAILS1") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# Read source: EMPLOYEE_DETAILS2 (Database: Oracle)
df_employee_details2 = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMPLOYEE_DETAILS2") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# Read source: EMPLOYEEDETAILS (Database: Oracle)
df_employeedetails = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMPLOYEEDETAILS") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# Read source: EMPLOYEE_DETAILS_FLAT (Flat File)
df_employee_details_flat = spark.read \
    .option("header", "true") \
    .option("delimiter", "\t") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employee_details_flat.csv")

# ============================================================================
# STEP 2: LOOKUP TABLES (for unconnected lookups - loaded as broadcast)
# ============================================================================

# Lookup table: LKPTRANS - Employee lookup for existing records
# Source: Flat File with employee data
df_lkp_employee = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/employee_details_flat.csv") \
    .select(
        F.col("EMPID").alias("T_EMPID"),
        F.col("EMAIL").alias("LKP_EMAIL"),
        F.col("PHONENO").alias("LKP_PHONENO"),
        F.col("SALARY").alias("LKP_SALARY"),
        F.col("DEPID").alias("LKP_DEPID")
    )

# Broadcast the lookup table for efficient joins
df_lkp_employee_broadcast = F.broadcast(df_lkp_employee)

# Lookup table: LKPTRANS1 - Employee location lookup (Database)
# TODO: Update with actual EMP_LOC table connection
df_lkp_emp_loc = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMP_LOC") \
    .option("user", "username") \
    .option("password", "password") \
    .load() \
    .select(
        F.col("EMPID").alias("LOC_EMPID"),
        F.col("LOC")
    )

# Broadcast the location lookup
df_lkp_emp_loc_broadcast = F.broadcast(df_lkp_emp_loc)

# ============================================================================
# STEP 3: UNION TRANSFORMATION
# Combines data from 5 sources with schema alignment
# ============================================================================

# Define common schema for union
common_columns = ["EMPID", "FIRSTNAME", "LASTNAME", "EMAIL", "PHONENO", "SALARY", "DEPID"]

# Align schemas and union all sources
df_sq_flat = df_employee_details_flat.select(
    F.col("EMPID").cast("double"),
    F.col("FIRSTNAME").cast("string"),
    F.col("LASTNAME").cast("string"),
    F.col("EMAIL").cast("string"),
    F.col("PHONENO").cast("string"),
    F.col("SALARY").cast("double"),
    F.col("DEPID").cast("double")
)

df_sq_details1 = df_employee_details1.select(
    F.col("EMPID").cast("double"),
    F.col("FIRSTNAME").cast("string"),
    F.col("LASTNAME").cast("string"),
    F.col("EMAIL").cast("string"),
    F.col("PHONENO").cast("string"),
    F.col("SALARY").cast("double"),
    F.col("DEPID").cast("double")
)

df_sq_details2 = df_employee_details2.select(
    F.col("EMPID").cast("double"),
    F.col("FIRSTNAME").cast("string"),
    F.col("LASTNAME").cast("string"),
    F.col("EMAIL").cast("string"),
    F.col("PHONENO").cast("string"),
    F.col("SALARY").cast("double"),
    F.col("DEPID").cast("double")
)

df_sq_employeedetails = df_employeedetails.select(
    F.col("EMPID").cast("double"),
    F.col("FIRSTNAME").cast("string"),
    F.col("LASTNAME").cast("string"),
    F.col("EMAIL").cast("string"),
    F.col("PHONENO").cast("string"),
    F.col("SALARY").cast("double"),
    F.col("DEPID").cast("double")
)

# Union all sources (equivalent to Informatica Union transformation)
df_union = df_sq_flat \
    .union(df_sq_details1) \
    .union(df_sq_details2) \
    .union(df_sq_employeedetails)

# ============================================================================
# STEP 4: SORTER TRANSFORMATIONS
# ============================================================================

# Sorter transformation: SRTTRANS - Sort employee data
df_srttrans = df_union.orderBy("EMPID", "FIRSTNAME", "LASTNAME")

# Sorter transformation: SRTTRANS1 - Sort department data
df_srttrans1 = df_emp_dept.orderBy("DEPTNO", "DNAME", "LOC")

# ============================================================================
# STEP 5: JOINER TRANSFORMATION
# Join employee data with department data
# ============================================================================

# Joiner transformation: JNRTRANS
# Join Condition: DEPTNO = DEPID
# Join Type: Normal Join (inner)
df_jnrtrans = df_srttrans.alias("emp").join(
    df_srttrans1.alias("dept"),
    F.col("emp.DEPID") == F.col("dept.DEPTNO"),
    "inner"
).select(
    F.col("emp.EMPID"),
    F.col("emp.FIRSTNAME"),
    F.col("emp.LASTNAME"),
    F.col("emp.EMAIL"),
    F.col("emp.PHONENO"),
    F.col("emp.SALARY"),
    F.col("emp.DEPID"),
    F.col("dept.DEPTNO"),
    F.col("dept.DNAME"),
    F.col("dept.LOC")
)

# ============================================================================
# STEP 6: MAPPLET - mp_name (Expression transformation)
# Creates concatenated name field
# ============================================================================

df_mp_name = df_jnrtrans.withColumn(
    "NAME",
    F.concat(F.col("FIRSTNAME"), F.lit(" "), F.col("LASTNAME"))
)

# ============================================================================
# STEP 7: EXPRESSION TRANSFORMATION - EXPTRANS
# Includes unconnected lookup call: :LKP.LKPTRANS1(EMPID) for ELOC
# ============================================================================

# Expression transformation with lookup join for ELOC
df_exptrans = df_mp_name.alias("main").join(
    df_lkp_emp_loc_broadcast.alias("lkp"),
    F.col("main.EMPID") == F.col("lkp.LOC_EMPID"),
    "left"
).select(
    F.col("main.EMPID"),
    F.col("main.EMAIL"),
    F.col("main.PHONENO"),
    F.col("main.SALARY"),
    F.col("main.DEPID"),
    F.col("main.DNAME"),
    F.trim(F.col("main.NAME")).alias("ENAME"),
    F.coalesce(F.col("lkp.LOC"), F.lit("")).alias("ELOC")
)

# ============================================================================
# STEP 8: AGGREGATOR TRANSFORMATION - AGGTRANS
# Calculates MAX and MIN salary
# ============================================================================

df_aggtrans = df_exptrans.groupBy(
    "EMPID", "EMAIL", "PHONENO", "SALARY", "DEPID", "DNAME", "ENAME"
).agg(
    F.max("SALARY").alias("MAXSAL"),
    F.min("SALARY").alias("MINSAL")
)

# ============================================================================
# STEP 9: ROUTER TRANSFORMATION - RTRTRANS
# Routes data to different targets based on conditions
# Implemented as multiple filter branches
# ============================================================================

# Router Group 1: Records for T_EMP_DEP10 (DEPID = 10)
df_router_dep10 = df_exptrans.filter(F.col("DEPID") == 10).select(
    F.col("EMPID").alias("EMPID1"),
    F.col("EMAIL").alias("EMAIL1"),
    F.col("PHONENO").alias("PHONENO1"),
    F.col("SALARY").alias("SALARY1"),
    F.col("DEPID").alias("DEPID1"),
    F.col("DNAME").alias("DNAME1"),
    F.col("ENAME").alias("NAME1")
)

# Router Group 2: Default group for T_DEFAULT (all other records)
df_router_default = df_exptrans.filter(F.col("DEPID") != 10).select(
    F.col("EMPID").alias("EMPID2"),
    F.col("EMAIL").alias("EMAIL2"),
    F.col("PHONENO").alias("PHONENO2"),
    F.col("SALARY").alias("SALARY2"),
    F.col("DEPID").alias("DEPID2"),
    F.col("DNAME").alias("DNAME2"),
    F.col("ENAME").alias("NAME2")
)

# ============================================================================
# STEP 10: LOOKUP FOR UPDATE STRATEGY - LKPTRANS
# Check if record exists in target (for insert/update decision)
# ============================================================================

df_with_lookup = df_exptrans.alias("src").join(
    df_lkp_employee_broadcast.alias("lkp"),
    F.col("src.EMPID") == F.col("lkp.T_EMPID"),
    "left"
).select(
    F.col("src.*"),
    F.col("lkp.T_EMPID"),
    F.col("lkp.LKP_EMAIL"),
    F.col("lkp.LKP_PHONENO"),
    F.col("lkp.LKP_SALARY"),
    F.col("lkp.LKP_DEPID")
)

# ============================================================================
# STEP 11: EXPRESSION TRANSFORMATION - EXPTRANS2
# Determines if record is new or existing for update strategy
# ============================================================================

df_exptrans2 = df_with_lookup.withColumn(
    "NEWRECORD_FLAG",
    F.when(F.col("T_EMPID").isNull(), F.lit("TRUE")).otherwise(F.lit("FALSE"))
).withColumn(
    "UPDATERECORD_FLAG",
    F.when(F.col("T_EMPID").isNull(), F.lit("FALSE")).otherwise(F.lit("TRUE"))
)

# ============================================================================
# STEP 12: FILTER TRANSFORMATIONS FOR INSERT/UPDATE
# ============================================================================

# Filter for INSERT: New records in DEPID = 20
df_filtrans_insert = df_exptrans2.filter(
    (F.col("DEPID") == 20) & (F.col("NEWRECORD_FLAG") == "TRUE")
).select(
    F.col("EMPID").alias("T_EMPID"),
    F.col("EMAIL"),
    F.col("PHONENO"),
    F.col("SALARY"),
    F.col("DEPID"),
    F.col("DNAME"),
    F.col("ENAME"),
    F.col("ELOC")
)

# Filter for UPDATE: Existing records in DEPID = 30
df_filtrans_update = df_exptrans2.filter(
    (F.col("DEPID") == 30) & (F.col("UPDATERECORD_FLAG") == "TRUE")
).select(
    F.col("EMPID").alias("T_EMPID"),
    F.col("EMAIL"),
    F.col("PHONENO"),
    F.col("SALARY"),
    F.col("DEPID"),
    F.col("DNAME"),
    F.col("ENAME"),
    F.col("ELOC")
)

# ============================================================================
# STEP 13: UPDATE STRATEGY TRANSFORMATION - UPDTRANS
# Marks records for update (DD_UPDATE = 1)
# In PySpark, we handle this by separating insert and update streams
# ============================================================================

# Add update strategy flag for update records
df_update_strategy = df_filtrans_update.withColumn(
    "UPDATE_STRATEGY",
    F.lit(1)  # DD_UPDATE
)

# ============================================================================
# STEP 14: WRITE TO TARGETS
# ============================================================================

# Write to target: EMP_MAX_MIN (Aggregated data with MAX/MIN salary)
df_aggtrans.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/emp_max_min")

# Write to target: T_EMP_DEP10 (Router output for DEPID = 10)
df_router_dep10.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/t_emp_dep10")

# Write to target: T_DEFAULT (Router default output)
df_router_default.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/t_default")

# Write to target: TARGET_INSERT (New records for insert)
df_filtrans_insert.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/target_insert")

# Write to target: TARGET_UPDATE (Existing records for update)
df_update_strategy.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/target_update")

# ============================================================================
# STEP 15: DATABASE WRITES (Commented - uncomment for actual JDBC writes)
# ============================================================================

# Uncomment below for actual JDBC writes to Oracle:
# df_aggtrans.write \
#     .format("jdbc") \
#     .option("url", "jdbc:oracle:thin:@//host:port/service") \
#     .option("dbtable", "EMP_MAX_MIN") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .mode("overwrite") \
#     .save()

# For Delta Lake update strategy (if using Delta):
# from delta.tables import DeltaTable
# 
# delta_table = DeltaTable.forPath(spark, "path/to/target_table")
# delta_table.alias("target").merge(
#     df_update_strategy.alias("source"),
#     "target.EMPID = source.T_EMPID"
# ).whenMatchedUpdate(set={
#     "EMAIL": "source.EMAIL",
#     "PHONENO": "source.PHONENO",
#     "SALARY": "source.SALARY"
# }).whenNotMatchedInsertAll().execute()

print("ETL completed successfully!")
print("Output files written to: generated/outputs/")

# Stop Spark session
spark.stop()
