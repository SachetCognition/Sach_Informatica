"""
PySpark ETL Script - Migrated from Informatica PowerCenter
Source: wf_s_m_multiple_mapplet.XML
Mapping: m_multiple_mapplet

This ETL includes:
- Multiple chained mapplets (mp_name, mp_name1)
- Expression transformations for name concatenation
- Sorter transformation
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Informatica_to_PySpark_m_multiple_mapplet") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.broadcastTimeout", "600") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_multiple_mapplet
# Description: ETL with multiple chained mapplets for name processing
################################################################################

# ============================================================================
# STEP 1: READ SOURCES
# ============================================================================

# Read source: EMPLOYEEDETAILS (Database: Oracle)
# TODO: Update connection string with actual database credentials
df_employeedetails = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:port/service") \
    .option("dbtable", "EMPLOYEEDETAILS") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

# ============================================================================
# STEP 2: SOURCE QUALIFIER
# ============================================================================

# Source Qualifier: SQ_EMPLOYEEDETAILS
df_sq_employeedetails = df_employeedetails.select(
    F.col("EMPID"),
    F.col("FIRSTNAME"),
    F.col("LASTNAME"),
    F.col("EMAIL"),
    F.col("PHONENO"),
    F.col("SALARY"),
    F.col("DEPID")
)

# ============================================================================
# STEP 3: MAPPLET - mp_name (First name processing mapplet)
# Creates concatenated name field
# ============================================================================

# Expression transformation within mp_name mapplet
# Concatenates FIRSTNAME and LASTNAME to create NAME
df_mp_name = df_sq_employeedetails.withColumn(
    "NAME",
    F.concat(F.col("FIRSTNAME"), F.lit(" "), F.col("LASTNAME"))
)

# ============================================================================
# STEP 4: MAPPLET - mp_name1 (Second name processing mapplet)
# Additional name processing - creates trimmed and uppercase name
# ============================================================================

# Expression transformation within mp_name1 mapplet
# Trims and uppercases the NAME field
df_mp_name1 = df_mp_name.withColumn(
    "NAME_UPPER",
    F.upper(F.trim(F.col("NAME")))
).withColumn(
    "NAME_TRIMMED",
    F.trim(F.col("NAME"))
)

# ============================================================================
# STEP 5: SORTER TRANSFORMATION - SRTTRANS
# Sort by EMPID
# ============================================================================

df_srttrans = df_mp_name1.orderBy("EMPID")

# ============================================================================
# STEP 6: EXPRESSION TRANSFORMATION - EXPTRANS
# Final expression transformation before target
# ============================================================================

df_exptrans = df_srttrans.select(
    F.col("EMPID"),
    F.col("FIRSTNAME"),
    F.col("LASTNAME"),
    F.col("EMAIL"),
    F.col("PHONENO"),
    F.col("SALARY"),
    F.col("DEPID"),
    F.col("NAME"),
    F.col("NAME_UPPER"),
    F.col("NAME_TRIMMED")
)

# ============================================================================
# STEP 7: WRITE TO TARGETS
# ============================================================================

# Write to target: T_EMPLOYEEDETAILS (Database or Flat File)
df_exptrans.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/t_employeedetails_mapplet")

# Uncomment below for actual JDBC write:
# df_exptrans.write \
#     .format("jdbc") \
#     .option("url", "jdbc:oracle:thin:@//host:port/service") \
#     .option("dbtable", "T_EMPLOYEEDETAILS") \
#     .option("user", "username") \
#     .option("password", "password") \
#     .mode("overwrite") \
#     .save()

print("ETL completed successfully!")
print("Output files written to: generated/outputs/t_employeedetails_mapplet")

# Stop Spark session
spark.stop()
