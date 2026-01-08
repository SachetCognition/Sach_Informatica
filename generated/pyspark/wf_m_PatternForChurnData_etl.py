"""
PySpark ETL: wf_m_PatternForChurnData
Migrated from Informatica PowerCenter XML

Original Mapping: m_PatternForChurnData
Sources: Gender (Flat File), Churndata (Flat File)
Targets: CHURNRATIO, CHURNDIM, ChurnDT (all Flat Files)

Transformations:
- SQ_Churndata: Source Qualifier for Churndata
- SQ_Gender: Source Qualifier for Gender
- EXPTRANS: Expression transformation with IIF logic for contract-based charges
- JNRTRANS1: Joiner to combine churn data with gender lookup
- SRTTRANS: Sorter for ordering data
- EXPTRANS1: Expression transformation for additional calculations
- AGGTRANS: Aggregator for churn ratio calculations
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("wf_m_PatternForChurnData_ETL") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_PatternForChurnData
################################################################################

# ============================================================================
# STEP 1: READ SOURCES
# ============================================================================

# Read source: Gender (Flat File)
df_gender = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/gender.csv")

# Read source: Churndata (Flat File)
df_churndata = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/churndata.csv")

# ============================================================================
# STEP 2: TRANSFORMATIONS
# ============================================================================

# Expression transformation: EXPTRANS
# Implements IIF(Contract='One Year',23,12) logic and calculates TotalCharges
df_exptrans = df_churndata \
    .withColumn("ContVar1", 
        F.when(F.col("Contract") == "One year", 23).otherwise(12)) \
    .withColumn("TotChargesVar", 
        F.col("ContVar1") + F.col("MonthlyCharges")) \
    .withColumn("TotalCharges_Calc", F.col("TotChargesVar"))

# Joiner transformation: JNRTRANS1
# Join churn data with gender lookup table on gender field
# Note: gender column in churndata is lowercase, Gender in gender.csv is uppercase
df_gender_renamed = df_gender.withColumnRenamed("Gender", "Gender1")
df_jnrtrans1 = df_exptrans.join(
    df_gender_renamed,
    F.upper(df_exptrans["gender"]) == F.upper(df_gender_renamed["Gender1"]),
    "inner"
).select(
    df_exptrans["gender"],
    df_exptrans["SeniorCitizen"],
    df_exptrans["TotalCharges_Calc"].alias("TotalCharges"),
    df_gender_renamed["Type"]
)

# Sorter transformation: SRTTRANS
# Sort by gender, SeniorCitizen, Partner
df_srttrans = df_exptrans.orderBy("gender", "SeniorCitizen", "Partner")

# Expression transformation: EXPTRANS1
# Apply UPPER transformation to Partner field
df_exptrans1 = df_srttrans \
    .withColumn("Partner", F.upper(F.col("Partner")))

# Aggregator transformation: AGGTRANS (for CHURNRATIO target)
# Group by gender, Contract, InternetService, PaymentMethod, Churn
# Calculate aggregated TotalCharges and MonthlyCharges
df_aggtrans = df_exptrans \
    .groupBy("gender", "Contract", "InternetService", "PaymentMethod", "Churn") \
    .agg(
        F.sum("TotalCharges_Calc").alias("TotalCharges"),
        F.sum("MonthlyCharges").alias("MonthlyCharges")
    )

# ============================================================================
# STEP 3: WRITE TO TARGETS
# ============================================================================

# Write to target: CHURNRATIO (Flat File)
# Contains aggregated churn ratio data
df_aggtrans.select(
    "gender", "Contract", "InternetService", "PaymentMethod", 
    "Churn", "TotalCharges", "MonthlyCharges"
).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/churnratio")

# Write to target: CHURNDIM (Flat File)
# Contains dimension data with transformed fields
df_exptrans1.select(
    "gender", "SeniorCitizen", "Partner", "Dependents", "tenure",
    "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity",
    "OnlineBackup", "DeviceProtection", "Contract", "PaperlessBilling",
    "PaymentMethod", "MonthlyCharges", "TotalCharges_Calc", "Churn"
).withColumnRenamed("TotalCharges_Calc", "TotalCharges") \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/churndim")

# Write to target: ChurnDT (Flat File)
# Contains joined data with gender type
df_jnrtrans1.select(
    "gender", "SeniorCitizen", "TotalCharges"
).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/churndt")

print("ETL completed successfully!")
print(f"Records processed from Churndata: {df_churndata.count()}")
print(f"Records processed from Gender: {df_gender.count()}")
print(f"Records written to CHURNRATIO: {df_aggtrans.count()}")
print(f"Records written to CHURNDIM: {df_exptrans1.count()}")
print(f"Records written to ChurnDT: {df_jnrtrans1.count()}")

# Stop Spark session
spark.stop()
