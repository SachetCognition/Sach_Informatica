"""
PySpark ETL Script - Migrated from Informatica PowerCenter
Source: wf_m_PatternForChurnData.xml
Mapping: m_PatternForChurnData

This ETL includes:
- Customer churn analysis
- Joiner transformation (Gender with Churndata)
- Aggregator for churn ratio calculations
- Multiple target outputs (CHURNDIM, ChurnDT, CHURNRATIO)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Informatica_to_PySpark_m_PatternForChurnData") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.broadcastTimeout", "600") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

################################################################################
# MAPPING: m_PatternForChurnData
# Description: Customer churn analysis ETL
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
# STEP 2: SOURCE QUALIFIERS
# ============================================================================

# Source Qualifier: SQ_Gender
df_sq_gender = df_gender.select(
    F.col("Gender").alias("Gender1"),
    F.col("Gender")
)

# Source Qualifier: SQ_Churndata
df_sq_churndata = df_churndata.select(
    F.col("customerID"),
    F.col("gender"),
    F.col("SeniorCitizen"),
    F.col("Partner"),
    F.col("Dependents"),
    F.col("tenure"),
    F.col("PhoneService"),
    F.col("MultipleLines"),
    F.col("InternetService"),
    F.col("OnlineSecurity"),
    F.col("OnlineBackup"),
    F.col("DeviceProtection"),
    F.col("TechSupport"),
    F.col("StreamingTV"),
    F.col("StreamingMovies"),
    F.col("Contract"),
    F.col("PaperlessBilling"),
    F.col("PaymentMethod"),
    F.col("MonthlyCharges"),
    F.col("TotalCharges"),
    F.col("Churn")
)

# ============================================================================
# STEP 3: EXPRESSION TRANSFORMATION - EXPTRANS
# Convert TotalCharges to numeric and handle nulls
# ============================================================================

df_exptrans = df_sq_churndata.withColumn(
    "TotalCharges",
    F.when(
        F.col("TotalCharges").cast("double").isNull(),
        F.lit(0.0)
    ).otherwise(F.col("TotalCharges").cast("double"))
)

# ============================================================================
# STEP 4: JOINER TRANSFORMATION - JNRTRANS1
# Join Churndata with Gender lookup
# ============================================================================

# Broadcast the gender lookup for efficient join
df_gender_broadcast = F.broadcast(df_sq_gender)

df_jnrtrans1 = df_exptrans.alias("churn").join(
    df_gender_broadcast.alias("gen"),
    F.col("churn.gender") == F.col("gen.Gender"),
    "inner"
).select(
    F.col("churn.customerID"),
    F.col("churn.gender"),
    F.col("gen.Gender1"),
    F.col("churn.SeniorCitizen"),
    F.col("churn.Partner"),
    F.col("churn.Dependents"),
    F.col("churn.tenure"),
    F.col("churn.PhoneService"),
    F.col("churn.MultipleLines"),
    F.col("churn.InternetService"),
    F.col("churn.OnlineSecurity"),
    F.col("churn.OnlineBackup"),
    F.col("churn.DeviceProtection"),
    F.col("churn.TechSupport"),
    F.col("churn.StreamingTV"),
    F.col("churn.StreamingMovies"),
    F.col("churn.Contract"),
    F.col("churn.PaperlessBilling"),
    F.col("churn.PaymentMethod"),
    F.col("churn.MonthlyCharges"),
    F.col("churn.TotalCharges"),
    F.col("churn.Churn")
)

# ============================================================================
# STEP 5: SORTER TRANSFORMATION - SRTTRANS
# Sort by gender, SeniorCitizen, Partner
# ============================================================================

df_srttrans = df_jnrtrans1.orderBy("gender", "SeniorCitizen", "Partner")

# ============================================================================
# STEP 6: EXPRESSION TRANSFORMATION - EXPTRANS1
# Additional transformations for output
# ============================================================================

df_exptrans1 = df_srttrans.withColumn(
    "Partner",
    F.upper(F.col("Partner"))
).withColumn(
    "Dependents",
    F.upper(F.col("Dependents"))
)

# ============================================================================
# STEP 7: AGGREGATOR TRANSFORMATION - AGGTRANS
# Calculate churn ratio by gender and contract type
# ============================================================================

df_aggtrans = df_exptrans1.groupBy(
    "gender",
    "Contract"
).agg(
    F.count("*").alias("TotalCustomers"),
    F.sum(F.when(F.col("Churn") == "Yes", 1).otherwise(0)).alias("ChurnedCustomers"),
    F.avg("MonthlyCharges").alias("AvgMonthlyCharges"),
    F.sum("TotalCharges").alias("TotalRevenue")
).withColumn(
    "ChurnRatio",
    F.round(F.col("ChurnedCustomers") / F.col("TotalCustomers") * 100, 2)
)

# ============================================================================
# STEP 8: WRITE TO TARGETS
# ============================================================================

# Write to target: CHURNDIM (Flat File) - Dimension data
df_churndim = df_exptrans1.select(
    "customerID",
    "gender",
    "SeniorCitizen",
    "Partner",
    "Dependents",
    "tenure",
    "PhoneService",
    "MultipleLines",
    "InternetService",
    "OnlineSecurity",
    "OnlineBackup",
    "DeviceProtection",
    "TechSupport",
    "StreamingTV",
    "StreamingMovies",
    "Contract",
    "PaperlessBilling"
)

df_churndim.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/churndim")

# Write to target: ChurnDT (Flat File) - Churn details
df_churndt = df_exptrans1.select(
    "customerID",
    "gender",
    "Churn"
)

df_churndt.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/churndt")

# Write to target: CHURNRATIO (Flat File) - Aggregated churn ratio
df_aggtrans.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/churnratio")

print("ETL completed successfully!")
print("Output files written to: generated/outputs/")
print("  - churndim: Customer dimension data")
print("  - churndt: Churn details")
print("  - churnratio: Aggregated churn ratio by gender and contract")

# Stop Spark session
spark.stop()
