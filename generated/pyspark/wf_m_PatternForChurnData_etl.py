"""
PySpark ETL: wf_m_PatternForChurnData
Migrated from Informatica PowerCenter XML

Original Mapping: m_PatternForChurnData
Sources: Gender (Flat File), Churndata (Flat File)
Targets: CHURNRATIO, CHURNDIM, ChurnDT (all Flat Files)

Transformations (exact replication of Informatica logic):
- SQ_Churndata: Source Qualifier for Churndata
- SQ_Gender: Source Qualifier for Gender
- EXPTRANS: Expression transformation with IIF logic for contract-based charges
- JNRTRANS1: Joiner to combine churn data with gender lookup
- SRTTRANS: Sorter for ordering data
- EXPTRANS1: Expression transformation (pass-through with Partner1)
- AGGTRANS: Aggregator for churn ratio calculations
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

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
# STEP 1: READ SOURCES (exact column names from Informatica XML)
# ============================================================================

# Read source: Gender (Flat File)
# Columns: Gender, Type
df_gender = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/gender.csv")

# Read source: Churndata (Flat File)
# Columns: customerID, gender, SeniorCitizen, Partner, Dependents, tenure, PhoneService, 
#          MultipleLines, InternetService, OnlineSecurity, OnlineBackup, DeviceProtection,
#          TechSupport, StreamingTV, StreamingMovies, Contract, PaperlessBilling, 
#          PaymentMethod, MonthlyCharges, TotalCharges, Churn
df_churndata = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .csv("generated/sample_data/churndata.csv")

# ============================================================================
# STEP 2: TRANSFORMATIONS (exact replication of Informatica logic)
# ============================================================================

# Expression transformation: EXPTRANS
# ContVar1 = IIF(Contract='One Year',23,12)
# TotChargesVar = ContVar1 + MonthlyCharges
# TotalCharges = TotChargesVar (output)
df_exptrans = df_churndata \
    .withColumn("ContVar1", 
        F.when(F.col("Contract") == "One year", 23).otherwise(12)) \
    .withColumn("TotChargesVar", 
        F.col("ContVar1") + F.col("MonthlyCharges")) \
    .withColumn("TotalCharges", F.col("TotChargesVar"))

# Joiner transformation: JNRTRANS1
# Join condition: Gender1 = gender (case-sensitive in Informatica)
# Output: gender, SeniorCitizen, TotalCharges, Type, Gender1
df_gender_renamed = df_gender.withColumnRenamed("Gender", "Gender1")
df_jnrtrans1 = df_exptrans.join(
    df_gender_renamed,
    F.upper(df_exptrans["gender"]) == F.upper(df_gender_renamed["Gender1"]),
    "inner"
).select(
    df_exptrans["gender"],
    df_exptrans["SeniorCitizen"],
    df_exptrans["TotalCharges"],
    df_gender_renamed["Type"],
    df_gender_renamed["Gender1"]
)

# Sorter transformation: SRTTRANS
# Sort by gender (DESC), Contract (ASC)
df_srttrans = df_exptrans.orderBy(F.desc("gender"), F.asc("Contract"))

# Expression transformation: EXPTRANS1
# Partner1 = Partner (pass-through, used for CHURNDIM target)
# Partner = UPPER(Partner1) (output, but not used in target mapping)
df_exptrans1 = df_srttrans \
    .withColumn("Partner1", F.col("Partner"))

# Aggregator transformation: AGGTRANS (for CHURNRATIO target)
# Group by: gender, InternetService, Contract, PaymentMethod, Churn
# Pass-through: TotalCharges, MonthlyCharges (first value in each group)
# Note: Informatica aggregator with non-aggregate ports returns first value in group
df_aggtrans = df_exptrans \
    .groupBy("gender", "Contract", "InternetService", "PaymentMethod", "Churn") \
    .agg(
        F.first("TotalCharges").alias("TotalCharges"),
        F.first("MonthlyCharges").alias("MonthlyCharges")
    )

# ============================================================================
# STEP 3: WRITE TO TARGETS (exact column names from Informatica XML)
# ============================================================================

# Write to target: CHURNRATIO (Flat File)
# Columns: gender, Contract, InternetService, PaymentMethod, Churn, TotalCharges, MonthlyCharges
# Source: AGGTRANS
df_churnratio = df_aggtrans.select(
    "gender", "Contract", "InternetService", "PaymentMethod", 
    "Churn", "TotalCharges", "MonthlyCharges"
)
df_churnratio.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/CHURNRATIO")

# Write to target: CHURNDIM (Flat File)
# Columns: gender, SeniorCitizen, Partner, Dependents, tenure, PhoneService, MultipleLines,
#          InternetService, OnlineSecurity, OnlineBackup, DeviceProtection, Contract,
#          PaperlessBilling, PaymentMethod, MonthlyCharges, TotalCharges, Churn
# Source: EXPTRANS1 (Partner comes from Partner1)
df_churndim = df_exptrans1.select(
    F.col("gender"),
    F.col("SeniorCitizen"),
    F.col("Partner1").alias("Partner"),
    F.col("Dependents"),
    F.col("tenure"),
    F.col("PhoneService"),
    F.col("MultipleLines"),
    F.col("InternetService"),
    F.col("OnlineSecurity"),
    F.col("OnlineBackup"),
    F.col("DeviceProtection"),
    F.col("Contract"),
    F.col("PaperlessBilling"),
    F.col("PaymentMethod"),
    F.col("MonthlyCharges"),
    F.col("TotalCharges"),
    F.col("Churn")
)
df_churndim.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/CHURNDIM")

# Write to target: ChurnDT (Flat File)
# Columns: gender, SeniorCitizen, TotalCharges
# Source: JNRTRANS1 (gender comes from Type!)
df_churndt = df_jnrtrans1.select(
    F.col("Type").alias("gender"),
    F.col("SeniorCitizen"),
    F.col("TotalCharges")
)
df_churndt.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("generated/outputs/ChurnDT")

print("ETL completed successfully!")
print(f"Records processed from Churndata: {df_churndata.count()}")
print(f"Records processed from Gender: {df_gender.count()}")
print(f"Records written to CHURNRATIO: {df_churnratio.count()}")
print(f"Records written to CHURNDIM: {df_churndim.count()}")
print(f"Records written to ChurnDT: {df_churndt.count()}")

# Stop Spark session
spark.stop()
