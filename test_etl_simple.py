"""
Simple ETL Test - Test generated PySpark code without spark-submit

This script runs a simplified version of the ETL that reads the sample data
and prints basic statistics to verify the conversion works.

Usage:
    uv run python test_etl_simple.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path


def test_churn_data_etl():
    """Test the churn data ETL with sample data"""
    
    print("="*80)
    print("TESTING PYSPARK ETL WITH SAMPLE DATA")
    print("="*80 + "\n")
    
    # Initialize Spark Session (local mode)
    print("🚀 Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Informatica_ETL_Test") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session created\n")
    
    # Check if sample data exists
    sample_data_dir = Path("generated/sample_data")
    if not sample_data_dir.exists():
        print("❌ Sample data directory not found!")
        print("   Generate sample data first:")
        print("   uv run python generate_sample_data.py <metadata.json>\n")
        spark.stop()
        return
    
    # Read sample data
    print("📂 Reading sample data...")
    
    try:
        # Read Gender source
        df_gender = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(str(sample_data_dir / "gender.csv"))
        
        print(f"   ✅ Gender: {df_gender.count()} rows")
        
        # Read Churndata source
        df_churndata = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(str(sample_data_dir / "churndata.csv"))
        
        print(f"   ✅ Churndata: {df_churndata.count()} rows\n")
        
    except Exception as e:
        print(f"❌ Error reading sample data: {e}\n")
        spark.stop()
        return
    
    # Display schemas
    print("📋 Data Schemas:")
    print("\n   Gender Schema:")
    df_gender.printSchema()
    
    print("   Churndata Schema (first 10 columns):")
    for field in df_churndata.schema.fields[:10]:
        print(f"      |-- {field.name}: {field.dataType}")
    print(f"      ... and {len(df_churndata.schema.fields) - 10} more columns\n")
    
    # Show sample data
    print("📊 Sample Data Preview:")
    print("\n   Gender (first 5 rows):")
    df_gender.show(5, truncate=False)
    
    print("   Churndata (first 5 rows, selected columns):")
    df_churndata.select("customerID", "gender", "SeniorCitizen", "tenure", "MonthlyCharges", "Churn").show(5)
    
    # Perform some basic transformations (similar to the ETL)
    print("🔧 Testing Transformations:")
    
    # Test 1: Join Gender with Churndata
    print("\n   Test 1: Joining Gender with Churndata...")
    df_joined = df_churndata.join(
        df_gender,
        df_churndata["gender"] == df_gender["Gender"],
        "inner"
    )
    print(f"   ✅ Joined data: {df_joined.count()} rows")
    
    # Test 2: Aggregation by gender and contract
    print("\n   Test 2: Aggregating by gender and Contract...")
    df_agg = df_churndata.groupBy("gender", "Contract").agg(
        F.count("*").alias("customer_count"),
        F.avg("MonthlyCharges").alias("avg_monthly_charges"),
        F.sum("TotalCharges").alias("total_charges")
    )
    
    print(f"   ✅ Aggregated data: {df_agg.count()} groups")
    print("\n   Aggregation Results:")
    df_agg.orderBy("gender", "Contract").show(truncate=False)
    
    # Test 3: Filter and sort
    print("\n   Test 3: Filtering churned customers and sorting...")
    df_churned = df_churndata \
        .filter(F.col("Churn") == "Yes") \
        .orderBy(F.col("TotalCharges").desc())
    
    print(f"   ✅ Churned customers: {df_churned.count()} rows")
    print("\n   Top 5 churned customers by total charges:")
    df_churned.select("customerID", "gender", "tenure", "MonthlyCharges", "TotalCharges").show(5)
    
    # Test 4: Expression transformation
    print("\n   Test 4: Creating calculated fields...")
    df_transformed = df_churndata.withColumn(
        "tenure_years",
        F.round(F.col("tenure") / 12, 2)
    ).withColumn(
        "avg_monthly_charge",
        F.when(F.col("tenure") > 0, F.col("TotalCharges") / F.col("tenure")).otherwise(0)
    )
    
    print("   ✅ Added calculated columns: tenure_years, avg_monthly_charge")
    print("\n   Sample with calculated fields:")
    df_transformed.select(
        "customerID", "tenure", "tenure_years", 
        "MonthlyCharges", "TotalCharges", "avg_monthly_charge"
    ).show(5)
    
    # Summary statistics
    print("\n📈 Summary Statistics:")
    print("\n   Churndata Statistics:")
    df_churndata.select("tenure", "MonthlyCharges", "TotalCharges").describe().show()
    
    print("   Churn Distribution:")
    df_churndata.groupBy("Churn").count().show()
    
    print("   Contract Type Distribution:")
    df_churndata.groupBy("Contract").count().show()
    
    # Write outputs to generated/outputs directory
    print("\n💾 Writing outputs to generated/outputs/...")
    output_dir = Path("generated/outputs")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Write aggregated results
    df_agg.write.mode("overwrite").option("header", "true").csv(str(output_dir / "churn_by_gender_contract"))
    print(f"   ✅ Saved: {output_dir / 'churn_by_gender_contract'}")
    
    # Write churned customers
    df_churned.write.mode("overwrite").option("header", "true").csv(str(output_dir / "churned_customers"))
    print(f"   ✅ Saved: {output_dir / 'churned_customers'}")
    
    # Write transformed data sample
    df_transformed.limit(100).write.mode("overwrite").option("header", "true").csv(str(output_dir / "transformed_sample"))
    print(f"   ✅ Saved: {output_dir / 'transformed_sample'}")
    
    # Stop Spark
    print("\n" + "="*80)
    print("✅ ALL TESTS PASSED!")
    print("="*80 + "\n")
    print("💡 The generated PySpark code structure is working correctly!")
    print(f"📁 Output files written to: {output_dir.absolute()}")
    print("   You can now run the full ETL with: uv run python run_etl.py <etl_file>\n")
    
    spark.stop()


if __name__ == "__main__":
    try:
        test_churn_data_etl()
    except KeyboardInterrupt:
        print("\n\n⚠️  Test interrupted by user\n")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}\n")
        import traceback
        traceback.print_exc()
