from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, sum, avg, count

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("Investment Banking - Portfolio Analysis") \
    .getOrCreate()

# Step 2: Load Client Portfolio Data (Parquet Format)
df = spark.read.parquet("/home/labuser/Downloads/client_portfolio.parquet")

# Step 3: Explore the Data
print("Schema of the DataFrame:")
df.printSchema()

print("Sample rows from the DataFrame:")
df.show(10)

# Step 4: Add a New Column - Portfolio Risk Index
# Formula: Portfolio Risk Index = AssetsUnderManagement * RiskFactor
df = df.withColumn("PortfolioRiskIndex", col("AssetsUnderManagement") * col("RiskFactor"))

# Step 5: Rename Columns for Better Readability
df = df.withColumnRenamed("ClientID", "Client_ID").withColumnRenamed("AssetsUnderManagement", "AUM")

# Step 6: Group Data by Risk Profile and Perform Aggregations
agg_df = df.groupBy("RiskProfile").agg(
    sum("AUM").alias("TotalInvestments"),
    avg("AUM").alias("AveragePortfolioValue"),
    count("Client_ID").alias("NumberOfClients")
)

# Step 7: Show Results
print("Aggregated Results by Risk Profile:")
agg_df.show()

# Step 8: Save Results to Parquet
output_path = "aggregated_portfolio_analysis"
agg_df.write.parquet(output_path, mode="overwrite")

# Stop SparkSession
spark.stop()
