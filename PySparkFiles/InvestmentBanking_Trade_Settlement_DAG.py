from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

# Step 1: Initialize SparkSession and SparkContext
spark = SparkSession.builder \
    .appName("Trade Settlement Analysis") \
    .getOrCreate()

# Access SparkContext from SparkSession
sc = spark.sparkContext

# Step 2: Load Trade Settlement Data
# The data is in a Parquet file format
df = spark.read.parquet("/home/labuser/Downloads/trade_settlement.parquet")

# Step 3: Explore the Data
print("Schema of the DataFrame:")
df.printSchema()

print("Sample rows from the DataFrame:")
df.show(10)

# Step 4: Identify Delayed Settlements
# Delayed Settlement: Settlement time greater than 2 days (T+2)
delayed_trades = df.filter(col("SettlementDays") > 2)

# Step 5: Calculate Average Settlement Time by Financial Instrument
avg_settlement_time = df.groupBy("Instrument").agg(
    avg("SettlementDays").alias("AverageSettlementTime")
)

# Step 6: Calculate Trade Volume by Instrument
trade_volume = df.groupBy("Instrument").agg(
    count("TradeID").alias("TotalTrades")
)

# Step 7: Perform Actions to Trigger Execution
print("Count of delayed settlements:")
print(delayed_trades.count())  # Action: Count

print("Average settlement time by instrument:")
avg_settlement_time.show()  # Action: Show

print("Trade volume by instrument:")
trade_volume.show()  # Action: Show

# Step 8: Explain DAG
print("DAG for the delayed settlements query:")
delayed_trades.explain(extended=True)

# Step 9: Stop SparkSession
spark.stop()
