from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# Create Spark Session
spark = SparkSession.builder \
    .appName("RealTimeStockBankingStreaming") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .getOrCreate()

# Define the schema for banking transactions
banking_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("transaction_amount", DoubleType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", LongType(), True)  # TIMESTAMP(MILLIS)
])

# Define the schema for stock market data
stock_schema = StructType([
    StructField("stock_symbol", StringType(), True),
    StructField("stock_price", DoubleType(), True),
    StructField("timestamp", LongType(), True)  # TIMESTAMP(MILLIS)
])

# Define the schema for the overall JSON structure
schema = StructType([
    StructField("banking_transaction", banking_schema, True),
    StructField("stock_data", stock_schema, True)
])

# Define the input directory where JSON files are arriving
input_directory = "/home/labuser/Documents/Level3/Day3/StreamingData/"

# Read streaming data from the input directory
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .load(input_directory)

# Flatten the JSON structure
flattened_df = streaming_df.select(
    col("banking_transaction.account_id").alias("account_id"),
    col("banking_transaction.transaction_amount").alias("transaction_amount"),
    col("banking_transaction.transaction_type").alias("transaction_type"),
    col("banking_transaction.location").alias("location"),
    col("banking_transaction.timestamp").alias("transaction_timestamp"),
    col("stock_data.stock_symbol").alias("stock_symbol"),
    col("stock_data.stock_price").alias("stock_price"),
    col("stock_data.timestamp").alias("stock_timestamp")
)

# Convert timestamp from TIMESTAMP(MILLIS) to readable format
processed_df = flattened_df.withColumn("transaction_time", expr("timestamp_millis(transaction_timestamp)")) \
                           .withColumn("stock_time", expr("timestamp_millis(stock_timestamp)"))

# ✅ Try to write stream to Jupyter Notebook if supported, else fallback to console
try:
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("streaming_table") \
        .start()

    print("✅ Streaming started. Run `spark.sql('SELECT * FROM streaming_table').show(truncate=False)` to see real-time data in Jupyter Notebook.")

except Exception as e:
    print("⚠️ Jupyter Notebook output not supported, switching to console.")
    query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

query.awaitTermination()
