from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, max, window
import random
import datetime

def generate_sales_data(num_records):
    """Generate a simulated sales dataset for Walmart."""
    stores = [f"Store_{i}" for i in range(1, 101)]  # 100 stores
    skus = [f"SKU_{i}" for i in range(1, 1001)]  # 1000 SKUs
    data = []

    for _ in range(num_records):
        store = random.choice(stores)
        sku = random.choice(skus)
        sales = random.randint(0, 100)
        date = datetime.date.today() - datetime.timedelta(days=random.randint(0, 365))
        data.append((store, sku, sales, date))
    return data

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Walmart Inventory Optimization") \
        .getOrCreate()

    # Generate a large dataset
    num_records = 1_000_000  # 1 million records
    data = generate_sales_data(num_records)

    # Create a DataFrame
    sales_df = spark.createDataFrame(data, ["Store", "SKU", "Sales", "Date"])

    # Show initial dataset
    print("Initial Dataset:")
    sales_df.show(5)

    # Transformation: Aggregate sales data by Store, SKU, and Day
    aggregated_df = sales_df.groupBy("Store", "SKU", "Date").agg(
        sum("Sales").alias("Total_Sales"),
        avg("Sales").alias("Avg_Sales")
    )
    print("Aggregated Sales Data:")
    aggregated_df.show(5)

    # Trend Analysis: Find the highest sales day per Store-SKU combination
    trend_df = aggregated_df.groupBy("Store", "SKU").agg(
        max("Total_Sales").alias("Max_Sales")
    )
    print("Trend Analysis Data:")
    trend_df.show(5)

    # Real-Time Demand Forecasting: Simulating a moving average over a window of days
    forecast_df = sales_df.groupBy("Store", "SKU").agg(
        avg("Sales").alias("Forecasted_Demand")
    )
    print("Forecasted Demand:")
    forecast_df.show(5)

    # Warehouse Optimization: Calculate total sales per store
    warehouse_df = sales_df.groupBy("Store").agg(
        sum("Sales").alias("Total_Store_Sales")
    )
    print("Warehouse Optimization Data:")
    warehouse_df.show(5)

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
