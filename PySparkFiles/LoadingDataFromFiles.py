from pyspark.sql import SparkSession

def load_csv_example():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Load CSV Example") \
        .getOrCreate()

    # Load CSV data
    df = spark.read.csv("/content/client_portfolio.csv", header=True, inferSchema=True)

    # Show schema
    print("Schema of the DataFrame:")
    df.printSchema()

    # Show first 5 rows
    print("First 5 rows:")
    df.show(5)

    # Basic exploration
    print("Column names:", df.columns)
    print("Row count:", df.count())

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    load_csv_example()
-------

def load_json_example():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Load JSON Example") \
        .getOrCreate()

    # Load JSON data
    df = spark.read.json("transactions.json")

    # Show schema
    print("Schema of the DataFrame:")
    df.printSchema()

    # Show first 5 rows
    print("First 5 rows:")
    df.show(5)

    # Basic exploration
    print("Column names:", df.columns)
    print("Row count:", df.count())

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    load_json_example()
------


def load_parquet_example():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Load Parquet Example") \
        .getOrCreate()

    # Load Parquet data
    df = spark.read.parquet("stock_prices.parquet")

    # Show schema
    print("Schema of the DataFrame:")
    df.printSchema()

    # Show first 5 rows
    print("First 5 rows:")
    df.show(5)

    # Basic exploration
    print("Column names:", df.columns)
    print("Row count:", df.count())

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    load_parquet_example()
