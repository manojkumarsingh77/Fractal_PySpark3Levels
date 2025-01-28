# PySpark Execution Plan
from pyspark.sql import SparkSession

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("PySpark Execution Plan Demo") \
        .getOrCreate()

    # Sample data
    data = [
        (1, "Alice", 50),
        (2, "Bob", 40),
        (3, "Cathy", 60),
        (4, "David", 70),
        (5, "Eva", 30),
    ]

    # Create a DataFrame
    df = spark.createDataFrame(data, ["ID", "Name", "Score"])

    # Transformation: Filter and select operations
    filtered_df = df.filter(df["Score"] > 40).select("Name", "Score")

    # Show the DataFrame
    print("Filtered DataFrame:")
    filtered_df.show()

    # Explain the execution plan
    print("\nComplete Execution Plan:")
    filtered_df.explain(mode="extended")  # Shows detailed logical and physical plans

    # Explain specific plans
    print("\nUnoptimized Logical Plan:")
    filtered_df.explain(mode="simple")  # Simple logical plan

    print("\nOptimized Logical Plan:")
    filtered_df.explain(mode="codegen")  # Optimized plan with code generation

    print("\nPhysical Plan:")
    filtered_df.explain(mode="cost")  # Physical plan with cost model information

    # Stop SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
