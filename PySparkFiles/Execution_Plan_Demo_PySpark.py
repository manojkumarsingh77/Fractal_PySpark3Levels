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






from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("Insurance Example") \
    .getOrCreate()

# Step 2: Create Policies DataFrame
data1 = [("1", "Health Insurance", "20000"),
         ("2", "Car Insurance", "100000"),
         ("3", "Life Insurance", "3000")]

policies = spark.createDataFrame(data1, ["policy_id", "policy_type", "premium_amount"])

# Create temporary view
policies.createOrReplaceTempView("policies")

# Step 3: Create Policyholders DataFrame
data2 = [("1", "John Doe"), ("1", "Jane Doe"),
         ("2", "John Doe"), ("3", "Mary Smith")]

policyholders = spark.createDataFrame(data2, ["policy_id", "policyholderName"])

# Create temporary view
policyholders.createOrReplaceTempView("policyholders")

# Step 4: Perform Join and Aggregation
result_df = spark.sql("""
    SELECT policyholders.policyholderName, 
           SUM(CAST(policies.premium_amount AS INT)) AS total_premium
    FROM policyholders
    INNER JOIN policies
    ON policyholders.policy_id = policies.policy_id
    WHERE policyholders.policyholderName = 'John Doe'
    GROUP BY policyholders.policyholderName
""")

# Step 5: Show Results
result_df.show()

# Stop SparkSession
spark.stop()


if __name__ == "__main__":
    main()
result_df.explain(extended=True)
