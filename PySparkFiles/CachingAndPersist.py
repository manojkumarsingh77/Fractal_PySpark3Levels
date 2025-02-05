from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.storagelevel import StorageLevel
import time

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Caching_vs_Persistence_Performance") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Load dataset
file_path = "clinical_trial_data.csv"  # Adjust path if needed
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Filter patients based on conditions
filtered_df = df.filter((col("age") > 50) & (col("drug_administered") == "DrugX"))

### **1️⃣ Execution Without Caching or Persistence**
start_time = time.time()
filtered_df.count()  # Trigger action
end_time = time.time()
time_without_cache = end_time - start_time
print(f"Execution Time Without Caching/Persistence: {time_without_cache:.4f} seconds")

### **2️⃣ Execution With Caching**
filtered_df.cache()  # Cache the DataFrame
start_time = time.time()
filtered_df.count()  # Trigger action (first access after caching)
end_time = time.time()
time_with_cache = end_time - start_time
print(f"Execution Time With Caching: {time_with_cache:.4f} seconds")

# Remove cache
filtered_df.unpersist()

### **3️⃣ Execution With Persistence (MEMORY_AND_DISK)**
filtered_df.persist(StorageLevel.MEMORY_AND_DISK)  # Persist the DataFrame
start_time = time.time()
filtered_df.count()  # Trigger action (first access after persistence)
end_time = time.time()
time_with_persist = end_time - start_time
print(f"Execution Time With Persistence (MEMORY_AND_DISK): {time_with_persist:.4f} seconds")

# Remove persistence
filtered_df.unpersist()

# Stop Spark Session
spark.stop()
