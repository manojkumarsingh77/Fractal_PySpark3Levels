from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

# Initialize SparkSession
spark = SparkSession.builder.appName("SupplyChainManagement").getOrCreate()

# Load Inventory Data
inventory_path = "/home/labuser/Downloads/supply_chain_data/inventory.parquet"
df_inventory = spark.read.parquet(inventory_path)

# Load Order Data
orders_path = "/home/labuser/Downloads/supply_chain_data/orders.parquet"
df_orders = spark.read.parquet(orders_path)

# -------------------------------
# Handling Missing Data & Type Casting
# -------------------------------
# Replace null values in StockLevel with 0 and ensure data types are correct
df_inventory = df_inventory.fillna({"StockLevel": 0}) \
                           .withColumn("StockLevel", col("StockLevel").cast("integer")) \
                           .withColumn("ReorderLevel", col("ReorderLevel").cast("integer"))

df_orders = df_orders.fillna({"Quantity": 1}) \
                     .withColumn("Quantity", col("Quantity").cast("integer"))

# Drop rows with missing ItemID or Warehouse
df_inventory = df_inventory.dropna(subset=["ItemID", "Warehouse"])
df_orders = df_orders.dropna(subset=["OrderID", "ItemID", "Warehouse"])

# Show cleaned data
print("Cleaned Inventory Data:")
df_inventory.show(5)

print("Cleaned Order Data:")
df_orders.show(5)

# -------------------------------
# Perform Joins (INNER, LEFT, RIGHT, OUTER)
# -------------------------------
# INNER JOIN: Get only orders that have matching inventory items
df_inner_join = df_orders.join(df_inventory, on=["ItemID", "Warehouse"], how="inner")
print("Inner Join Results:")
df_inner_join.show(5)

# LEFT JOIN: Get all orders, even if inventory data is missing
df_left_join = df_orders.join(df_inventory, on=["ItemID", "Warehouse"], how="left")
print("Left Join Results:")
df_left_join.show(5)

# RIGHT JOIN: Get all inventory items, even if they have no orders
df_right_join = df_orders.join(df_inventory, on=["ItemID", "Warehouse"], how="right")
print("Right Join Results:")
df_right_join.show(5)

# FULL OUTER JOIN: Get all records from both tables
df_outer_join = df_orders.join(df_inventory, on=["ItemID", "Warehouse"], how="outer")
print("Outer Join Results:")
df_outer_join.show(5)

# -------------------------------
# Union Operations: Combine Order Data from Multiple Warehouses
# -------------------------------
# Simulating data from multiple locations
df_orders_warehouse_A = df_orders.filter(col("Warehouse") == "Warehouse_A")
df_orders_warehouse_B = df_orders.filter(col("Warehouse") == "Warehouse_B")

# Perform UNION operation (Union requires same schema)
df_combined_orders = df_orders_warehouse_A.union(df_orders_warehouse_B)
print("Combined Orders from Warehouse_A and Warehouse_B:")
df_combined_orders.show(5)

# Stop SparkSession
spark.stop()
