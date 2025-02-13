# Generating a sample DataFrame
data = [(1, "Alice", 5000), (2, "Bob", 7000), (3, "Charlie", 9000)]
columns = ["CustomerID", "Name", "Balance"]



data = [(1, "Alice", 5000), (2, None, 7000), (3, "Charlie", None)]
columns = ["CustomerID", "Name", "Balance"]


df.select([col(c).isNull().alias(c) for c in df.columns]).show()
