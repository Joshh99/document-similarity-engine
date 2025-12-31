from pyspark.sql import SparkSession

# When running inside container, connect to local master
spark = SparkSession.builder \
    .appName("DocumentSimilarity-ClusterTest") \
    .getOrCreate()

print(f" Connected to Spark cluster!")
print(f"Spark version: {spark.version}")
print(f"Master URL: {spark.sparkContext.master}")
print(f"App ID: {spark.sparkContext.applicationId}")

# Simple test
data = [("doc1", "test abstract one"), ("doc2", "test abstract two")]
df = spark.createDataFrame(data, ["id", "text"])
print(f"\n Created DataFrame with {df.count()} rows")
df.show()

spark.stop()
print("\n Job completed successfully!")
