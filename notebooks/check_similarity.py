# Run this in spark-master container
from pyspark.sql import SparkSession
from pyspark.sql.functions import array_contains, col


spark = SparkSession.builder.appName("Check Similarity").getOrCreate()

# Load bands
df = spark.read.parquet("/opt/spark/data/lsh_bands.parquet")

# Get the docs in the largest bucket
target_band = 4600065898070791942

# Find which band position contains this hash

df_filtered = df.filter(array_contains(col("bands"), target_band))
df_filtered.select("doc_id", "abstract").show(10, truncate=80)

spark.stop()