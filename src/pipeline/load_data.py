from pyspark.sql import SparkSession
from pyspark.sql.functions import col, length
import json

def load_arxiv_data(spark, data_path="/opt/spark/data/arxiv_100k_abstracts.json"):
    """Load arXiv abstracts into Spark DataFrame"""

    # Read JSON Lines
    df = spark.read.json(data_path)

    # 
    df_clean = df.filter(
        (col("abstract").isNotNull()) & 
        (length(col("abstract")) > 50)  # Filter very short abstracts
    ).select(
        col("id").alias("doc_id"),
        col("title"),
        col("abstract"),
        col("categories")
    )

    return df_clean

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DocumentSimilarity-LoadData") \
        .getOrCreate()
    
    print("Loading arXiv dataset...")
    df = load_arxiv_data(spark)

    print(f" Loaded {df.count()} documents")
    print(f" Schema:")
    df.printSchema()

    print(f"\n Sample documents:")
    df.show(5, truncate=50)

    print(f"\n Abstract length statistics:")
    df.select(length(col("abstract")).alias("abstract_length")).describe().show()

    spark.stop()
    print("\n Data load complete!")