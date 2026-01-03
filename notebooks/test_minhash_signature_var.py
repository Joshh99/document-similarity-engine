from pyspark.sql import SparkSession
from pyspark.sql.functions import size, col, explode

def check_signature_variance(file_path="/opt/spark/data/minhash_signatures.parquet"):
    """Check if MinHash signatures are too diverse."""
    spark = SparkSession.builder \
        .appName("Test MinHash Variance") \
        .getOrCreate()
    
    df = spark.read.parquet(file_path)
    
    # Check if signatures are mostly zeros or have limited range
    
    # Count unique values in signatures
    unique_counts = df.select(
        explode(col("signature")).alias("hash_val")
    ).distinct().count()
    
    print("=" * 60)
    print("=" * 60)
    print("Checking Minhash Var")
    print("=" * 60)
    print("=" * 60)
    print(" " * 60)
    print(" " * 60)
    
    print("=" * 60)
    print("=" * 60)
    print(f"Unique MinHash values across all documents: {unique_counts:,}")
    
    print("=" * 60)
    print("=" * 60)
    # Expected: Should have many repeated values across documents
    # If each document has completely unique values → problem!

if __name__ == "__main__":
    check_signature_variance()