import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, size, explode

sys.path.insert(0, '/opt/spark/src/pipeline')
sys.path.insert(0, '/opt/spark/src/similarity')

def inspect_lsh_bands(file_path="/opt/spark/data/lsh_bands.parquet"):
    """Inspect LSH bands Parquet file in detail."""
    
    spark = SparkSession.builder \
        .appName("Inspect LSH Bands") \
        .getOrCreate()
    
    print("=" * 80)
    print(f"INSPECTING: {file_path}")
    print("=" * 80)
    
    # Read data
    df = spark.read.parquet(file_path)
    
    # Basic info
    print(f"\n1. TOTAL RECORDS: {df.count():,}")
    
    # Schema
    print("\n2. SCHEMA:")
    df.printSchema()
    
    # Sample records in detail
    print("\n3. SAMPLE RECORDS (first 2, full detail):")
    
    # Method 1: Show with vertical formatting
    df.limit(2).show(vertical=True, truncate=False)
    
    # Method 2: Convert to Pandas for detailed view
    print("\n4. PANDAS VIEW (first 2 records):")
    pd_df = df.limit(2).toPandas()
    
    for idx, row in pd_df.iterrows():
        print(f"\n--- Record {idx} ---")
        print(f"doc_id: {row['doc_id']}")
        print(f"abstract (first 100 chars): {row['abstract'][:100]}...")
        print(f"signature length: {len(row['signature'])}")
        print(f"signature first 5 values: {row['signature'][:5]}")
        print(f"bands length: {len(row['bands'])}")
        print(f"bands values: {row['bands']}")
        
        # Show band composition
        print(f"\n  Band composition (8 values per band):")
        signature = row['signature']
        bands = row['bands']
        rows_per_band = len(signature) // len(bands)
        
        for band_idx, band_hash in enumerate(bands):
            start = band_idx * rows_per_band
            end = start + rows_per_band
            band_values = signature[start:end]
            print(f"  Band {band_idx} (hash={band_hash}): values={band_values}")
    
    # Statistics
    print("\n5. STATISTICS:")
    
    
    # Band counts
    band_counts = df.select(size(col("bands")).alias("band_count"))
    print("\n  Band array lengths:")
    band_counts.groupBy("band_count").count().orderBy("band_count").show()
    
    # Unique bands
    print("\n  Unique band hashes (first 10):")
    df.select(explode(col("bands")).alias("band_hash")) \
      .distinct() \
      .limit(10) \
      .show(truncate=False)
    
    # Documents per band (top 10)
    print("\n  Documents per band (top 10 most common bands):")
    df.select(col("doc_id"), explode(col("bands")).alias("band_hash")) \
      .groupBy("band_hash") \
      .count() \
      .orderBy(col("count").desc()) \
      .limit(10) \
      .show(truncate=False)
    
    print("=" * 80)
    spark.stop()

if __name__ == "__main__":
    inspect_lsh_bands()

    # spark = SparkSession.builder \
    #             .appName("Test_Parquet") \
    #             .getOrCreate()

    # output_path="/opt/spark/data/lsh_bands.parquet"
    # df_loaded = spark.read.parquet(output_path)
    # verify_count = df_loaded.count()
    # print(f" Verified {verify_count:,} records in Parquet")
    
    # print("\nSchema is: ---------")
    # df_loaded.printSchema()

    # print("\nThe first 5 records are: -----------------")
    # df_loaded.select("bands").show(5, truncate=50)

    # df_loaded.select("bands").first()[0]
    # spark.stop()
    # print("Spark Teminated!!!!")