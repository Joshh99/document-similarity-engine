from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, collect_list
import json


def export_lsh_index(
    input_path="/opt/spark/data/lsh_bands.parquet",
    output_path="/opt/spark/data/lsh_index.json"
):
    """
    Convert LSH bands to Redis-ready format.
    
    Output: {band_hash: [doc_id1, doc_id2, ...]}
    """
    spark = SparkSession.builder \
        .appName("Export LSH Index") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    
    print("=" * 60)
    print("EXPORTING LSH INDEX FOR REDIS")
    print("=" * 60)
    
    # Load bands
    print(f"\nLoading from: {input_path}")
    df = spark.read.parquet(input_path)
    print(f"Loaded {df.count():,} documents")
    
    # Step 1: Explode bands array into separate rows
    # Before: [doc1, [band0, band1, ...]]
    # After:  [doc1, band0], [doc1, band1], ...
    print("\nExploding bands...")
    df_exploded = df.select(
        col("doc_id"),
        explode(col("bands")).alias("band_hash")
    )
    
    print(f"Total (doc_id, band_hash) pairs: {df_exploded.count():,}")
    
    # Step 2: Group by band_hash, collect doc_ids
    # Result: {band_hash: [doc_id1, doc_id2, ...]}
    print("\nGrouping by band_hash...")
    df_grouped = df_exploded.groupBy("band_hash") \
        .agg(collect_list("doc_id").alias("doc_ids"))
    
    num_buckets = df_grouped.count()
    print(f"Number of unique band buckets: {num_buckets:,}")
    
    # Show sample
    print("\nSample buckets:")
    df_grouped.show(5, truncate=False)
    
    # Step 3: Convert to dictionary and save as JSON
    print(f"\nCollecting to driver and saving to {output_path}...")
    
    # Collect to driver (this brings data to master node)
    buckets = df_grouped.collect()
    
    # Convert to dictionary
    lsh_index = {}
    for row in buckets:
        band_hash = str(row.band_hash)  # Convert to string for JSON
        doc_ids = row.doc_ids
        lsh_index[band_hash] = doc_ids
    
    # Save as JSON
    with open(output_path, 'w') as f:
        json.dump(lsh_index, f)
    
    # Summary stats
    bucket_sizes = [len(docs) for docs in lsh_index.values()]
    avg_bucket_size = sum(bucket_sizes) / len(bucket_sizes)
    max_bucket_size = max(bucket_sizes)
    
    print("\n" + "=" * 60)
    print("EXPORT SUMMARY")
    print("=" * 60)
    print(f"Total buckets:       {num_buckets:,}")
    print(f"Average bucket size: {avg_bucket_size:.1f} docs")
    print(f"Max bucket size:     {max_bucket_size} docs")
    print(f"Output file:         {output_path}")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    export_lsh_index()