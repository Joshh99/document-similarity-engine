from pyspark.sql import SparkSession
import time

def build_minhash_index(spark, output_path="/opt/spark/data/minhash_signatures.parquet"):
    """
    Build complete MinHash index for all documents
    
    Args:
        spark: SparkSession
        output_path: Where to save Parquet files
        
    Returns:
        DataFrame with signatures
    """
    import sys
    sys.path.insert(0, '/opt/spark/src/pipeline')
    sys.path.insert(0, '/opt/spark/src/similarity')
    
    from load_data import load_arxiv_data
    from spark_minhash import SparkMinHash

    print("=" * 60)
    print("BUILDING MINHASH INDEX")
    print("=" * 60)

    # Load data
    print("\n Step 1: Loading dataset...")
    start_load = time.time()
    df = load_arxiv_data(spark)
    load_count = df.count()
    load_time = time.time() - start_load
    print(f" Loaded {load_count:,} documents in {load_time:.2f}s")

    # Generate signatures
    print("\n Step 2: Generating MinHash signatures...")
    minhash = SparkMinHash(num_hashes=128)
    
    start_sig = time.time()
    df_signatures = minhash.process_dataframe(df)
    df_signatures.cache()  # Cache in memory

    sig_count = df_signatures.count()  # Trigger computation
    sig_time = time.time() - start_sig

    print(f" Generated {sig_count:,} signatures in {sig_time:.2f}s")
    print(f" Throughput: {sig_count/sig_time:.0f} docs/sec")

    # Save to Parquet
    print(f"\n Step 3: Saving to {output_path}...")
    start_save = time.time()

    df_signatures.write \
        .mode("overwrite") \
        .parquet(output_path)
    
    save_time = time.time() - start_save
    print(f" Saved to Parquet in {save_time:.2f}s")

    # Verify saved data
    print("\n🔍 Step 4: Verifying saved data...")
    df_loaded = spark.read.parquet(output_path)
    verify_count = df_loaded.count()
    print(f" Verified {verify_count:,} records in Parquet")

    # Show sample
    print("\nSample records:")
    df_loaded.select("doc_id", "signature").show(3, truncate=50)

    # Summary
    total_time = time.time() - start_load
    print("\n" + "=" * 60)
    print("PIPELINE SUMMARY")
    print("=" * 60)
    print(f"Documents processed: {sig_count:,}")
    print(f"Load time:          {load_time:.2f}s")
    print(f"Signature time:     {sig_time:.2f}s")
    print(f"Save time:          {save_time:.2f}s")
    print(f"Total time:         {total_time:.2f}s")
    print(f"Output:             {output_path}")
    print("=" * 60)
    
    return df_loaded

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("DocumentSimilarity-BuildIndex") \
        .getOrCreate()
    
    df = build_minhash_index(spark=spark)

    spark.stop()
    print("\n Index build complete!")