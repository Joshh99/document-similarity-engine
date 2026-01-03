from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, count, col

def check_band_collisions(file_path="/opt/spark/data/lsh_bands.parquet"):
    """Check if band hashes actually collide across documents."""
    
    spark = SparkSession.builder \
        .appName("Check Band Collisions") \
        .getOrCreate()
    
    df = spark.read.parquet(file_path)
    
    print("Checking band hash collisions...")
    
    # Explode bands
    exploded = df.select("doc_id", explode("bands").alias("band_hash"))
    
    # Count documents per band_hash
    band_stats = exploded.groupBy("band_hash") \
        .agg(count("doc_id").alias("doc_count"))
    
    # Show distribution
    print("\nDocuments per band_hash distribution:")
    band_stats.groupBy("doc_count") \
        .count() \
        .orderBy("doc_count") \
        .show(20)
    
    # Find bands with most documents (should have many!)
    print("\nTop 10 bands with most documents:")
    band_stats.orderBy(col("doc_count").desc()) \
        .limit(10) \
        .show(truncate=False)
    
    # What percentage of bands have collisions?
    total_bands = band_stats.count()
    single_doc_bands = band_stats.filter(col("doc_count") == 1).count()
    multi_doc_bands = total_bands - single_doc_bands
    
    print(f"\n--- Statistics ---")
    print(f"Total unique band hashes: {total_bands:,}")
    print(f"Bands with only 1 document: {single_doc_bands:,} ({single_doc_bands/total_bands:.1%})")
    print(f"Bands with 2+ documents: {multi_doc_bands:,} ({multi_doc_bands/total_bands:.1%})")
    
    spark.stop()

if __name__ == "__main__":
    check_band_collisions()