from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, LongType


def compute_bands(signature, num_bands=32):
    """
    Split signature into bands for LSH.
    Uses simple tuple hashing that works in Spark UDFs.
    """
    if signature is None or len(signature) != 128:
        return [0] * num_bands
    
    rows_per_band = len(signature) // num_bands
    bands = []
    
    for band_idx in range(num_bands):
        start = band_idx * rows_per_band
        end = start + rows_per_band
        
        # Get band slice (8 hash values)
        band_slice = tuple(signature[start:end])
        
        # Use Python's hash on tuple - deterministic within same Python version
        # We'll handle cross-version consistency later if needed
        band_hash = hash(band_slice)
        
        bands.append(band_hash)
    
    return bands


def add_lsh_bands(
    input_path="/opt/spark/data/minhash_signatures.parquet",
    output_path="/opt/spark/data/lsh_bands.parquet"
):
    """Add LSH band columns to signatures."""
    
    spark = SparkSession.builder \
        .appName("Compute LSH Bands") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .getOrCreate()
    
    print("=" * 60)
    print("COMPUTING LSH BANDS")
    print("=" * 60)
    
    # Load signatures
    print(f"\nLoading from: {input_path}")
    df = spark.read.parquet(input_path)
    count = df.count()
    print(f"Loaded {count:,} documents")
    
    # Register UDF - use LongType for hash values
    compute_bands_udf = udf(compute_bands, ArrayType(LongType()))
    
    # Add bands column
    print("\nComputing bands...")
    df_with_bands = df.withColumn("bands", compute_bands_udf(col("signature")))

    # Show sample
    print("\nSample output:")
    df_with_bands.select("doc_id", "bands").show(5, truncate=False)
    
    # Save
    print(f"\nSaving to: {output_path}")
    df_with_bands.write.mode("overwrite").parquet(output_path)
    
    # Verify
    df_verify = spark.read.parquet(output_path)
    verify_count = df_verify.count()
    
    # Check for nulls
    null_count = df_verify.filter(col("bands").isNull()).count()
    
    print(f"\n Saved {verify_count:,} records with LSH bands")
    print(f"   NULL bands: {null_count}")
    print("=" * 60)
    
    spark.stop()


if __name__ == "__main__":
    add_lsh_bands()