from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import ArrayType, IntegerType, StringType
import hashlib
import numpy as np
import re

class SparkMinHash:
    """Distributed MinHash signature generation using Spark"""
    def __init__(self, num_hashes: int=128, seed: int=42):
        self.num_hashes = num_hashes
        self.seed = seed
        self.large_prime = 2**31 - 1

        # Generate Hash params
        np.random.seed(seed)
        self.hash_params = []
        for _ in range(num_hashes):
            a = int(np.random.randint(1, self.large_prime))
            b = int(np.random.randint(0, self.large_prime))
            self.hash_params.append((a, b))

    @staticmethod
    def preprocess_text(text):
        """Text preprocessing - same as Phase 1"""
        if not text:
            return []
        
        # Lowercase and remove non-alphanumeric
        text = text.lower()
        text = re.sub(r'[^a-z0-9\s]', ' ', text)

        # Generate shingles (3-grams)
        words = text.split()
        shingles = []
        for i in range(len(words) - 2):
            shingle = ' '.join(words[i:i+3])
            shingles.append(shingle)
        
        return shingles
    

    def process_dataframe(self, df):
        """
        Generate MinHash signatures for all documents in DataFrame

        Args:
            df: Spark DataFrame with 'doc_id' and 'abstract' columns
            
        Returns:
            DataFrame with 'doc_id', 'abstract', 'signature' columns
        """
        # Broadcast hash params to all workers
        hash_params_bc = df.sparkSession.sparkContext.broadcast(self.hash_params)
        num_hashes = self.num_hashes
        large_prime = self.large_prime

        # UDF 1: Preprocess text to shingles (returns strings)
        preprocess_udf = udf(self.preprocess_text, ArrayType(StringType()))

        # UDF 2: Generate signature from shingles
        def generate_signature(shingles):
            """
            Exactly your Phase 1 algorithm, adapted for Spark
            """
            if not shingles:
                return [0] * num_hashes
            
            # Initialize signature with infinity
            signature = [float('inf')] * num_hashes

            # For each hash function
            for i, (a, b) in enumerate(hash_params_bc.value):
                # For each shingle (token)
                for shingle in shingles:
                    # Convert shingle to integer using MD5
                    token_hash = int(hashlib.md5(shingle.encode()).hexdigest(), 16)
                    
                    # Compute hash value: (a * token_hash + b) mod large_prime       
                    hash_value = (a * token_hash + b) % large_prime

                    # Update signature[i] = min(signature[i], hash_value)         
                    signature[i] = min(signature[i], hash_value)
            
            # Convert to integers for return
            return [int(sig) if sig != float('inf') else 0 for sig in signature]

        signature_udf = udf(generate_signature, ArrayType(IntegerType()))

        # Apply transformations
        # Apply transformations
        df_with_shingles = df.withColumn("shingles", preprocess_udf(col("abstract")))
        df_with_signatures = df_with_shingles.withColumn(
            "signature", 
            signature_udf(col("shingles"))
        )
        
        return df_with_signatures.select("doc_id", "abstract", "signature")
    

if __name__ == "__main__":
    import sys
    import time
    sys.path.append('/opt/spark/src/pipeline')
    from load_data import load_arxiv_data
    
    spark = SparkSession.builder \
        .appName("DocumentSimilarity-MinHash") \
        .getOrCreate()
    
    print("📥 Loading dataset...")
    start_load = time.time()
    df = load_arxiv_data(spark)
    load_count = df.count()
    load_time = time.time() - start_load
    print(f"✅ Loaded {load_count} documents in {load_time:.2f}s")
    
    print("\n🔨 Generating MinHash signatures...")
    minhash = SparkMinHash(num_hashes=128)
    
    start_sig = time.time()
    df_signatures = minhash.process_dataframe(df)
    
    # Cache for performance
    df_signatures.cache()
    
    # Trigger computation with count()
    count = df_signatures.count()
    sig_time = time.time() - start_sig
    
    print(f"✅ Generated signatures for {count} documents in {sig_time:.2f}s")
    print(f"⚡ Throughput: {count/sig_time:.0f} docs/sec")
    
    print(f"\n📊 Sample signatures:")
    df_signatures.select("doc_id", "signature").show(3, truncate=50)
    
    # Verify signature length
    first_sig = df_signatures.select("signature").first()[0]
    print(f"\n✅ Signature length: {len(first_sig)} (expected: 128)")
    
    total_time = time.time() - start_load
    print(f"\n⏱️  Total pipeline time: {total_time:.2f}s")
    
    spark.stop()
    print("\n🎉 MinHash generation complete!")