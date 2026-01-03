import json 
import redis
import time

def load_lsh_index_to_redis(
    json_path="/opt/spark/data/lsh_index.json",
    redis_host="redis-similarity",
    redis_port=6379,
    redis_db=0
):
    """
    Load LSH index from JSON into Redis.
    
    Schema:
    - Key: "band:{band_hash}"
    - Value: Set of doc_ids
    """
    print("=" * 60)
    print("LOADING LSH INDEX INTO REDIS")
    print("=" * 60)

    # Connect to Redis
    print(f"\nConnecting to Redis at {redis_host}:{redis_port}...")
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

    # Test connection
    r.ping()
    print("Connected to Redis")

    # Load JSON
    print(f"\nLoading index from {json_path}...")
    with open(json_path, 'r') as f:
        lsh_index = json.load(f)

    print(f"Loaded {len(lsh_index):,} band buckets")

    # Clear existing data (optional - be careful in production!)
    print("\nClearing existing Redis data...")
    r.flushdb()

    # Load into Redis
    print("\nLoading into Redis...")
    start_time = time.time()

    pipeline = r.pipeline()
    count = 0

    for band_hash, doc_ids in lsh_index.items():
        key = f"band:{band_hash}"

        # Store as Redis Set (efficient for membership queries)
        pipeline.sadd(key, *doc_ids)

        count += 1
        if count % 10000 == 0:
            pipeline.execute()
            pipeline = r.pipeline()
            print(f"  Loaded {count:,} buckets...")
        
    # Execute remaining
    pipeline.execute()

    load_time = time.time() - start_time

    # Verify
    total_keys = r.dbsize()

    # Get sample stats
    sample_key = f"band:{list(lsh_index.keys())[0]}"
    sample_size = r.scard(sample_key)

    print("\n" + "=" * 60)
    print("LOAD SUMMARY")
    print("=" * 60)
    print(f"Total keys in Redis:  {total_keys:,}")
    print(f"Load time:            {load_time:.2f}s")
    print(f"Throughput:           {total_keys/load_time:.0f} keys/sec")
    print(f"Sample key:           {sample_key}")
    print(f"Sample bucket size:   {sample_size}")
    print("=" * 60)

    # Test query
    print("\n Testing query for withdrawn papers...")
    test_band = "-6424781738644967111"  # The bucket with 45 withdrawn papers
    test_key = f"band:{test_band}"
    candidates = r.smembers(test_key)
    
    print(f"Query band {test_band}:")
    print(f"Found {len(candidates)} candidate documents")
    print(f"Sample docs: {list(candidates)[:5]}")
    
    return r

if __name__ == "__main__":
    load_lsh_index_to_redis()
    