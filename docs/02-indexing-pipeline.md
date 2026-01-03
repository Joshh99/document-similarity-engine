# Indexing Pipeline Documentation

## Overview
Distributed MinHash signature generation and LSH index building pipeline using Apache Spark.

## Architecture
```
┌─────────────────┐
│ arXiv Abstracts │
│   (99,904 docs) │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Spark Distributed Processing       │
│  ┌──────────────────────────────┐  │
│  │ 1. Text Preprocessing        │  │
│  │    - Lowercase & clean       │  │
│  │    - 3-gram shingles         │  │
│  └──────────────────────────────┘  │
│  ┌──────────────────────────────┐  │
│  │ 2. MinHash Signatures        │  │
│  │    - 128 hash functions      │  │
│  │    - MD5 token hashing       │  │
│  └──────────────────────────────┘  │
│  ┌──────────────────────────────┐  │
│  │ 3. LSH Band Computation      │  │
│  │    - 32 bands × 4 rows       │  │
│  │    - Threshold: ~0.3 Jaccard │  │
│  └──────────────────────────────┘  │
└─────────────────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Parquet Files           │
│ - minhash_signatures    │
│ - lsh_bands             │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Export & Index          │
│ - Group by band_hash    │
│ - Export to JSON        │
└─────────────────────────┘
         │
         ▼
┌─────────────────────────┐
│ Redis In-Memory Index   │
│ - 3.2M band buckets     │
│ - O(1) candidate lookup │
└─────────────────────────┘
```

## Performance Metrics

### Phase 1: Local Prototype
- **Dataset:** 10,000 documents
- **Throughput:** 4 docs/sec
- **Total time:** 41 minutes
- **Technology:** Single-threaded Python

### Phase 2: Distributed Processing
- **Dataset:** 99,904 documents (10x scale)
- **Throughput:** 172 docs/sec (43x improvement)
- **Total time:** 10 minutes
- **Technology:** Apache Spark (2 cores, 4GB RAM)

### Redis Loading
- **Index size:** 3,187,229 keys
- **Load time:** 22 seconds
- **Throughput:** 143,798 keys/sec
- **Memory usage:** [Add from redis stats]

## LSH Parameter Tuning

### Band Configuration Analysis

| Bands | Rows/Band | Threshold | Collision Rate | Decision |
|-------|-----------|-----------|----------------|----------|
| 16    | 8         | ~0.50     | 0.1%          | Too selective |
| 32    | 4         | ~0.30     | 0.2%          | **Optimal** ✅ |
| 64    | 2         | ~0.15     | 1.2%          | False positives |

**Rationale for 32 bands:**
- Captures genuinely similar documents (e.g., withdrawn paper templates)
- Minimizes false positives (64 bands matched dissimilar physics/math papers)
- Realistic for arXiv dataset where 99.8% of papers are unique

### Key Findings

1. **Dataset Characteristics:**
   - 58% of MinHash values are unique across all documents
   - Indicates high diversity in arXiv abstracts
   - Most papers are genuinely dissimilar (expected for academic research)

2. **LSH Validation:**
   - Largest bucket (45 docs) contains withdrawn paper notices
   - All share template: "This paper has been withdrawn by the author..."
   - Proves LSH correctly identifies true near-duplicates

3. **Collision Statistics:**
   - 99.8% of buckets contain 1 document (singleton)
   - 0.2% contain 2+ documents (7,962 buckets)
   - Max bucket size: 45 documents

## Trade-offs Made

### Recall vs. Precision
- **Choice:** 32 bands (medium recall, high precision)
- **Reasoning:** Better to return few accurate results than many false positives
- **Alternative:** Could use 64 bands for higher recall at cost of precision

### Memory vs. Speed
- **Choice:** Load full index into Redis (in-memory)
- **Reasoning:** Sub-millisecond queries worth the memory cost
- **Memory:** ~324.89MB for 3.2M keys
- **Alternative:** Could use disk-based storage (ElasticSearch) for larger datasets

### Batch vs. Streaming
- **Choice:** Offline batch processing with Spark
- **Reasoning:** arXiv grows slowly (~16K papers/month), batch reindexing is sufficient
- **Frequency:** Weekly/monthly reindex acceptable
- **Alternative:** Could use Spark Streaming for real-time updates

## Reproducibility

### Running the Full Pipeline
```bash
# 1. Start Docker services
docker-compose up -d

# 2. Build MinHash signatures
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/src/pipeline/build_index.py

# 3. Compute LSH bands
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/src/similarity/compute_lsh_bands.py

# 4. Export to JSON
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/src/similarity/export_to_redis.py

# 5. Load into Redis
docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /opt/spark/src/redis/load_index.py
```

### Expected Output
- `data/minhash_signatures.parquet` (~100 MB)
- `data/lsh_bands.parquet` (~120 MB)
- `data/lsh_index.json` (~120 MB)
- Redis: 3.2M keys in memory

## Scalability Projections

Based on current performance:

| Dataset Size | Estimated Time | Notes |
|--------------|----------------|-------|
| 100K papers  | 10 minutes     |  Tested |
| 1M papers    | ~97 minutes    | Linear scaling |
| 2.3M (full arXiv) | ~3.7 hours | Projected |
| 10M papers   | ~16 hours      | May need cluster scaling |

**Bottleneck:** Signature generation is CPU-bound, scales linearly with worker cores.

## Future Optimizations

1. **Increase Spark cluster size** - Add more workers for larger datasets
2. **Redis Cluster** - Shard index across multiple Redis nodes for >10M documents
3. **Incremental updates** - Add new papers without full reindex
4. **Compression** - Use Redis compression for larger indexes

---

**Created:** 2026-01-03  
**Last Updated:** 2026-01-03  
**Author:** Joshua