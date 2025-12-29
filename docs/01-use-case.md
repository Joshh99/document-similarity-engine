# Use Case: Plagiarism Detection for arXiv Papers

**Date:** December 29, 2025  
**Author:** Joshua  
**Phase:** 1 - System Architecture & Local Prototype

---

## Problem Statement

Academic researchers submit thousands of papers to arXiv monthly. Detecting potential plagiarism or highly similar existing work is critical for:
- Maintaining research integrity
- Helping reviewers identify prior art
- Assisting authors in finding related work
- Preventing duplicate publications

**Current Challenge:** Manual review is slow and doesn't scale. Naive pairwise comparison of N papers requires O(N²) comparisons - infeasible at scale.

---

## Dataset Choice: arXiv Paper Abstracts

**Source:** Kaggle arXiv Dataset (https://www.kaggle.com/datasets/Cornell-University/arxiv)

**Why arXiv Abstracts?**
- **Real-world relevance:** Actual academic papers, not synthetic data
- **Rich text content:** Abstracts are dense, technical, well-structured
- **Clear similarity metric:** Academic papers have natural near-duplicates (same authors, similar topics, follow-up work)
- **Publicly available:** Easy to access and reproduce
- **Manageable size:** 100K abstracts provide sufficient scale without requiring massive infrastructure

**Sample Size for This Project:** 100,000 abstracts

---

## Scale Requirements

### Current State (Manual/Naive)
- **Dataset:** 100K papers (will simulate real arXiv scale of 2.3M eventually)
- **Naive approach:** 100K × 100K = 10 billion comparisons
- **Estimated time:** Days to weeks for full pairwise comparison

### Target State (With MinHash LSH)
- **Query latency:** < 1 second for finding similar papers to a new submission
- **Indexing time:** < 30 minutes for 100K papers
- **Recall target:** > 80% (find at least 80% of truly similar papers)
- **Precision target:** > 70% (at least 70% of returned results are actually similar)

---

## Success Metrics

### Performance Metrics
1. **Query Latency (p95):** < 1 second
2. **Indexing Throughput:** > 3,000 documents/minute
3. **Memory Usage:** < 4GB for 100K document index

### Quality Metrics
1. **Recall@10:** > 0.80 (at similarity threshold 0.5)
2. **Precision@10:** > 0.70
3. **False Positive Rate:** < 0.15

### System Metrics
1. **Code Coverage:** > 80%
2. **API Response Time:** p95 < 100ms
3. **Docker Startup Time:** < 30 seconds

---

## Expected Outcomes

By the end of this project, I will have:
1. ✅ A working similarity search system for 100K documents
2. ✅ MinHash LSH implementation from scratch (not just using libraries)
3. ✅ Distributed processing pipeline using Spark
4. ✅ Production-ready FastAPI service
5. ✅ Comprehensive documentation and deployment guide
6. ✅ Portfolio-ready demo and technical writeup

---

## Next Steps

- [ ] Download and explore arXiv dataset (100K subset)
- [ ] Perform initial EDA on abstract lengths, vocabulary size
- [ ] Define baseline: simple cosine similarity on TF-IDF vectors
- [ ] Implement MinHash signature generation

**Target Completion:** Week 1 (Phase 1)