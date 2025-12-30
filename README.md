# Real-Time Document Similarity Engine

> Finding near-duplicate and related documents in milliseconds using MinHash LSH and distributed computing

**Status:** 🚧 In Development - Phase 1 (Week 1/4)

## Project Overview

A production-grade document similarity system that processes streaming documents, identifies near-duplicates, and surfaces related content in real-time.

**Use Case:** Plagiarism Detection for arXiv Papers
- **Dataset:** 100K arXiv paper abstracts
- **Problem:** Identify submitted papers similar to existing work
- **Scale:** Processing 100K+ documents
- **Requirement:** Sub-second query response for new submissions

## Tech Stack

- **Core Algorithm:** MinHash + Locality-Sensitive Hashing (LSH)
- **Distributed Computing:** Apache Spark
- **Storage:** Redis (in-memory index)
- **API:** FastAPI
- **Deployment:** Docker

## Project Timeline

- [ ] **Phase 1 (Week 1):** System Architecture & Local Prototype
- [ ] **Phase 2 (Week 2):** Scale with Spark + Distributed Storage
- [ ] **Phase 3 (Week 3):** REST API + Query Service
- [ ] **Phase 4 (Week 4):** Production Deployment & Monitoring

## Current Phase: Phase 1 - Local Prototype

### Progress
- [x] Project setup
- [x] Dataset acquisition (arXiv 100K abstracts)
- [x] MinHash implementation
- [ ] Text preprocessing pipeline
- [ ] Baseline end-to-end demo

## Repository Structure
```
├── data/                  # Dataset files (not tracked in git)
├── docs/                  # Documentation
├── notebooks/             # Jupyter notebooks for exploration
├── src/                   # Source code
│   ├── similarity/        # MinHash + LSH implementation
│   ├── preprocessing/     # Text processing
│   └── pipeline/          # End-to-end pipeline
├── tests/                 # Unit tests
├── README.md
└── requirements.txt
```

## Setup Instructions

*Coming soon - Phase 1 in progress*

---

**Portfolio Context:** This project demonstrates end-to-end ML system design, scalable algorithms, and production engineering thinking - translating academic ML concepts into deployable systems.