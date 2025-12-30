"""
Unit tests for MinHash implementation
"""

import token
import numpy as np
from src.similarity.minhash import MinHashLSH

def test_signature_length():
    """Test that signature has correct length"""

    minhash = MinHashLSH(num_hashes=128, num_bands=16)
    tokens = {"the", "quick", "brown", "fox"}

    signature = minhash.compute_signature(tokens=tokens)

    assert len(signature) == 128, f"Expected signature length 128, got {len(signature)}"
    print("test_signature_length passed")

def test_identical_documents():
    """Test that identical documents have similarity 1.0"""
    minhash = MinHashLSH(num_hashes=128, num_bands=16)
    tokens = {"the", "quick", "brown", "fox"}

    sig1 = minhash.compute_signature(tokens)
    sig2 = minhash.compute_signature(tokens)

    similarity = minhash.estimate_jaccard(sig1, sig2)

    assert similarity == 1.0, f"Expected similarity 1.0, instead got {similarity}"
    print("test_identical_documents passed")

def test_disjoint_documents():
    """Test that completely different documents have low similarity"""
    minhash = MinHashLSH(num_hashes=128, num_bands=16)
    
    doc1 = {"the", "quick", "brown", "fox"}
    doc2 = {"hello", "world", "goodbye", "universe"}
    
    sig1 = minhash.compute_signature(doc1)
    sig2 = minhash.compute_signature(doc2)
    
    similarity = minhash.estimate_jaccard(sig1, sig2)

    assert similarity < 0.1, f"Expected similarity < 0.1, got {similarity}"
    print("test_disjoint_documents passed")

def test_jaccard_estimation():
    """Test that MinHash estimates Jaccard similarity reasonably well"""
    minhash = MinHashLSH(num_hashes=256, num_bands=16)  # More hashes = better estimate
    
    doc1 = {"a", "b", "c", "d", "e"}
    doc2 = {"c", "d", "e", "f", "g"}
    
    # True Jaccard: |{c,d,e}| / |{a,b,c,d,e,f,g}| = 3/7 ≈ 0.428
    true_jaccard = len(doc1 & doc2) / len(doc1 | doc2)

    sig1 = minhash.compute_signature(doc1)
    sig2 = minhash.compute_signature(doc2)
    estimated_jaccard = minhash.estimate_jaccard(sig1, sig2)

    error = abs(true_jaccard - estimated_jaccard)

    assert error < 0.15, f"Estimation error too large: {error:.3f}"
    print(f"test_jaccard_estimation passed (true: {true_jaccard:.3f}, estimated: {estimated_jaccard:.3f}, error: {error:.3f})")


if __name__ == "__main__":
    test_signature_length()
    test_identical_documents()
    test_disjoint_documents()
    test_jaccard_estimation()

    print("\nAll tests passed!")
