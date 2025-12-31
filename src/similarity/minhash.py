"""
MinHash Locality-Sensitive Hashing Implementation

Based on the algorithm from "Mining of Massive Datasets" (Chapter 3)
Extended for production use with configurable parameters.
"""

import numpy as np
from typing import Set, List, Tuple
import hashlib


class MinHashLSH:
    """
    Locality-Sensitive Hashing using MinHash signatures.
    
    This implementation computes MinHash signatures for documents represented
    as sets of tokens, enabling efficient approximate similarity search.
    
    Args:
        num_hashes: Number of hash functions to use (signature length)
        num_bands: Number of bands for LSH (must divide num_hashes evenly)
        seed: Random seed for hash function generation
    """
    
    def __init__(self, num_hashes: int = 128, num_bands: int = 16, seed: int = 42):
        if num_hashes % num_bands != 0:
            raise ValueError(f"num_hashes ({num_hashes}) must be divisible by num_bands ({num_bands})")
        
        self.num_hashes = num_hashes
        self.num_bands = num_bands
        self.rows_per_band = num_hashes // num_bands
        self.seed = seed
        
        # Generate hash function parameters
        np.random.seed(seed)
        self.hash_params = self._generate_hash_params()
    
    def _generate_hash_params(self) -> List[Tuple[int, int]]:
        """
        Generate parameters for hash functions.
        Each hash function is of the form: h(x) = (a*x + b) mod large_prime
        
        Returns:
            List of (a, b) tuples for each hash function
        """
        # Use a large prime number
        large_prime = 2**31 - 1
        
        params = []
        for _ in range(self.num_hashes):
            a = np.random.randint(1, large_prime)
            b = np.random.randint(0, large_prime)
            params.append((a, b))
        
        return params
    
    def compute_signature(self, tokens: Set[str]) -> np.ndarray:
        """
        Compute MinHash signature for a set of tokens.
        
        Args:
            tokens: Set of string tokens (words, shingles, etc.)
        
        Returns:
            MinHash signature as numpy array of length num_hashes
        """
         
        # Algorithm:
        large_prime = 2**31 - 1

        # 1. Initialize signature array with infinity values
        signature = np.full(self.num_hashes, np.inf)
        
        # 2. For each hash function
        for i, (a, b) in enumerate(self.hash_params):
            # For each token in the document
            for token in tokens:
                # a. Convert token to integer using hash
                token_hash = int(hashlib.md5(token.encode()).hexdigest(), 16)
                # print(f"Token integer: {token_hash}")

                # b. Compute hash value: (a * token_hash + b) mod large_prime
                hash_value = (a * token_hash + b) % large_prime
                # print(f"Hash_value: {hash_value}")

                # c. Update signature[i] = min(signature[i], hash_value)
                signature[i] = min(signature[i], hash_value)
                
        # 3. Return signature array
        return signature
    
    def estimate_jaccard(self, sig1: np.ndarray, sig2: np.ndarray) -> float:
        """
        Estimate Jaccard similarity from two MinHash signatures.
        
        Args:
            sig1: First MinHash signature
            sig2: Second MinHash signature
        
        Returns:
            Estimated Jaccard similarity (float between 0 and 1)
        """
    
        # The estimated Jaccard similarity is simply the fraction of
        # signature positions where sig1[i] == sig2[i]
        jacc_sim = np.sum(sig1 == sig2) / len(sig1)
        return jacc_sim


def main():
    """Quick test of MinHash implementation"""
    # Test with simple example
    doc1 = {"the", "quick", "brown", "fox"}
    doc2 = {"the", "quick", "brown", "dog"}
    doc3 = {"hello", "world", "test"}
    
    minhash = MinHashLSH(num_hashes=128, num_bands=16)
    
    sig1 = minhash.compute_signature(doc1)
    sig2 = minhash.compute_signature(doc2)
    sig3 = minhash.compute_signature(doc3)
    
    print(f"Signature shape: {sig1.shape}")
    print(f"Similarity(doc1, doc2): {minhash.estimate_jaccard(sig1, sig2):.3f}")
    print(f"Similarity(doc1, doc3): {minhash.estimate_jaccard(sig1, sig3):.3f}")
    print(f"True Jaccard(doc1, doc2): {len(doc1 & doc2) / len(doc1 | doc2):.3f}")


if __name__ == "__main__":
    main()