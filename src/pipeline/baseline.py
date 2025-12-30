"""
Baseline similarity search pipeline using MinHash LSH.

This module implements a simple in-memory similarity search system
that indexes documents and finds similar documents efficiently.
"""

from typing import List, Tuple, Dict, Set
import numpy as np
from src.similarity.minhash import MinHashLSH
from src.preprocessing.tokenizer import preprocess_for_minhash


class SimilarityPipeline:
    """
    End-to-end pipeline for document similarity search.
    
    This baseline implementation stores all signatures in memory
    and performs brute-force comparison for similarity search.
    Later phases will add LSH indexing for scalability.
    
    Args:
        num_hashes: Number of hash functions for MinHash
        num_bands: Number of bands for LSH
        preprocessing_method: Method for text preprocessing
        shingle_size: Size parameter for shingling (k)
    """
    
    def __init__(
        self,
        num_hashes: int = 128,
        num_bands: int = 16,
        preprocessing_method: str = "char_shingles",
        shingle_size: int = 3
    ):
        self.minhash = MinHashLSH(num_hashes=num_hashes, num_bands=num_bands)
        self.preprocessing_method = preprocessing_method
        self.shingle_size = shingle_size
        
        # Storage for indexed documents
        self.doc_ids: List[str] = []
        self.signatures: List[np.ndarray] = []
        self.documents: Dict[str, str] = {}  # doc_id -> original text
    
    def index_documents(self, documents: List[Dict[str, str]]) -> None:
        """
        Build MinHash index from document corpus.
        
        Args:
            documents: List of dicts with 'id' and 'text' keys
        """ 
        print(f"Indexing {len(documents)} documents...")
    
        for i, doc_dict in enumerate(documents):
            # Extract id and text from the dictionary
            doc_id = doc_dict['id']
            document = doc_dict['text']
            
            # a. Preprocess text to get token set
            token_set = preprocess_for_minhash(
                document, 
                self.preprocessing_method, 
                k=self.shingle_size
            )
            
            # b. Compute MinHash signature
            signature = self.minhash.compute_signature(tokens=token_set)
            
            # c. Store doc_id, signature, and original text
            self.documents[doc_id] = document
            self.doc_ids.append(doc_id)
            self.signatures.append(signature)
            
            # 2. Print progress every 1000 documents
            if (i + 1) % 1000 == 0:
                print(f"  Indexed {i + 1:,} documents...")
        
        print(f"Indexed {len(documents):,} documents")
    
    def query(self, text: str, top_k: int = 10, threshold: float = 0.0) -> List[Tuple[str, float]]:
        """
        Find top-k most similar documents to query text.
        
        Args:
            text: Query text
            top_k: Number of results to return
            threshold: Minimum similarity threshold (0.0 to 1.0)
        
        Returns:
            List of (doc_id, similarity_score) tuples, sorted by similarity descending
        """

        # Algorithm:
        # 1. Preprocess query text to get token set
        # 2. Compute MinHash signature for query
        # 3. Compare query signature with all indexed signatures
        # 4. Filter by threshold
        # 5. Sort by similarity 
        # (descending) and return top-k
        
        token_set = preprocess_for_minhash(text, self.preprocessing_method, k=self.shingle_size)
        signature = self.minhash.compute_signature(tokens=token_set)

        results: List[Tuple[str, float]] = []
        
        for i, sig in enumerate(self.signatures):
            est_jacc = self.minhash.estimate_jaccard(sig1=signature, sig2=sig)
            if est_jacc >= threshold:
                results.append((self.doc_ids[i], est_jacc))
                # keep_id = keep_id.append(self.doc_ids[i])
        
        return sorted(results, key=lambda x: x[1], reverse=True)

    
    def get_stats(self) -> Dict[str, any]:
        """Get statistics about the indexed corpus"""
        return {
            'num_documents': len(self.doc_ids),
            'num_hashes': self.minhash.num_hashes,
            'preprocessing_method': self.preprocessing_method,
            'shingle_size': self.shingle_size
        }


def main():
    """Simple test of the pipeline"""
    # Create sample documents
    documents = [
        {'id': 'doc1', 'text': 'The quick brown fox jumps over the lazy dog'},
        {'id': 'doc2', 'text': 'The quick brown fox jumps over the sleepy dog'},
        {'id': 'doc3', 'text': 'A fast brown fox leaps over a lazy dog'},
        {'id': 'doc4', 'text': 'Hello world this is a completely different document'},
    ]
    
    # Create pipeline and index documents
    pipeline = SimilarityPipeline(
        num_hashes=128,
        num_bands=16,
        preprocessing_method="word_shingles",
        shingle_size=2
    )
    
    print("Indexing documents...")
    pipeline.index_documents(documents)
    
    print(f"\nIndex stats: {pipeline.get_stats()}")
    
    # Query for similar documents
    query_text = "The quick brown fox jumps over the lazy dog"
    print(f"\nQuery: '{query_text}'")
    
    # First show ALL results (no threshold)
    print("\n--- All results (threshold=0.0) ---")
    all_results = pipeline.query(query_text, top_k=10, threshold=0.0)
    for doc_id, similarity in all_results:
        print(f"  {doc_id}: {similarity:.3f}")
    
    # Then show filtered results
    print("\n--- Filtered results (threshold=0.3) ---")
    filtered_results = pipeline.query(query_text, top_k=3, threshold=0.3)
    for doc_id, similarity in filtered_results:
        print(f"  {doc_id}: {similarity:.3f} - {pipeline.documents[doc_id][:60]}...")


if __name__ == "__main__":
    main()