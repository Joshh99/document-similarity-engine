"""
Text preprocessing utilities for document similarity.

Implements character-level shingling and word-level tokenization
for converting raw text into token sets suitable for MinHash.
"""

import re 
from typing import Set, List
from collections import Counter

def create_shingles(text: str, k: int=3) -> Set[str]:
    """
    Generate k-character shingles from text.
    
    Character shingles are better than word tokens for detecting near-duplicates
    because they're robust to minor word variations and typos.
    
    Args:
        text: Input text string
        k: Length of each shingle (default: 3)
    
    Examples:
        >>> create_shingles("hello", k=3)
        {'hel', 'ell', 'llo'}
    
    Returns:
        Set of k-character shingles
    """
    # shingles_set = Set()

    # Algorithm:
    # 1. Normalize text: lowercase, remove extra whitespace
    normalized_text = text.lower().strip()

    if len(normalized_text) < k:
        return set()  # Return empty set if text is shorter than k

    # 2. Slide a window of size k across the text
    # 3. Add each k-character substring to a set
    shingles_set = {normalized_text[i:i+k] for i in range(len(normalized_text) - k + 1)}

    return shingles_set
    

def tokenize_words(text: str, min_length: int = 2) -> List[str]:
    """
    Tokenize text into words with basic normalization.
    
    Args:
        text: Input text string
        min_length: Minimum word length to keep (default: 2)
    
    Returns:
        List of normalized word tokens
    """

    # Algorithm:
    # 1. Lowercase the text
    # 2. Remove punctuation and split on whitespace
    # Hint: Use re.findall(r'\b\w+\b', text.lower()) to extract words
    words = re.findall(r'\b\w+\b', text.lower())

    # 3. Filter out short words (< min_length)
    filtered_words = [word for word in words if len(word) >= min_length]
    
    return filtered_words


def create_word_shingles(text: str, k: int = 2) -> Set[str]:
    """
    Generate k-word shingles from text.
    
    Word shingles (n-grams) capture phrase-level similarity.
    Example: "the quick brown fox" with k=2 gives {"the quick", "quick brown", "brown fox"}
    
    Args:
        text: Input text string
        k: Number of words per shingle (default: 2)
    
    Returns:
        Set of k-word shingles
    """
    
    # Algorithm:
    # 1. Tokenize text into words
    words = tokenize_words(text)

    # If we want to preserve stop words, use simple split:
    # words = text.lower().split()

    # 2. Slide a window of size k across the word list
    word_shingles = set()

    # Need at least k words to create shingles
    if len(words) < k:
        return set()
    
    for i in range(len(words) - k + 1):
        # 3. Join each k-word sequence with space and add to set
        shingle = " ".join(words[i:i+k])
        word_shingles.add(shingle)
    
    return word_shingles

def compute_term_frequency(tokens: List[str]) -> Counter:
    """
    Compute term frequency for a list of tokens.
    
    Args:
        tokens: List of tokens (words or shingles)
    
    Returns:
        Counter object with token frequencies
    """
    return Counter(tokens)


def preprocess_for_minhash(text: str, method: str = "char_shingles", k: int = 3) -> Set[str]:
    """
    Preprocess text and return token set ready for MinHash.
    
    Args:
        text: Input text string
        method: Tokenization method - "char_shingles", "word_tokens", or "word_shingles"
        k: Parameter for shingling (character length or word count)
    
    Returns:
        Set of tokens
    """
    if method == "char_shingles":
        return create_shingles(text, k=k)
    elif method == "word_tokens":
        return set(tokenize_words(text))
    elif method == "word_shingles":
        return create_word_shingles(text, k=k)
    else:
        raise ValueError(f"Unknown method: {method}")


def main():
    """Test preprocessing functions"""
    text = "The quick brown fox jumps over the lazy dog. it is a so. I can do it."
    
    print("Original text:", text)
    print("\nCharacter 3-shingles:", create_shingles(text, k=3))
    print("\nWord tokens:", tokenize_words(text))
    print("\nWord 2-shingles:", create_word_shingles(text, k=2))


if __name__ == "__main__":
    main()