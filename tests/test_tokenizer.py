"""
Unit tests for text preprocessing
"""
from src.preprocessing.tokenizer import (
    create_shingles,
    tokenize_words,
    create_word_shingles,
    preprocess_for_minhash
)

def test_create_shingles():
    """Test character shingling"""
    text = "hello"
    shingles = create_shingles(text, k=3)

    expected = {'hel', 'ell', 'llo'}

    assert shingles == expected, f"Expected {expected} but got {shingles}"
    print(f"text_create_shingles passed")

def test_create_shingles_short_text():
    """Test shingling with text shorter than k"""
    text = "hi"
    shingles = create_shingles(text, k=3)

    assert shingles == set(), f"Expected empty set, got {shingles}"
    print("test_create_shingles_short_text passed")

def test_tokenize_words():
    """Test word tokenization"""
    text = "The quick brown fox!"
    tokens = tokenize_words(text)

    expected = ['the', 'quick', 'brown', 'fox']

    assert tokens == expected, f"Expected {expected}, got {tokens}"
    print("text_tokenize_words passed")

def test_tokenize_words_min_length():
    """Test word tokenization with min_length filter"""
    text = "I am a student"
    tokens = tokenize_words(text, min_length=3)

    # Should filter out "I" and "am" (length < 3)
    assert 'student' in tokens
    assert 'i' not in tokens
    assert 'am' not in tokens
    print("test_tokenize_words_min_length passed")

def test_create_word_shingles():
    """Test word shingling"""
    text = "the quick brown fox"
    shingles = create_word_shingles(text, k=2)
    
    expected = {'the quick', 'quick brown', 'brown fox'}
    assert shingles == expected, f"Expected {expected}, got {shingles}"
    print("test_create_word_shingles passed")

def test_preprocess_for_minhash():
    """Test unified preprocessing interface"""
    text = "hello world"
    
    # Test different methods
    char_shingles = preprocess_for_minhash(text, method="char_shingles", k=3)
    word_tokens = preprocess_for_minhash(text, method="word_tokens")
    word_shingles = preprocess_for_minhash(text, method="word_shingles", k=2)
    
    assert len(char_shingles) > 0
    assert 'hello' in word_tokens
    assert 'hello world' in word_shingles
    print("test_preprocess_for_minhash passed")


if __name__ == "__main__":
    test_create_shingles()
    test_create_shingles_short_text()
    test_tokenize_words()
    test_tokenize_words_min_length()
    test_create_word_shingles()
    test_preprocess_for_minhash()
    print("\n All tokenizer tests passed!")