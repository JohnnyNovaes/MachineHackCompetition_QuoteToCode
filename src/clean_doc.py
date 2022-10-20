import nltk
nltk.download('punkt')
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import re
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from models.language_detector_model import LanguageDetector

def remove_stop_words(row: str) -> str:
    """ Remove english stop words from text."""
    words = nltk.word_tokenize(row)
    wordsFiltered = []
    for w in words:
        if w not in stopwords.words('english'):
            wordsFiltered.append(w)
    row = " ".join(wordsFiltered)
    return(row)

def clean_text(row: str) -> str:
    """ Remove HTML, especial caracteres and lowercase"""
    clean = re.compile('<.*?>')
    text_pipe = re.sub(clean, '', row)
    text_pipe = re.sub(r'[0-9]', " ", text_pipe)
    text_pipe = re.sub(r"(@\[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", " ", text_pipe)
    row = text_pipe.lower()
    return(row)

def get_languages(row: str) -> str:
    """ Detects the language of each doc in corpus."""
    language_detector = LanguageDetector()
    row = language_detector.detect_language(row)
    return(row)

def get_stem(row: str) -> str:
    """ Stemm words."""
    stemmer = SnowballStemmer("english")
    row = stemmer.stem(row)
    return(row)

def get_tokens(row: str) -> list[str]:
    """ Get tokens from doc."""
    tokens = nltk.word_tokenize(row)
    return(tokens)
