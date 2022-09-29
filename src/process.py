# files management 
from hydra import compose, initialize
import os
import pandas as pd
import logging

import sys
sys.path.append('../')
from models.language_detector_model import LanguageDetector

# Clean Corpus
import nltk
from nltk.corpus import stopwords
import re
nltk.download('punkt')
 


class DataGetter():
    """Load data and build dataframes."""
    
    def __init__(self):
        # Build dataframes
        logging.info("Loading datasets")
        
        initialize(version_base=None, config_path="../config", job_name="DataGetter")
        cfg = compose(config_name="data_getter")
        
        current_path = os.getcwd()
        
        actual_set_path = os.path.join(current_path, cfg.raw_path.actual_set)
        self.actual_set_df = pd.read_csv(actual_set_path)

        app_installs_path = os.path.join(current_path, cfg.raw_path.app_installs)
        self.app_installs_df = pd.read_csv(app_installs_path)

        app_metadata_path = os.path.join(current_path, cfg.raw_path.app_metadata)
        self.app_metadata_df = pd.read_csv(app_metadata_path)

        app_usage_path = os.path.join(current_path, cfg.raw_path.app_usage)
        self.app_usage_df = pd.read_csv(app_usage_path)

        validation_set_path = os.path.join(current_path, cfg.raw_path.validation_data)
        self.validation_set_df = pd.read_csv(validation_set_path)

        sample_submission_path = os.path.join(current_path, cfg.raw_path.sample_submission)
        self.sample_submission_df = pd.read_csv(sample_submission_path)

        logging.info("Loading datasets finished successfully!")

class CleanCorpus():
    """
     Clean the description text of apps.
        input: 
                data: Dataframe to be processed.
                doc_column: Target column with description text.
    """
    def __init__(self, data: pd.DataFrame, doc_column: str) -> None:
        self.data = data
        self.doc_column = doc_column

        self.stop_words = set(stopwords.words('english'))

    @staticmethod
    def get_languages(data: pd.DataFrame) -> list[str]:
        """ Detects the language of each doc in corpus."""
        language_detector = LanguageDetector()
        languages = []
        for _, row in data.iterrows():
            app_language = language_detector.detect_language(row.description)
            languages.append(app_language)
        return(languages)

    @staticmethod
    def remove_stop_words(stop_words: set(str), text: str) -> str:
        """ Remove english stop words from doc."""
        words = nltk.word_tokenize(text)
        wordsFiltered = []
        for w in words:
            if w not in stop_words:
                wordsFiltered.append(w)

            return(" ".join(wordsFiltered))

    @staticmethod
    def clean_text(text):
        """ Remove HTML, especial caracteres and lowercase"""
        clean = re.compile('<.*?>')
        text_pipe = re.sub(clean, '', text)
        text_pipe = re.sub(r"(@\[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|^rt|http.+?", " ", text_pipe)
        text_pipe = text_pipe.lower()
        return(text_pipe)

    def add_language(self) -> None:
        """ Creates a column language with the language of each doc."""
        languages = self.get_languages(self.data)
        self.data['language'] = languages

    def remove_non_english_docs(self) -> None:
        """ Remove apps with description not writen in english."""

        # Remove non english docs
        remove_indexes = self.data[self.data.language != 'en'].index
        self.data.drop(remove_indexes, inplace=True)

        # drop column language
        self.data.drop(columns=['language'],inplace=True)
        
