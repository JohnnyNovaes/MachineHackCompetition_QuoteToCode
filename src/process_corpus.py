import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# Clean Corpus
from clean_doc import clean_text, get_tokens, remove_stop_words, get_languages, get_stem

# Dataframe
import pandas as pd

# Get Data
from load_data import DataGetter

# Configs
import hydra
from omegaconf import DictConfig

import logging

def add_languages(data: pd.DataFrame) -> pd.DataFrame:
    """Creates a column language with the language of each doc."""
    logging.info("Add Languages.")
    data['language'] = data.description.apply(get_languages)
    return(data)

def remove_stop_words_corpus(data: pd.DataFrame) -> pd.DataFrame:
    """Remove stop words from corpus."""
    logging.info("Remove Stop Words.")
    data['description'] = data.description.apply(remove_stop_words)
    return(data)

def clean_text_corpus(data: pd.DataFrame) -> pd.DataFrame:
    """Remove HTML, especial caracteres and lowercase from corpus"""
    logging.info("Clean Text Corpus.")
    data['description'] = data.description.apply(clean_text)
    return(data)

def remove_non_english_docs(data: pd.DataFrame) -> pd.DataFrame:
    """Remove apps with description not writen in english."""
    logging.info("Remove Non English Docs.")
    remove_indexes = data[data.language != 'en'].index
    data.drop(remove_indexes, inplace=True)
    return(data)

def remove_columns(data: pd.DataFrame, remove_columns: list[str]) -> pd.DataFrame:
    """Removes columns from dataset"""
    logging.info("Drop Columns.")
    data.drop(columns=remove_columns, inplace=True)
    return(data)

def steam_corpus(data: pd.DataFrame) -> pd.DataFrame:
    """Steam words from corpus"""
    logging.info("Steamming docs from corpus.")
    stemmed_docs = []
    for _, row in data.iterrows():
        tokens = get_tokens(row.description)
        stem_words = [get_stem(word) for word in tokens]
        stem_doc = ' '.join(stem_words)
        stemmed_docs.append(stem_doc)
    data['description'] = stemmed_docs
    return(data)

def pipeline(data: pd.DataFrame) -> pd.DataFrame:
    """ Apply the corpus clean pipeline."""
    logging.info("Start Clean Corpus")
    (data.pipe(add_languages)
                .pipe(remove_non_english_docs)
                .pipe(remove_columns, remove_columns=['language'])
                .pipe(clean_text_corpus)
                .pipe(remove_stop_words_corpus)
                .pipe(steam_corpus))
    return(data)

@hydra.main(version_base=None, config_path="../config", config_name="data_flow")
def clean_corpus_metadata_app(cfg : DictConfig) -> None:
    """ Clean corpus of app_metadata & save in processed data folder"""
    logging.info("Start Cleaning app_metadata corpus.")
    get_data = DataGetter()
    app_metadata = get_data.app_metadata_df
    app_metadata = pipeline(app_metadata.copy())
    app_metadata.to_csv(cfg.process_path.app_metadata)

if __name__ == "__main__":
    clean_corpus_metadata_app()
