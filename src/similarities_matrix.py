import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from tqdm import tqdm
from load_data import DataGetter
import pickle

def similarities_matrix(tfidf_data: pd.DataFrame) -> None:
    """
        Process and builds cosine similarities dataframe between chuncks of apps ids for
        memory optimization.
            inputs: 
                    tfidf_data: TF-IDF map of apps text description. 
    """ 
    store = pd.HDFStore('../data/raw/similiarities.h5')
    last_idx = 0
    for idx in tqdm(range(1000, tfidf_data.shape[0], 1000)):
       temp_data = tfidf_data.dot(tfidf_data.iloc[last_idx:idx].transpose())
       store.append(f'val_{int(idx/1000)}', temp_data)
       last_idx = idx
       del temp_data

    #last apps in the range
    temp_data = tfidf_data.dot(tfidf_data.iloc[last_idx:tfidf_data.shape[0]].transpose())
    store.append('val_final', temp_data)
    store.close()

def tfidf_map(data: pd.DataFrame, column: str) -> pd.DataFrame:
    """TF-IDF of corpus."""

    # build TF-IDF sparse matrix.
    tfidf_vector = TfidfVectorizer(ngram_range=(1,1),
                                   min_df=0.005,
                                   use_idf=True)                                   
    tf_idf_data = tfidf_vector.fit_transform(data[column])
    
    # build dataframe of TF-IDF Map
    data = pd.DataFrame(tf_idf_data.toarray(),
                        columns = tfidf_vector.get_feature_names_out(),
                        index=data.item_id) 
    return(data)

def get_top10_similiarities_from_h5_store() -> None:
    """Gets top 10 most similars apps for each app."""

    store = pd.HDFStore('../data/raw/similiarities.h5')
    similiarities = {}
    for key in tqdm(store.keys()):
        df = store.get(key=key)
        for column in df.columns:
            similiarities[column] = df[column].nlargest(11).iloc[1:10]

    with open('../data/final/similiarities.p', 'wb') as fp:
        pickle.dump(similiarities, fp)

def main() -> None:
    """Builds the pipeline of Similarities Matrix."""
    
    app_metadata = DataGetter(['app_metadata'], data_form='process').app_metadata_df
    tfidf_data = tfidf_map(app_metadata, 'description')
    similarities_matrix(tfidf_data)
    get_top10_similiarities_from_h5_store()

if __name__ == "__main__":
    main()
