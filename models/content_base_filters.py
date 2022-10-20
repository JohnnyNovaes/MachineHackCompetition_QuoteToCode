# Imports
import os
import pandas as pd
import sys
import pickle

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
sys.path.append('../src')

from load_data import DataGetter
from build_base_dataframes import apps_by_user, final_dataframe

class baseline_TFIDF():
    """ Builds a TF-IDF Map of the description of apps and """

    def __init__(self, topk:int):
        # load data getters
        self.validation_df = DataGetter(['validation_data'],
                                        data_form='raw').validation_set_df
        
        with open('../data/final/similiarities.p', 'rb') as fp:
            self.similiarities = pickle.load(fp)
        
        with open('../data/final/topk.p', 'rb') as fp:
            self.topk = pickle.load(fp)

        # build dataframes
        self.apps_by_user = apps_by_user()
        self.final_dataframe = final_dataframe(topk)
        self.tfidf_data = pd.DataFrame
        
    def recommendations(self) -> None:
        """ Recommends top 4 most similars apps of users installed apps."""

        top4_most_downloaded = [25349, 55857, 20875, 145569]
        tfidf_base_pred = {}
        for _,row in self.validation_df.iterrows():
            try:
                if len(self.topk[row.uid]) == 0:
                    tfidf_base_pred[row.uid] = top4_most_downloaded
                else:
                    tfidf_base_pred[row.uid] = self.topk[row.uid]
            except KeyError:
                tfidf_base_pred[row.uid] = top4_most_downloaded

        self.base_df = (pd.DataFrame(tfidf_base_pred)
              .melt(value_name='item_id', var_name='user_id'))
            

    def save_recommendations(self) -> None:
        self.base_df.to_csv('../predictions/baseline_tfidf.csv', index=False)


if __name__ == "__main__":
    model = baseline_TFIDF(topk=4)
    model.recommendations()
    model.save_recommendations()
