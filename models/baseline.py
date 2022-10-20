# Imports
import hydra
from omegaconf import DictConfig, OmegaConf
import os
import pandas as pd
import logging
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from src.load_data import DataGetter
import numpy as np

class Baseline():
    '''
    Recommends top 4 most used apps in the store for all users.
    '''
    def __init__(self):
        # load data
        data_getter = DataGetter(data_to_load=['actual_set','validation_data'], data_form='raw')

        # get only needed data
        self.actual_set = data_getter.actual_set_df
        self.validation_set_df = data_getter.validation_set_df
        
        # run pipeline
        self.get_predictions()

    def calculate_top4_apps(self) -> None:
        """
        Get top 4 most downloaded apps in the store.
        """
        self.top4 = self.actual_set.item_id.value_counts(normalize=True).index.tolist()[0:4]

    def process_dataframe(self) -> None:
        """
        For each user repeat the row 4 times to receive the 4 recommendations and
        build a item_id column.
        """
        self.baseline_df = pd.DataFrame(np.repeat(self.validation_set_df.values, 4, axis=0),
                                                  columns=self.validation_set_df.columns)
        self.baseline_df['item_id'] = np.zeros(self.baseline_df.shape[0])

    def process_top4(self) -> None:
        """
        Repeat the list of 4 recommendations to match the size of baseline_df.
        """
        self.top4 = np.resize(self.top4, self.validation_set_df.shape[0]*4)

    def recommend_top4_for_users(self) -> None:
        """
        Recommend for each user the top 4 apps.
        """
        self.baseline_df.item_id = self.top4

    def get_predictions(self) -> None:
        '''
        Baseline pipeline.
        '''
        self.calculate_top4_apps()
        self.process_dataframe()
        self.process_top4()
        self.recommend_top4_for_users()

    def save_predictions(self) -> None:
        self.baseline_df.to_csv('../predictions/baseline.csv', index=False)

if __name__ == "__main__":
    baseline_model = Baseline()
    baseline_model.save_predictions()