# files management 
import hydra
from hydra import compose, initialize, utils
from omegaconf import OmegaConf
import os
import pandas as pd
import logging

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

        user_metadata_path = os.path.join(current_path, cfg.raw_path.user_metadata)
        self.user_metadata_df = pd.read_csv(user_metadata_path)

        sample_submission_path = os.path.join(current_path, cfg.raw_path.sample_submission)
        self.sample_submission_df = pd.read_csv(sample_submission_path)

        logging.info("Loading datasets finished successfully!")