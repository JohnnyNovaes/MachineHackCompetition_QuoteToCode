# files management 
from hydra import compose, initialize, core
import os
import pandas as pd
import logging

class DataGetter():
    """Load data and build dataframes."""
    
    def __init__(self,data_to_load: list[str], data_form='raw'):
        # Build dataframes
        logging.info(f"Loading {data_to_load} {data_form} datasets")
        
        core.global_hydra.GlobalHydra.instance().clear()
        initialize(version_base=None, config_path="../config", job_name="DataGetter")
        cfg = compose(config_name="data_flow")
        
        data_form_dict = {'raw': cfg.raw_path,
                          'process': cfg.process_path,
                          'final': cfg.final_path }

        cfg = data_form_dict[data_form]

        current_path = os.getcwd()

        if 'actual_set' in data_to_load:
            actual_set_path = os.path.join(current_path, cfg.actual_set)
            self.actual_set_df = pd.read_csv(actual_set_path)
        
        if 'app_installs' in data_to_load:
            app_installs_path = os.path.join(current_path, cfg.app_installs)
            self.app_installs_df = pd.read_csv(app_installs_path)

        if 'app_metadata' in data_to_load:
            app_metadata_path = os.path.join(current_path, cfg.app_metadata)
            self.app_metadata_df = pd.read_csv(app_metadata_path)

        if 'app_usage' in data_to_load:
            app_usage_path = os.path.join(current_path, cfg.app_usage)
            self.app_usage_df = pd.read_csv(app_usage_path)

        if 'validation_data' in data_to_load:
            validation_set_path = os.path.join(current_path, cfg.validation_data)
            self.validation_set_df = pd.read_csv(validation_set_path)

        if 'sample_submission' in data_to_load:
            sample_submission_path = os.path.join(current_path, cfg.sample_submission)
            self.sample_submission_df = pd.read_csv(sample_submission_path)

        logging.info(f"Loading {data_to_load} {data_form} datasets finished successfully!")