# files management 
import hydra
from hydra import utils
from omegaconf import DictConfig, OmegaConf
import os

import pandas as pd

from logzero import logger

@hydra.main(config_path="../config", config_name="main")
def load_data(config: DictConfig) -> None:
    """Load data and build dataframes."""

    # Build dataframes
    logger.info("Loading datasets")
    
    current_path = utils.get_original_cwd()

    actual_set_path = os.path.join(current_path, config.raw_path.actual_set_path)
    actual_set_df = pd.read_csv(actual_set_path)

    app_installs_path = os.path.join(current_path, config.raw_path.app_installs_path)
    app_installs_df = pd.read_csv(app_installs_path)

    app_metadata_path = os.path.join(current_path, config.raw_path.app_metadata_path)
    app_metadata_df = pd.read_csv(app_metadata_path)

    app_usage_path = os.path.join(current_path, config.raw_path.app_usage_path)
    app_usage_df = pd.read_csv(app_usage_path)

    user_metadata_path = os.path.join(current_path, config.raw_path.user_metadata_path)
    user_metadata_df = pd.read_csv(user_metadata_path)

    logger.info("Loading datasets finished successfully!")
    
def join_data() -> None:
    """Join dataframes by User ID."""
    pass

if __name__ == "__main__":
    load_data() 
