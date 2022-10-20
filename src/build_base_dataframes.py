import pandas as pd
import numpy as np


from load_data import DataGetter

def apps_by_user() -> pd.DataFrame:
    """ 
    Builds a dataframe with a list of apps installed
    in each mobile's user.
     """
    datagetter = DataGetter(['app_installs'], data_form='raw')
    data = datagetter.app_installs_df

    apps_per_user = data[data.status == 'installed'][['uid','item_id']].copy()
    list_apps_per_user = apps_per_user.groupby(by='uid').agg({'item_id': lambda x: list(x)})
    
    return(list_apps_per_user)

def final_dataframe(topk: int) -> pd.DataFrame:
    """
    For each user repeat the row topk times to receive the topk recommendations and
    build a item_id column.
    """
    datagetter = DataGetter(['validation_data'], data_form='raw')
    validation_df = datagetter.validation_set_df

    base_df = pd.DataFrame(np.repeat(validation_df.values, topk, axis=0),
                                     columns=validation_df.columns)

    base_df['item_id'] = np.zeros(base_df.shape[0])
    base_df['uid'] = base_df['uid'].astype(int)
    
    return(base_df)