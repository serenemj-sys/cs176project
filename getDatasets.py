import kagglehub
import pandas as pd

def get_datasets():
    path = kagglehub.dataset_download("dansbecker/melbourne-housing-snapshot")

    df_melb = pd.read_csv(f'{path}/melb_data.csv')

    path = kagglehub.dataset_download("luvathoms/portugal-real-estate-2024")

    df_portugal = pd.read_csv(f'{path}/real_estate.csv')

    path = kagglehub.dataset_download("yellowj4acket/real-estate-california")

    df_cali = pd.read_csv(f'{path}/data.csv')

    return df_melb, df_portugal, df_cali