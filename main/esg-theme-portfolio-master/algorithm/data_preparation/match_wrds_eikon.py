import pandas as pd
import numpy as np

from algorithm import addpath

from os.path import join 
from os import listdir

def map_cusip_cik_to_theme_ticker():
    wrds_mapping_file = join(addpath.data_path, 'theme_mapping.csv')
    wrds_mapping = pd.read_csv(wrds_mapping_file, index_col=[0])

    instruments = listdir(join(addpath.data_path, 'us_data', 'trading'))

    instrument_data_list = []
    for instrument in instruments:
        instrument_data = pd.read_csv(join(addpath.data_path, 'us_data', 'trading', instrument), nrows=1, usecols=['GVKEY', 'Ticker'])
        instrument_data_list.append(instrument_data)
        
    instrument_data_df = pd.concat(instrument_data_list)
    instrument_data_df.columns = ['GVKEY', 'Theme Ticker']

    mapped_instrument_data_df = instrument_data_df.merge(wrds_mapping, how='left', left_on='GVKEY', right_index=True)
    mapped_instrument_data_df.to_csv(join(addpath.data_path, 'us_data', 'theme_instruments.csv'))

def check_eikon():
    theme_mapped_from_eikon = pd.read_csv(join(addpath.data_path, 'eikon', 'theme_mapped_from_eikon.csv'))

    mapped_instrument_data_df.merge(eikon_mapping, how='left', left_on='cusip', right_on='CUSIP')


if __name__ == "__main__":
    map_cusip_cik_to_theme_ticker()
    refinitiv_esg = pd.read_csv(join(addpath.data_path, 'eikon', 'refinitiv_esg.csv'))
    refinitiv_esg['Date'] = pd.to_datetime(pd.to_datetime(refinitiv_esg['Date']).dt.date)
    refinitiv_esg