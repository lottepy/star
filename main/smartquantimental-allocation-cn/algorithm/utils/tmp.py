import os
from os.path import join
from datamaster import dm_client
import pandas as pd
from algorithm import addpath


ashare_config_path = join(addpath.root_path, 'config')
ashare_data_path = join(addpath.root_path, 'data')
ashare_result_path = join(addpath.root_path, 'data','Ashare','trading_data')
ticker = pd.read_csv(os.path.join(ashare_config_path, 'ticker_industry.csv'))

for i in [ticker['Stkcd'][0]]:
    print(i)
    try:
        tmp_data = pd.read_csv(os.path.join(ashare_result_path, i+'.csv'))
        tmp_data = tmp_data.drop('Unnamed: 0.1.1', 1)
        tmp_data.set_index('date', inplace=True)
        tmp_data.to_csv(join(ashare_result_path, i + '.csv'))

    except:
        continue