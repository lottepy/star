import numpy as np
from matplotlib import pyplot as plt
from statsmodels.tsa.vector_ar.vecm import *
from algorithm.addpath import config_path, data_path
import pandas as pd
from os import makedirs
from os.path import exists, join
from datetime import datetime
from sklearn.preprocessing import StandardScaler

def std_scaler(sample_in):
    sample_out = pd.DataFrame(StandardScaler().fit_transform(sample_in.values), columns=sample_in.columns, index=sample_in.index)
    return sample_out.iloc[-1, :]

nav_path = join(data_path, 'bundles', 'daily')
ticker_path = join(data_path, 'bundles', 'Summary.csv')
return_path = join(data_path, 'returns')

hrz = 1

ms_secids = pd.read_csv(ticker_path)
ms_secids = ms_secids['Name'].tolist()
nav_list = []
for ms_secid in ms_secids:
    try:
        nav_tmp_path = join(nav_path, str(ms_secid) + ".csv")
        nav_tmp = pd.read_csv(nav_tmp_path, parse_dates=[0], index_col=0)
        nav_tmp = nav_tmp[['close']].rename(columns={'close': ms_secid})
        nav_tmp = nav_tmp.resample('1M').last().ffill()
        nav_list.append(nav_tmp)
    except:
        print("Fail for " + str(ms_secid))
nav = pd.concat(nav_list, axis=1)
ret_tmp = nav / nav.shift(hrz) - 1

start_date = datetime.datetime.strptime('2005-01-31', '%Y-%m-%d')
end_date = datetime.datetime.strptime('2020-02-29', '%Y-%m-%d')
dates = pd.date_range(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'), freq='M')

category_path = join(data_path, 'categorization', 'fund_category')

category_return_list = []

for date in dates:
    category_df = pd.read_csv(join(category_path, date.strftime("%Y-%m-%d") + '.csv'), index_col=0)
    ret = ret_tmp.loc[date.strftime('%Y-%m-%d'), :]
    category_df['returns'] = ret
    category_return_tmp = pd.DataFrame(category_df.groupby('category').returns.mean()).transpose()
    category_return_tmp.index = [date]
    category_return_list.append(category_return_tmp)
category_return = pd.concat(category_return_list)
category_return.loc[datetime.datetime.strptime('2004-12-31', '%Y-%m-%d'), :] = [0, 0, 0, 0, 0, 0]
category_return = category_return.sort_index()

return_path = join(data_path, 'returns', 'category_fwd_return_mean.csv')
ret = pd.read_csv(return_path, parse_dates=[0], index_col=0)

['Balance', 'Bond', 'Equity_Large', 'Equity_Mid', 'Money', 'QD']
category = 'Equity_Large'
tmp = category_return[[category]]
tmp['gross_return'] = tmp[category] + 1
tmp['pv'] = tmp['gross_return'].cumprod()
for i in range(60):
    tmp['cr' + str(i + 1)] = tmp['pv'] / tmp['pv'].shift(i + 1) - 1
tmp['fwd_return'] = ret[category]

tmp2 = tmp.drop(columns=[category, 'gross_return', 'pv', 'fwd_return'])
tmp3 = tmp.drop(columns=[category, 'gross_return', 'pv', 'fwd_return'])
for idx in range(12, len(tmp2.index)):
    tmp3.iloc[idx, :] = std_scaler(tmp2.iloc[max(0, idx - 60): idx + 1, :])
tmp3['fwd_return'] = ret[category]

signals = {
    'Balance' : ['c12', 2],
    'Bond' : 'None',
    'Equity_Large' : ['c12', 2],
    'Equity_Mid' : ['c12', 1],
    'Money' : '',
    'QD' : ''
}