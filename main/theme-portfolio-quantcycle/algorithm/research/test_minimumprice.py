import pandas as pd
import numpy as np
import os
import sys

import warnings
warnings.filterwarnings('ignore')

#设置路径
read_path = r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\cn_data\trading'
os.chdir(read_path)
csv_name_list = os.listdir()

pricedata=pd.DataFrame()
symbol_list = pd.read_csv(r'/config/CN_Quality/symbol_list.csv')['symbol'].tolist()

for symbol in symbol_list:
    print(symbol)
    tmp_td_path = os.path.join(read_path, symbol + ".csv")
    datain = pd.read_csv(tmp_td_path, parse_dates=[0], index_col=0)
    price=datain.loc[:,'PX_LAST']
    pricedata=pd.concat([pricedata,price],axis=0)

sns.distplot(x)
pricedata.describe()


print(pricedata)


