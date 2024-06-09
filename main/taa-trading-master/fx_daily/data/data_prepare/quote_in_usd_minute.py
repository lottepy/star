# # 将分钟级货币对汇率转换成对美元汇率


import pandas as pd
from copy import copy
from datetime import datetime
import os


data_path = "//192.168.9.170/share/algodata/0_Projects/Proj_FX/bundle_data/min_bundle_5year/minute"
target_path = "//192.168.9.170/share/alioss/0_DymonFx/parse_data"

all_files = os.listdir(data_path)
all_currency_pair = [file.split('.')[0] for file in all_files]

#1.先把所有货币对merge在一起
if 'res' in locals():
    del res
    
for currency_pair_file in all_files:
    currency_pair_path = os.path.join(data_path,currency_pair_file)
    currency_pair_name = currency_pair_file.split('.')[0]
    currency_pair = pd.read_csv(currency_pair_path, index_col=0, parse_dates=True)
    currency_pair = currency_pair[['close']]
    currency_pair.rename(columns={'close': f'close_{currency_pair_name}'}, inplace=True)
    if 'res' not in locals():
        res = currency_pair
    else:
        res = res.join(currency_pair, how='outer')
res.ffill(inplace=True)
all_ccp_close = res


#2.算出货币对美元的汇率
all_currency_pair = [file.split('.')[0] for file in all_files]
left_all_currency  = [ccp[:3] for ccp in all_currency_pair]
right_all_currency = [ccp[3:] for ccp in all_currency_pair]
all_currency = list(set(left_all_currency + right_all_currency))

# 用时6s左右
all_currency_pair = [file.split('.')[0] for file in all_files]
ccy_usd = pd.DataFrame()
while len(all_currency_pair) > 0:
    all_ccp = copy(all_currency_pair)
    for ccp in all_ccp:
        statu = 0
        if ccp[:3] == 'USD':
            ccy = ccp[3:]
            ccy_usd[ccy] = all_ccp_close[f'close_{ccp}'].apply(lambda x: 1/x)
            statu = 1
        elif ccp[3:] == 'USD':
            ccy = ccp[:3]
            ccy_usd[ccy] = all_ccp_close[f'close_{ccp}']
            statu = 1
        elif ccp[:3] in ccy_usd.columns:
            ccy = ccp[3:]
            A_B = all_ccp_close[f'close_{ccp}']
            A_USD = ccy_usd[ccp[:3]]
            B_USD = A_USD/A_B
            ccy_usd[ccy] = B_USD
            statu = 1
        elif ccp[3:] in ccy_usd.columns:
            ccy = ccp[:3]
            A_B = all_ccp_close[f'close_{ccp}']
            B_USD = ccy_usd[ccp[3:]]
            A_USD = A_B * A_USD
            ccy_usd[ccy] = A_USD
            statu = 1
        if statu == 1:
            #print(ccp)
            #print(ccy)
            all_currency_pair.remove(ccp)
ccy_usd['USD'] = 1

ccy_usd.to_csv(os.path.join(target_path,'FX_AGAINST_USD_PRICE_MINUTE.csv'))


