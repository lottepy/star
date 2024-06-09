from os.path import join, exists
from os import listdir, makedirs
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from algorithm.addpath import data_path
from datamaster import dm_client
from constants import *

dm_client.refresh_config()
dm_client.start()

def dm_data_downloader(underlying_type, symbol_list, start, end):
    if underlying_type == "US_STOCK":
        output_path = join(data_path, "us_data")
        market_index = "SPX Index"
        strt_idx = 4
    elif underlying_type == "HK_STOCK":
        output_path = join(data_path, "hk_data")
        market_index = "HSI Index"
        strt_idx = 2
    elif underlying_type == "CN_STOCK":
        output_path = join(data_path, "cn_data")
        market_index = "CSI300 Index"
        strt_idx = 4
    save_path_1 = join(output_path, "trading")
    if exists(save_path_1):
        pass
    else:
        makedirs(save_path_1)

    save_path_2 = join(output_path, "financials")
    if exists(save_path_2):
        pass
    else:
        makedirs(save_path_2)

    save_path_3 = join(output_path, "reference")
    if exists(save_path_3):
        pass
    else:
        makedirs(save_path_3)

    for symbol in symbol_list:
        try:
            res = dm_client.historical(symbols=symbol, start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d"),
                                       fields=','.join(list(US_TRADINGS.keys())))
            res_output = pd.DataFrame(res['values'][symbol], columns=res['fields'])
            res_output['date'] = pd.to_datetime(res_output['date'])
            res_output.rename(columns=US_TRADINGS, inplace=True)
            res_output.set_index('date', inplace=True)
        except:
            res_output = pd.DataFrame(columns=list(US_TRADINGS.values()))

        try:
            res2 = dm_client.historical(symbols=symbol, start_date=start.strftime("%Y-%m-%d"), end_date=end.strftime("%Y-%m-%d"),
                                        fields='close', adjust_type=8)
            res_output_2 = pd.DataFrame(res2['values'][symbol], columns=res2['fields'])
            res_output_2['date'] = pd.to_datetime(res_output_2['date'])
            res_output_2.rename(columns={'close': 'PX_LAST_SPLIT'}, inplace=True)
            res_output_2.set_index('date', inplace=True)
        except:
            res_output_2 = pd.DataFrame(columns=['PX_LAST_SPLIT'])

        res_output = pd.concat([res_output,res_output_2], axis=1)
        res_output.dropna(thresh=5, inplace=True)
        res_output.to_csv(join(save_path_1, symbol + ".csv"))

        try:
            res3 = dm_client.historical(symbols=symbol, start_date=start.strftime("%Y-%m-%d"),
                                       end_date=end.strftime("%Y-%m-%d"),
                                       fields=','.join(list(US_FINANCIALS.keys())))
            res_output_3 = pd.DataFrame(res3['values'][symbol], columns=res3['fields'])
            res_output_3['date'] = pd.to_datetime(res_output_3['date'])

            res_output_3.rename(columns=US_FINANCIALS, inplace=True)

            res_output_3.set_index('date', inplace=True)
            res_output_3_new = res_output_3.loc[:, ['ANNOUNCEMENT_DT', 'BS_TOT_ASSET', 'BS_TOT_LIAB2', 'TOT_EQUITY']]
            flow_var_list = []
            for i in range(strt_idx - 1, res_output_3.shape[0]):
                ub = res_output_3.index[i]
                lb = ub - timedelta(days=350)
                tmp = res_output_3[res_output_3.index <= ub]
                tmp = tmp[tmp.index > lb]
                tmp = tmp.loc[:, ['SALES_REV_TURN', 'EBIT', 'NET_OPER_INCOME', 'NET_INCOME', 'CF_CASH_FROM_OPER',
                                  'CF_CAP_EXPEND_PRPTY_ADD', 'IS_INT_EXPENSE', 'EBITDA']]
                tmp = pd.DataFrame(tmp.sum()).transpose()
                tmp.index = [ub]
                flow_var_list.append(tmp)
            flow_var = pd.concat(flow_var_list)
            res_output_3_new = pd.concat([res_output_3_new, flow_var], axis=1)
            # res_output_3.dropna(thresh=5, inplace=True)
        except:
            res_output_3_new = pd.DataFrame(columns=list(US_FINANCIALS.values()))

        res_output_3_new.to_csv(join(save_path_2, symbol + ".csv"))

    try:
        res4 = dm_client.historical(symbols=[market_index], start_date=start.strftime("%Y-%m-%d"),
                                    end_date=end.strftime("%Y-%m-%d"), fields='close')
        res_output_4 = pd.DataFrame(res4['values'][market_index], columns=res4['fields'])
        res_output_4['date'] = pd.to_datetime(res_output_4['date'])
        res_output_4.rename(columns={'close': "market_index"}, inplace=True)
        res_output_4.set_index('date', inplace=True)
        res_output_4 = res_output_4.ffill()
    except:
        res_output_4 = pd.DataFrame(columns=[market_index])

    res_output_4.to_csv(join(save_path_3, "market_index.csv"))
