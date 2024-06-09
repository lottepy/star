import pandas as pd
import numpy as np
import os
import math
from algorithm import addpath
from constants import *
import datetime
from algorithm.utils.helpers import cal_rebalancing_dates
from algorithm.utils.portfolio_config_loader import portfolio_config_loader
import ast

def cul_turnover(rebalance_freq,start, end,portfolio_name,task,pv,cash):
    data = pd.read_csv(os.path.join(addpath.result_path,task, portfolio_name,'0','position.csv'),index_col=1,parse_dates=[1])
    data=data.drop(columns=['Unnamed: 0'])
    data_q = data.resample(reb_freq[rebalance_freq]).last()
    # rebalancing_dates = cal_rebalancing_dates(start, end, rebalance_freq)
    # data.index=rebalancing_dates
    pv=pv.resample(reb_freq[rebalance_freq]).last()

    data_abs = abs(data_q - data_q.shift())
    data_abs.iloc[0,:]=data_q.iloc[0,:]

    symbol_list=data_abs.columns
    if task=='backtesting':
        input_path=os.path.join(addpath.data_path,"strategy_temps",portfolio_name,"daily_data")
    else:
        input_path=os.path.join(addpath.data_path,"tracking_weight",portfolio_name,"daily_data")

    turnover=0
    for symbol in symbol_list:
        # print(symbol)
        tmp_td_path = os.path.join(input_path, symbol + ".csv")
        trading_data = pd.read_csv(tmp_td_path, parse_dates=[0], index_col=0)
        close = trading_data.loc[start:end, 'close']
        close=close.resample(reb_freq[rebalance_freq]).last()
        close_fix = pd.DataFrame(index=pv.index)
        close_fix['close']=close
        close_fix=close_fix.fillna(0)

        sum_symbol = sum(close_fix['close'].values * data_abs[symbol].values/pv[portfolio_name].values/cash)
        turnover = turnover + sum_symbol
        # print(symbol)
        # print(turnover)

    print('all period turnover:'+str(turnover))
    delta=end-start
    delta_year=delta.days/365
    print('annual turnover:'+str(turnover /delta_year))
