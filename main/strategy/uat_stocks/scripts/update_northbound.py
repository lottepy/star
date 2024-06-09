import os
import sys
from datetime import datetime, timedelta
import time

sys.path.append(".")

import pandas as pd
import numpy as np
from strategy.commonalgo.data.choice_proxy_client import choice_client as c
from tqdm import tqdm
from retrying import retry

@retry(stop_max_attempt_number=5, wait_fixed=2000)
def _get_choice_csd(*args, **kwargs):
    return c.csd(*args, **kwargs)


@retry(stop_max_attempt_number=5, wait_fixed=2000)
def _get_choice_css(*args, **kwargs):
    return c.css(*args, **kwargs)


def update_fundamental_data():
    start_date = "2014-01-01"
    end_date = datetime.today().strftime("%Y-%m-%d")
    all_date_set = pd.date_range(start_date, end_date, freq='W')
    all_stock_set = pd.read_csv("strategy/stocks/stock_universe/aqm_cn_stock.csv", index_col=0)['SECUCODE'].tolist()
    for factor in ['LIQASHARE']:
        try:
            factor_df = pd.read_csv(f'strategy/stocks/data/northbound/{factor}.csv',
                                    index_col=0, parse_dates=True)
        except FileNotFoundError:
            factor_df = pd.DataFrame()
        for date in tqdm(all_date_set):
            date_str = date.strftime("%Y-%m-%d")
            if date in factor_df.index:
                continue
            date_df = pd.DataFrame()
            for i in range(len(all_stock_set) // 100 + 1):
                params = f'EndDate={date_str}, TradeDate={date_str}'
                data = _get_choice_css(
                    all_stock_set[i * 100: (i + 1) * 100], factor, params)
                data.columns = all_stock_set[i * 100: (i + 1) * 100]
                date_df = pd.concat([date_df, data], axis=1)
            date_df.columns = all_stock_set
            factor_df = pd.concat([factor_df, date_df], axis=0)
        factor_df.index = all_date_set
        factor_df.to_csv(f'strategy/stocks/data/northbound/{factor}.csv')


def update_market_cap():
    liqashare = pd.read_csv('strategy/stocks/data/northbound/LIQASHARE.csv', index_col=0, parse_dates=True)
    close_df = pd.read_csv('strategy/stocks/data/daily_close.csv', index_col=0, parse_dates=True)
    close_df = close_df.resample('W').last()
    common_index = [i for i in liqashare.index if i in close_df.index]
    liqashare = liqashare.loc[common_index]
    close_df = close_df.loc[common_index]
    market_cap = liqashare * close_df
    market_cap.to_csv('strategy/stocks/data/northbound/LIQMARKETCAP.csv')
    return


def update_northbound_data():
    all_stock_set = pd.read_csv("strategy/stocks/stock_universe/aqm_cn_stock.csv", index_col=0)['SECUCODE'].values
    stockconnect_df = pd.read_csv("//192.168.9.170/share/alioss/1_StockStrategy/data/filter/"
                                  "StockConnectFilter.csv",
                                  index_col=0, parse_dates=True)
    try:
        hold = pd.read_csv("strategy/stocks/data/northbound/hold.csv", index_col=0, parse_dates=True)
    except FileNotFoundError:
        hold = pd.DataFrame(columns=all_stock_set)
    try:    
        hold_percent = pd.read_csv("strategy/stocks/data/northbound/hold_percent.csv", index_col=0, parse_dates=True)
    except FileNotFoundError:
        hold_percent = pd.DataFrame(columns=all_stock_set)

    last_date = pd.to_datetime('2017-01-01') if len(hold) == 0 else hold.index[-1]
    tmp_date = [i for i in stockconnect_df.index if i<=last_date][-1]
    stockconnect_df = stockconnect_df.loc[tmp_date:]
    hold_list = [hold]
    hold_percent_list = [hold_percent]
    for i in tqdm(range(len(stockconnect_df.index))):
        date = stockconnect_df.index[i]
        start_date = date.strftime("%Y-%m-%d")
        end_date = stockconnect_df.index[i+1] if i != len(stockconnect_df)-1 else datetime.today()-timedelta(days=1)
        end_date = end_date.strftime("%Y-%m-%d")
        stocks_list = all_stock_set[stockconnect_df.loc[date].values.astype(bool)]
        n = len(stocks_list)
        hold_date_list = []
        hold_percent_date_list = []
        for i in tqdm(range(n)):
            data = _get_choice_csd(stocks_list[i],
                                   "SHAREHDNUM,SHAREHDPCT",
                                   start_date,
                                   end_date,
                                   "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
            one_hold = data.loc[:, (slice(None), 'SHAREHDNUM')]
            one_hold.columns = one_hold.columns.get_level_values(0)
            one_hold.index = pd.to_datetime(one_hold.index)
            hold_date_list.append(one_hold)
            one_hold_percent = data.loc[:, (slice(None), 'SHAREHDPCT')]
            one_hold_percent.columns = one_hold_percent.columns.get_level_values(0)
            one_hold_percent.index = pd.to_datetime(one_hold_percent.index)
            hold_percent_date_list.append(one_hold_percent)
        hold_date = pd.concat(hold_date_list, axis=1, sort=True)
        hold_percent_date = pd.concat(hold_percent_date_list, axis=1, sort=True)
        hold_list.append(hold_date)
        hold_percent_list.append(hold_percent_date)
    hold = pd.concat(hold_list, axis=0, sort=True)
    hold_percent = pd.concat(hold_percent_list, axis=0, sort=True)
    hold = hold[~hold.index.duplicated(keep='last')]
    hold_percent = hold_percent[~hold_percent.index.duplicated(keep='last')]
    hold.sort_index(inplace=True)
    hold_percent.sort_index(inplace=True)
    hold.fillna(np.nan, inplace=True)
    hold_percent.fillna(np.nan, inplace=True)
    hold.to_csv("strategy/stocks/data/northbound/hold.csv")
    hold_percent.to_csv("strategy/stocks/data/northbound/hold_percent.csv")


def NORTHBOUND_HOLD(rolling_length=20, hold_length=20, top_n=50):
    hold_percent = pd.read_csv("strategy/stocks/data/northbound/hold_percent.csv", index_col=0, parse_dates=True)
    hold_group = hold_percent.rolling(rolling_length, min_periods=1).mean().rank(axis=1, ascending=False).values
    hold_group = (hold_group <= top_n).astype(int)
    signal = pd.DataFrame(0, columns=hold_percent.columns, index=hold_percent.index)
    for i in tqdm(range(len(signal))):
        one_signal = hold_group[max(i-hold_length+1, 0):i+1, :]
        one_signal = one_signal / np.nansum(one_signal, axis=1).reshape(-1, 1)
        one_signal = np.nansum(one_signal, axis=0) / hold_length
        signal.iloc[i] = one_signal
    signal.to_csv("//192.168.9.170/share/alioss/1_StockStrategy/data/signal/NORTHBOUND_HOLD.csv")
    
    market_cap = pd.read_csv("strategy/stocks/data/northbound/LIQMARKETCAP.csv", index_col=0, parse_dates=True)
    market_cap = pd.concat([market_cap, pd.DataFrame(index=hold_percent.index)], axis=1).ffill()
    market_cap = market_cap.loc[hold_percent.index]
    market_cap = np.log(market_cap).values
    signal = pd.DataFrame(0, columns=hold_percent.columns, index=hold_percent.index)
    for i in tqdm(range(len(signal))):
        one_signal = hold_group[max(i-hold_length+1, 0):i+1, :]
        one_weight = market_cap[max(i-hold_length+1, 0):i+1, :]
        one_signal = one_signal * one_weight
        one_signal = one_signal / np.nansum(one_signal, axis=1).reshape(-1, 1)
        one_signal = np.nansum(one_signal, axis=0) / hold_length
        signal.iloc[i] = one_signal
    signal.to_csv("//192.168.9.170/share/alioss/1_StockStrategy/data/signal/NORTHBOUND_HOLD_MARKET_CAP_WEIGHT.csv")
    return


if __name__ == '__main__':
    update_fundamental_data()
    update_market_cap()
    update_northbound_data()
    NORTHBOUND_HOLD()