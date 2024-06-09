import pandas as pd
import numpy as np
import os
from datetime import timedelta
import time
from tqdm import tqdm


SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
START_DATE = pd.to_datetime('2010-01-01')
END_DATE = pd.to_datetime('2020-10-31')


def _concat_by_col(df_list):
    df = pd.concat(df_list, axis=1, sort=True)
    return df


def generate_aqumon1000():
    ashare = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0, parse_dates=True)
    
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    mkt_cap = mkt_cap.loc[ashare.index, ashare.columns]
    mkt_cap = mkt_cap * ashare.replace({0: np.nan})
    mkt_cap_rank = mkt_cap.rank(ascending=False, axis=1)
    
    aqumon1000 = ((mkt_cap_rank <= 1000) & (mkt_cap_rank > -1)).astype(int)
    aqumon1000 = aqumon1000.loc[:, aqumon1000.columns[aqumon1000.sum(axis=0).astype(bool)]]
    
    aqumon1000.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index].to_csv('data/stockpools/AQUMON1000_COMPONENT.csv')
    
    df_list = []
    for name in tqdm(aqumon1000.columns):
        try:
            df = pd.read_csv(f'data/features/raw/volume/{name}.csv', index_col=0, parse_dates=True)
            df.columns = [name]
        except:
            df = pd.DataFrame(columns=[name])
        df_list.append(df)
    volume = _concat_by_col(df_list)
    volume = pd.concat([volume, pd.DataFrame(index=aqumon1000.index)], axis=1)
    volume.sort_index(inplace=True)
    volume = volume.loc[~volume.index.duplicated(keep='first')]
    volume = volume.fillna(0)
    aqumon1000_trade = aqumon1000 * volume.astype(bool).astype(int)
    
    df_list = []
    for name in tqdm(aqumon1000.columns):
        try:
            df = pd.read_csv(f'data/features/raw/adjust_ohlc/{name}.csv', index_col=0, parse_dates=True)
            # df = pd.read_csv(f'data/features/raw/ohlc/{name}.csv', index_col=0, parse_dates=True)
        except:
            df = pd.DataFrame(columns=['adjust_open', 'adjust_high', 'adjust_low', 'adjust_close'])
            # df = pd.DataFrame(columns=['open', 'high', 'low', 'close'])
        df['zero_amplitude'] = (df.max(axis=1) == df.min(axis=1))
        df['rtn'] = df['adjust_close'].pct_change()
        # df['rtn'] = df['close'].pct_change()
        df['bs_limit'] = (df['rtn'] > 0.095) | (df['rtn'] < -0.095)
        df_list.append((df['zero_amplitude'].astype(bool) & df['bs_limit'].astype(bool)).to_frame(name))
    bslimit = _concat_by_col(df_list)
    bslimit = pd.concat([bslimit, pd.DataFrame(index=aqumon1000.index)], axis=1)
    bslimit.sort_index(inplace=True)
    bslimit = bslimit.loc[~bslimit.index.duplicated(keep='first')]
    bslimit = bslimit.fillna(0)
    bslimit = (~bslimit.astype(bool)).astype(int)
    aqumon1000_trade = aqumon1000_trade * bslimit
        
    aqumon1000_trade.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index].to_csv('data/stockpools/AQUMON1000_TRADE_COMPONENT.csv')
    
    
if __name__ == '__main__':
    generate_aqumon1000()