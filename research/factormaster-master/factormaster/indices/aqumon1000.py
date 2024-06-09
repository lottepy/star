import pandas as pd
from datetime import timedelta, datetime
import numpy as np
from tqdm import tqdm
import statsmodels.api as sm
import itertools


INDEX_NAME = 'AQUMON1000'


START_DATE = pd.to_datetime('2011-12-30')
END_DATE = pd.to_datetime('2020-11-30')
SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
STOCK_LIST = pd.read_csv('data/stockpools/AQUMON1000_COMPONENT.csv', index_col=0).columns.tolist()
rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
rtn = rtn.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index, STOCK_LIST]

mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
mkt_cap = mkt_cap.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index, STOCK_LIST]


def _offset_trading_day(start_day, offset):
    assert offset != 0
    union = list(sorted(set(SSE_CALENDAR.index.tolist() + [start_day])))
    ix = union.index(start_day)
    if ix + offset < 0:
        raise ValueError('Exceed TRADEDATE start day')
    if ix + offset >= len(union):
        raise ValueError('Exceed TRADEDATE end day')
    return union[ix + offset]


def _weight2payoff(weights, rtn):
    all_weights = pd.DataFrame(columns=weights.columns, index=rtn.index)
    for i in weights.index:
        if i in rtn.index:
            all_weights.loc[i] = weights.loc[i]
    all_weights = all_weights.astype(np.float64)
    for i in range(1, len(all_weights)):
        one_weight = all_weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = all_weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            all_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
    payoffs = (all_weights.shift(1) * rtn).sum(axis=1).to_frame('rtn')
    return payoffs


def calc_index():
    # get all rebalance day
    rebalance_dates = pd.date_range(START_DATE, END_DATE, freq='M')
    rebalance_dates = [_offset_trading_day(i+timedelta(days=1), -1) for i in rebalance_dates]
    rebalance_dates = [i for i in rebalance_dates if i >= START_DATE and i < END_DATE]
    rebalance_dates = sorted(list(set(rebalance_dates)))
    # calc weights
    universe = pd.read_csv('data/stockpools/AQUMON1000_TRADE_COMPONENT.csv', index_col=0, parse_dates=True)
    universe = pd.concat([universe, pd.DataFrame(columns=STOCK_LIST)], axis=0).fillna(0)
    weights = pd.DataFrame(index=rebalance_dates, columns=STOCK_LIST)
    for date in rebalance_dates:
        universe_mask = universe.loc[date]
        one_mkt_cap = mkt_cap.loc[date]
        one_mkt_cap = one_mkt_cap * universe_mask
        one_mkt_cap = one_mkt_cap.fillna(0)
        weights.loc[date] = one_mkt_cap / one_mkt_cap.sum()
    # calc payoffs
    payoff = _weight2payoff(weights, rtn)
        
    # calc index value
    base_points = 1000
    index_value = base_points * (1 + payoff).cumprod()
    index_value.columns = [INDEX_NAME]
    index_value.to_csv(f'data/indices/{INDEX_NAME.lower()}.csv')
    return

if __name__ == '__main__':
    calc_index()