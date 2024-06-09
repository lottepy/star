import pandas as pd
from datetime import timedelta, datetime
import numpy as np
from tqdm import tqdm


START_DATE = pd.to_datetime('2010-01-01')
END_DATE = pd.to_datetime('2020-11-30')
SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
STOCK_LIST = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0).columns.tolist()



def _align_df(df, columns, index):
    df = df.loc[index, columns]
    return df


def _offset_trading_day(start_day, offset):
    assert offset != 0
    union = list(sorted(set(SSE_CALENDAR.index.tolist() + [start_day])))
    ix = union.index(start_day)
    if ix + offset < 0:
        raise ValueError('Exceed TRADEDATE start day')
    if ix + offset >= len(union):
        raise ValueError('Exceed TRADEDATE end day')
    return union[ix + offset]


def _get_rebalance_dates(start_date, end_date, freq):
    if freq == 'Y':
        rebalance_dates = [datetime(i, 5, 1) for i in range(start_date.year, end_date.year+1)]
        rebalance_dates = [_offset_trading_day(i, -1) for i in rebalance_dates]
        rebalance_dates = [i for i in rebalance_dates if i >= START_DATE and i <= END_DATE]
        return rebalance_dates
    else:
        assert freq in ['M', 'W']
        rebalance_dates = pd.date_range(START_DATE, END_DATE, freq=freq)
        rebalance_dates = [_offset_trading_day(i+timedelta(days=1), -1) for i in rebalance_dates]
        rebalance_dates = [i for i in rebalance_dates if i >= START_DATE and i <= END_DATE]
        return rebalance_dates
        

def classic_capm():
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    riskfreerate = pd.read_csv('data/features/riskfreerate.csv', index_col=0, parse_dates=True)
    components = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0, parse_dates=True)
    
    columns = components.columns.tolist()
    index = SSE_CALENDAR.loc[START_DATE: END_DATE].index.tolist()
    
    mkt_cap = _align_df(mkt_cap, columns, index)
    rtn = _align_df(rtn, columns, index)
    riskfreerate = _align_df(riskfreerate, ['RF'], index)
    components = _align_df(components, columns, index)
    
    rebalance_dates = _get_rebalance_dates(START_DATE, END_DATE, 'Y')
    weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_weight = one_component * one_mkt_cap
        one_weight[np.isnan(one_weight)] = 0
        one_weight = one_weight / np.sum(np.abs(one_weight))
        weights.loc[date] = one_weight
    
    weights = weights.astype(np.float64)
    for i in tqdm(range(1, len(index))):
        one_weight = weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            weights.iloc[i] = tmp / np.sum(np.abs(tmp))
        
    mkt_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('MKT')
    payoff_df = pd.concat([riskfreerate, mkt_rtn], axis=1)
    payoff_df.to_csv('data/payoffs/pricing_factors/CAPM/classic_capm.csv')
    return


def simplified_capm():
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    riskfreerate = pd.read_csv('data/features/riskfreerate.csv', index_col=0, parse_dates=True)
    components = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0, parse_dates=True)
    
    columns = components.columns.tolist()
    index = SSE_CALENDAR.loc[START_DATE: END_DATE].index.tolist()
    
    mkt_cap = _align_df(mkt_cap, columns, index)
    rtn = _align_df(rtn, columns, index)
    riskfreerate = _align_df(riskfreerate, ['RF'], index)
    components = _align_df(components, columns, index)
    
    rebalance_dates = _get_rebalance_dates(START_DATE, END_DATE, 'M')
    weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_weight = one_component * one_mkt_cap
        one_weight[np.isnan(one_weight)] = 0
        one_weight = one_weight / np.sum(np.abs(one_weight))
        weights.loc[date] = one_weight
    
    weights = weights.astype(np.float64)
    for i in tqdm(range(1, len(index))):
        one_weight = weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            weights.iloc[i] = tmp / np.sum(np.abs(tmp))
        
    mkt_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('MKT')
    payoff_df = pd.concat([riskfreerate, mkt_rtn], axis=1)
    payoff_df.to_csv('data/payoffs/pricing_factors/CAPM/simplified_capm.csv')
    return


def weekly_capm():
    mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
    riskfreerate = pd.read_csv('data/features/riskfreerate.csv', index_col=0, parse_dates=True)
    components = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0, parse_dates=True)
    
    columns = components.columns.tolist()
    index = SSE_CALENDAR.loc[START_DATE: END_DATE].index.tolist()
    
    mkt_cap = _align_df(mkt_cap, columns, index)
    rtn = _align_df(rtn, columns, index)
    riskfreerate = _align_df(riskfreerate, ['RF'], index)
    components = _align_df(components, columns, index)
    
    rebalance_dates = _get_rebalance_dates(START_DATE, END_DATE, 'W')
    weights = pd.DataFrame(index=index, columns=columns)
    for date in tqdm(rebalance_dates):
        one_component = components.loc[date].values
        one_mkt_cap = mkt_cap.loc[date].values
        one_weight = one_component * one_mkt_cap
        one_weight[np.isnan(one_weight)] = 0
        one_weight = one_weight / np.sum(np.abs(one_weight))
        weights.loc[date] = one_weight
    
    weights = weights.astype(np.float64)
    for i in tqdm(range(1, len(index))):
        one_weight = weights.iloc[i].values
        if np.isnan(one_weight).all():
            last_weight = weights.iloc[i-1].values
            today_rtn = rtn.iloc[i].values
            tmp = last_weight * (1 + today_rtn)
            tmp[np.isnan(tmp)] = 0
            weights.iloc[i] = tmp / np.sum(np.abs(tmp))
        
    mkt_rtn = (weights.shift(1) * rtn).sum(axis=1).to_frame('MKT')
    payoff_df = pd.concat([riskfreerate, mkt_rtn], axis=1)
    payoff_df.to_csv('data/payoffs/pricing_factors/CAPM/weekly_capm.csv')
    return


if __name__ == '__main__':
    classic_capm()
    simplified_capm()
    weekly_capm()