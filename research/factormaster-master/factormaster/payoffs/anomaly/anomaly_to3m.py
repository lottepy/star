import pandas as pd
from datetime import timedelta, datetime
import numpy as np
from tqdm import tqdm
import statsmodels.api as sm
import itertools


START_DATE = pd.to_datetime('2014-02-01')
END_DATE = pd.to_datetime('2020-11-30')
SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
STOCK_LIST = pd.read_csv('data/stockpools/ASHARE_COMPONENT.csv', index_col=0).columns.tolist()

# market_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
# market_cap = market_cap.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index, STOCK_LIST]
rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
rtn = rtn.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index, STOCK_LIST]
industry = pd.read_csv('data/features/industry.csv', index_col=0, parse_dates=True)
industry = pd.concat([industry, pd.DataFrame(index=SSE_CALENDAR.loc[START_DATE: END_DATE].index)], axis=1, sort=True).ffill().loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index]
industry = pd.concat([industry, pd.DataFrame(columns=STOCK_LIST)], axis=0, sort=True).fillna(0)

FACTOR_NAME = "TO3m"
LONG_HIGH = True
SUFFIX_LIST = ['0000', '0001', '0100', '0101']

DIGIT_MAPPING = {0: {0: 'ASHARE_TRADE_COMPONENT'},
                 1: {0: "M",
                     1: "W"},
                 2: {0: "No",
                     1: "Reg",
                     2: "Group-Sort"},
                 3: {0: "EqualWeight",
                     1: "SizeWeight"}}


def _get_raw_features():
    res = {}
    res['turnover'] = pd.read_csv('data/features/turnover.csv', index_col=0, parse_dates=True).rolling(21 * 3).mean()
    res['market_cap'] = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
    res['ASHARE_TRADE_COMPONENT'] = pd.read_csv('data/stockpools/ASHARE_TRADE_COMPONENT.csv', index_col=0, parse_dates=True).fillna(0).astype(int)
    return res


def _get_exposure(formatted_data, setting):
    # cast universe mask
    universe_mask = formatted_data[setting[0]]
    universe_mask = pd.concat([universe_mask, pd.DataFrame(columns=STOCK_LIST)], axis=0, sort=True)
    universe_mask = universe_mask.replace({0: np.nan})
    exposure = formatted_data['turnover']
    exposure = universe_mask * exposure
    # handle industry neutral
    exposure = _handle_industry_neutral(exposure, setting[2])
    return exposure
    

def _handle_industry_neutral(exposure, method):
    if method == 'No':
        return exposure
    elif method == 'Reg':
        new_exposure = pd.DataFrame(index=exposure.index, columns=exposure.columns)
        exposure = exposure.replace({np.inf: np.nan, -np.inf: np.nan})
        for i in range(len(exposure)):
            date = exposure.index[i]
            one_industry = industry.loc[date]
            one_exposure = exposure.loc[date]
            if len(one_exposure[np.isnan(one_exposure)]) / len(one_exposure) > 0.9:
                continue
            ix = (one_industry != 0) & (~np.isnan(one_exposure))
            reg_industry = one_industry.loc[ix]
            reg_exposure = one_exposure.loc[ix]
            dummy = pd.get_dummies(reg_industry)
            rslt = sm.OLS(reg_exposure.values, dummy.values).fit()
            one_new_exposure = new_exposure.loc[date]
            one_new_exposure.loc[ix] = rslt.resid
            new_exposure.loc[date] = one_new_exposure
        return new_exposure
    else:
        raise NotImplementedError


def _group(exposure):
    rank = exposure.rank(axis=1, pct=True, na_option='keep')
    group = rank // (1/10)
    return group
    

def _offset_trading_day(start_day, offset):
    assert offset != 0
    union = list(sorted(set(SSE_CALENDAR.index.tolist() + [start_day])))
    ix = union.index(start_day)
    if ix + offset < 0:
        raise ValueError('Exceed TRADEDATE start day')
    if ix + offset >= len(union):
        raise ValueError('Exceed TRADEDATE end day')
    return union[ix + offset]


def _parse_suffix(suffix):
    res = []
    for i in range(len(suffix)):
        s = int(suffix[i])
        res.append(DIGIT_MAPPING[i][s])
    return res


def _format_data(df, setting):
    freq = setting[1]
    if freq in ['M', 'W']:
        rebalance_dates = pd.date_range(START_DATE, END_DATE, freq=freq)
        rebalance_dates = [_offset_trading_day(i+timedelta(days=1), -1) for i in rebalance_dates]
        rebalance_dates = [i for i in rebalance_dates if i >= START_DATE and i <= END_DATE]
        rebalance_dates = sorted(list(set(rebalance_dates)))
    else:
        raise NotImplementedError
    
    rslt = df.loc[:, STOCK_LIST]
    rslt = pd.concat([rslt, pd.DataFrame(index=rebalance_dates)], axis=1, sort=True).ffill().loc[rebalance_dates]
    return rslt
    
    
def calc_anomaly():
    data = _get_raw_features()
    payoff_list = []
    for suffix in tqdm(SUFFIX_LIST):
        one_name = '_'.join([FACTOR_NAME, suffix])
        one_setting = _parse_suffix(suffix)
        formatted_data = {k: _format_data(v, one_setting) for k,v in data.items()}
        exposure = _get_exposure(formatted_data, one_setting)
        group = _group(exposure)
        group = group.fillna(-1)
        # weights
        long_weights = pd.DataFrame(columns=group.columns, index=rtn.index)
        short_weights = pd.DataFrame(columns=group.columns, index=rtn.index)
        for i in range(len(group)):
            date = group.index[i]
            long_end = (group.loc[date].values == 9).astype(int) # 默认long high, 若LONG_HIGH==False, 在最后乘以-1
            short_end = (group.loc[date].values == 0).astype(int)
            if one_setting[3] == 'EqualWeight':
                long_end = long_end / np.sum(np.abs(long_end)) if np.sum(np.abs(long_end)) > 0 else np.zeros_like(long_end)
                short_end = short_end / np.sum(np.abs(short_end)) if np.sum(np.abs(short_end)) > 0 else np.zeros_like(short_end)
            elif one_setting[3] == 'SizeWeight':
                long_end = long_end * formatted_data['market_cap'].loc[date].values
                short_end = short_end * formatted_data['market_cap'].loc[date].values
                long_end[np.isnan(long_end)] = 0
                short_end[np.isnan(short_end)] = 0
                long_end = long_end / np.sum(np.abs(long_end)) if np.sum(np.abs(long_end)) > 0 else np.zeros_like(long_end)
                short_end = short_end / np.sum(np.abs(short_end)) if np.sum(np.abs(short_end)) > 0 else np.zeros_like(short_end)
            long_weights.loc[date] = long_end
            short_weights.loc[date] = short_end
        # payoff
        long_weights = long_weights.astype(np.float64)
        for i in range(1, len(long_weights)):
            one_weight = long_weights.iloc[i].values
            if np.isnan(one_weight).all():
                last_weight = long_weights.iloc[i-1].values
                today_rtn = rtn.iloc[i].values
                tmp = last_weight * (1 + today_rtn)
                tmp[np.isnan(tmp)] = 0
                long_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
        long_payoff = (long_weights.shift(1) * rtn).sum(axis=1).to_frame(f'{FACTOR_NAME}_{suffix}')
        short_weights = short_weights.astype(np.float64)
        for i in range(1, len(short_weights)):
            one_weight = short_weights.iloc[i].values
            if np.isnan(one_weight).all():
                last_weight = short_weights.iloc[i-1].values
                today_rtn = rtn.iloc[i].values
                tmp = last_weight * (1 + today_rtn)
                tmp[np.isnan(tmp)] = 0
                short_weights.iloc[i] = tmp / np.sum(np.abs(tmp))
        short_payoff = (short_weights.shift(1) * rtn).sum(axis=1).to_frame(f'{FACTOR_NAME}_{suffix}')
        if LONG_HIGH:
            payoff_list.append(long_payoff - short_payoff)
        else:
            payoff_list.append(short_payoff - long_payoff)
    pd.concat(payoff_list, axis=1).to_csv(f'data/payoffs/anomaly/{FACTOR_NAME}.csv')
    return

if __name__ == '__main__':
    calc_anomaly()