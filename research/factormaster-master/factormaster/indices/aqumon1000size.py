import pandas as pd
from datetime import timedelta, datetime
import numpy as np
from tqdm import tqdm
import statsmodels.api as sm
import itertools


INDEX_NAME = 'AQUMON1000_SIZE'


START_DATE = pd.to_datetime('2014-2-28')
END_DATE = pd.to_datetime('2020-10-30')
SSE_CALENDAR = pd.read_csv('data/TRADEDATE.csv', index_col=0, parse_dates=True)
UNIVERSE = pd.read_csv('data/stockpools/AQUMON1000_COMPONENT.csv', index_col=0, parse_dates=True).loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index]
STOCK_LIST = UNIVERSE.columns.tolist()
rtn = pd.read_csv('data/features/rtn.csv', index_col=0, parse_dates=True)
rtn = rtn.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index, STOCK_LIST]

mkt_cap = pd.read_csv('data/features/marketcap.csv', index_col=0, parse_dates=True)
mkt_cap = mkt_cap.loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index, STOCK_LIST]

industry = pd.read_csv('data/features/industry.csv', index_col=0, parse_dates=True)
industry = pd.concat([industry, pd.DataFrame(index=SSE_CALENDAR.loc[START_DATE: END_DATE].index)], axis=1, sort=True).ffill().loc[SSE_CALENDAR.loc[START_DATE: END_DATE].index]
industry = pd.concat([industry, pd.DataFrame(columns=STOCK_LIST)], axis=0, sort=True).fillna(0)

def _get_factors():
    factors = []
    # ==========================
    direction = -1
    industry_neutral = True
    size_neutral = False
    value = np.log(mkt_cap) * (UNIVERSE.replace({0: np.nan}))
    # --------------------------
    value = _boxplot_remove_outliers(value)
    if industry_neutral:
        value = _handle_industry_neutral(value, 'Reg')
    if size_neutral:
        value = _handle_size_neutral(value)
    value = direction * value
    factors.append(value)
    # --------------------------
    # ==========================
    
    return factors


def _factors2score(factors, rebalance_dates):
    score = pd.DataFrame(index=rebalance_dates, columns=STOCK_LIST)
    for date in rebalance_dates:
        one_score_array = np.ones(shape=(len(factors), len(STOCK_LIST))) * np.nan
        for i in range(len(factors)):
            factor = factors[i]
            one_factor = factor.loc[date]
            one_factor = (one_factor - one_factor.mean()) / one_factor.std(ddof=0)
            one_score_array[i, :] = one_factor.values
        score.loc[date] = np.mean(one_score_array, axis=0)
    return score
            
            
def _calc_weights(score):
    universe = pd.read_csv('data/stockpools/AQUMON1000_TRADE_COMPONENT.csv', index_col=0, parse_dates=True)
    universe = pd.concat([universe, pd.DataFrame(columns=STOCK_LIST)], axis=0).replace({0: np.nan})
    weights = pd.DataFrame(index=score.index, columns=STOCK_LIST)
    for date in score.index:
        universe_mask = universe.loc[date]
        one_mkt_cap = mkt_cap.loc[date]
        one_score = score.loc[date]
        one_score = one_score * universe_mask
        one_score_rank = one_score.rank(ascending=False)
        one_signal = (one_score_rank <= 200).astype(int)
        one_signal = one_mkt_cap * universe_mask * one_signal
        one_signal = one_signal.fillna(0)
        weights.loc[date] = one_signal / one_signal.sum()
    return weights


def _boxplot_remove_outliers(df):
    output_values = df.copy().values
    q3 = df.quantile(0.75, axis=1)
    q1 = df.quantile(0.25, axis=1)
    upper = q3 + 1.5 * (q3 - q1)
    lower = q1 - 1.5 * (q3 - q1)
    output_values[(output_values.T > upper.values).T] = np.nan
    output_values[(output_values.T < lower.values).T] = np.nan
    output = pd.DataFrame(output_values, index=df.index, columns=df.columns)
    return output


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


def _handle_size_neutral(exposure):
    new_exposure = pd.DataFrame(index=exposure.index, columns=exposure.columns)
    for i in range(len(exposure)):
        date = exposure.index[i]
        one_exposure = exposure.loc[date]
        if len(one_exposure[np.isnan(one_exposure)]) / len(one_exposure) > 0.9:
            continue
        one_mkt_cap = np.log(mkt_cap.loc[date])
        ix = (~np.isnan(one_mkt_cap)) & (~np.isnan(one_exposure))
        reg_mkt_cap = one_mkt_cap.loc[ix]
        reg_exposure = one_exposure.loc[ix]
        rslt = sm.OLS(reg_exposure.values, reg_mkt_cap.values).fit()
        one_new_exposure = new_exposure.loc[date]
        one_new_exposure.loc[ix] = rslt.resid
        new_exposure.loc[date] = one_new_exposure
    return new_exposure


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
    # get factors
    factors = _get_factors()
    # handle factors
    score = _factors2score(factors, rebalance_dates)
    # calc weights
    weights = _calc_weights(score)
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