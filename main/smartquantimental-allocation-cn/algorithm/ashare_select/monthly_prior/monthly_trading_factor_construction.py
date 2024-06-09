import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
from scipy.stats import skew
import calendar
import statsmodels.api as sm
from algorithm import addpath
import multiprocessing


# Factor construction

def ps_liq(insample):
    temp_ps_liq = insample.loc[:, ['ex_ret_f1', 'ret_daily', 'signed_dvol']]
    temp_ps_liq = temp_ps_liq.dropna()
    temp_ps_liq = temp_ps_liq[~temp_ps_liq.isin([np.nan, np.inf, -np.inf]).any(1)]
    if temp_ps_liq.shape[0] > 10:
        X = sm.add_constant(temp_ps_liq.loc[:, ['ret_daily', 'signed_dvol']])
        Y = temp_ps_liq['ex_ret_f1']
        model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
        ps_liq = model.params.iloc[2] * 1000000000

        return ps_liq


def rsj(insample):
    insample = insample['ret_daily']
    demean_data = insample - insample.mean()
    pos_data = demean_data[demean_data > 0]
    pos_count = pos_data.count()
    pos_var = (pos_data ** 2).sum() / (pos_count - 1)
    neg_data = demean_data[demean_data < 0]
    neg_count = neg_data.count()
    neg_var = (neg_data ** 2).sum() / (neg_count - 1)
    rsj = pos_var - neg_var
    return rsj


def coskewness(insample):
    temp_coskewness = insample.loc[:, ['ret_daily', 'market_index']]
    temp_coskewness = temp_coskewness.dropna()
    temp_coskewness = temp_coskewness[~temp_coskewness.isin([np.nan, np.inf, -np.inf]).any(1)]
    temp_coskewness['m_sq'] = temp_coskewness['market_index'].map(lambda x: x ** 2)
    if temp_coskewness.shape[0] > 10:
        X = sm.add_constant(temp_coskewness.loc[:, ['market_index', 'm_sq']])
        Y = temp_coskewness['ret_daily']
        model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
        coskew = model.params.iloc[2]

        return coskew


def idiosyn(insample):
    temp_idiosyn = insample.loc[:, ['ret_daily', 'market_index']]
    temp_idiosyn = temp_idiosyn.dropna()
    temp_idiosyn = temp_idiosyn[~temp_idiosyn.isin([np.nan, np.inf, -np.inf]).any(1)]
    if temp_idiosyn.shape[0] > 10:
        X = sm.add_constant(temp_idiosyn.loc[:, 'market_index'])
        Y = temp_idiosyn['ret_daily']
        Y_mean = temp_idiosyn['ret_daily'].mean()
        lows = Y[Y < Y_mean]
        semivar = np.sum((lows - Y_mean) ** 2) / len(lows)
        model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
        beta = model.params.iloc[1]
        residual = model.resid
        ivol = np.std(residual)
        skewness = skew(residual)
        output = pd.DataFrame([ivol, skewness, beta, semivar]).transpose()
        output.columns = ['ivol', 'skew', 'beta', 'semivar']
        return output


def cal_factors(symbol):
    trading_data_path = os.path.join(addpath.data_path, "Ashare", "trading_data", symbol + ".csv")

    # Mkt factor
    mkt_dt = pd.read_csv(os.path.join(addpath.data_path, "Ashare", "monthly_prior", "market_index.csv"), parse_dates=[0], index_col=0)
    mkt_dt["market_index"] = mkt_dt["SHSZ300 Index"].pct_change()
    mkt_dt = mkt_dt.fillna(0)

    temp = pd.read_csv(trading_data_path, parse_dates=[0], index_col=0)
    temp['symbol'] = symbol
    temp['Date'] = temp.index
    temp['YYYY'] = temp['Date'].map(lambda x: x.year)
    temp['MM'] = temp['Date'].map(lambda x: x.month)
    temp = temp.fillna(method='ffill')
    temp['ret_daily'] = temp['PX_LAST'].pct_change()
    temp['EQY_SH_OUT'] = temp['MARKET_CAP'] / temp['PX_LAST_RAW']
    temp['TURN'] = temp['PX_VOLUME'] / temp['EQY_SH_OUT']
    # Liquidity Measures
    # Turnovers
    result = temp.loc[:, ['symbol', 'YYYY', 'MM']].resample('1m').last()
    result['TO_1M'] = temp['TURN'].resample('1m').mean()
    result['TO_1M'] = result['TO_1M'].ffill()
    result['TO_3M'] = result['TO_1M'].rolling(window=3, min_periods=1).mean()
    result['TO_6M'] = result['TO_1M'].rolling(window=6, min_periods=3).mean()
    result['TO_12M'] = result['TO_1M'].rolling(window=12, min_periods=6).mean()
    result['Abnm_TO'] = result['TO_1M'] - result['TO_12M'].shift()
    result['LNTO_1M'] = result['TO_1M'].map(lambda x: np.log(x))
    # Amihud Illiquidity and BPST Illiquidity Shock
    temp['ILLIQ'] = temp['ret_daily'].map(lambda x: abs(x)) / (temp['PX_VOLUME'] * temp['PX_LAST_RAW']) * 1000000
    result['ILLIQ'] = temp['ILLIQ'].resample('1m').mean().ffill()
    result['LIQU'] = result['ILLIQ'] - result['ILLIQ'].rolling(window=12, min_periods=6).apply(np.nanmean).shift()
    # Pastor and Stambaugh liquidity
    ps_raw = pd.merge(temp, mkt_dt[['market_index']], left_index=True, right_index=True)
    ps_raw['ex_ret'] = ps_raw['ret_daily'] - ps_raw['market_index']
    ps_raw['ex_ret_f1'] = ps_raw['ex_ret'].shift(-1)
    ps_raw['signed_dvol'] = ps_raw['ex_ret'].map(lambda x: np.sign(x)) * ps_raw['PX_VOLUME'] * ps_raw['PX_LAST_RAW']
    ps_liq_dt = ps_raw.groupby(['YYYY', 'MM']).apply(ps_liq)
    if ps_liq_dt.shape[0] > 0:
        ps_liq_dt = pd.DataFrame(ps_liq_dt, columns=['ps_liq'])
        ps_liq_dt = ps_liq_dt.reset_index()
        ps_liq_dt['YYYYMM'] = ps_liq_dt['YYYY'] * 100 + ps_liq_dt['MM']
        ps_liq_dt['Date'] = ps_liq_dt['YYYYMM'].map(lambda x: datetime(int(x / 100), x - int(x / 100) * 100, 1))
        ps_liq_dt = ps_liq_dt.set_index('Date')
        ps_liq_dt = ps_liq_dt.resample('1m').last()
        ps_liq_dt = ps_liq_dt[['ps_liq']]
        result = pd.merge(left=result, right=ps_liq_dt, left_index=True, right_index=True, how='outer')
    else:
        result['ps_liq'] = np.nan

    # Monthly Price, Marketcap, and Volume
    ADJCLOSE = temp['PX_LAST'].fillna(method='ffill')
    result['EQY_SH_OUT'] = temp['EQY_SH_OUT'].resample('1m').last()
    result['EQY_SH_OUT'] = result['EQY_SH_OUT'].ffill()
    result['VOLUME'] = temp['PX_VOLUME'].resample('1m').sum()
    result['VOLUME'] = result['VOLUME'].ffill()
    result['CLOSE'] = temp['PX_LAST_RAW'].resample('1m').last()
    result['CLOSE'] = result['CLOSE'].ffill()
    result['ADJCLOSE'] = ADJCLOSE.resample('1m').last()
    result['ADJCLOSE'] = result['ADJCLOSE'].ffill()
    result['MARKET_CAP'] = temp['MARKET_CAP'].resample('1m').last()

    result['ret_forward'] = result['ADJCLOSE'].shift(-1) / result['ADJCLOSE'] - 1

    # Reversal and Momentum (Simple and Path-dependent)
    # Simple Reversal and Momentum
    result['REVS_1M'] = result['ADJCLOSE'].pct_change()
    result['REVS_2M'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(2) - 1
    result['REVS_3M'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(3) - 1
    result['REVS10'] = (ADJCLOSE / ADJCLOSE.shift(10) - 1).resample('1M').fillna(method='ffill')
    result['REVS20'] = (ADJCLOSE / ADJCLOSE.shift(20) - 1).resample('1M').fillna(method='ffill')
    result['Momentum_2_7'] = result['ADJCLOSE'].shift(1) / result['ADJCLOSE'].shift(7) - 1
    result['Momentum_2_12'] = result['ADJCLOSE'].shift(1) / result['ADJCLOSE'].shift(12) - 1
    result['Momentum_2_15'] = result['ADJCLOSE'].shift(1) / result['ADJCLOSE'].shift(15) - 1
    result['Momentum_2_18'] = result['ADJCLOSE'].shift(1) / result['ADJCLOSE'].shift(18) - 1
    result['Momentum_2_24'] = result['ADJCLOSE'].shift(1) / result['ADJCLOSE'].shift(24) - 1
    result['Momentum_3_7'] = result['ADJCLOSE'].shift(2) / result['ADJCLOSE'].shift(7) - 1
    result['Momentum_3_12'] = result['ADJCLOSE'].shift(2) / result['ADJCLOSE'].shift(12) - 1
    result['Momentum_3_15'] = result['ADJCLOSE'].shift(2) / result['ADJCLOSE'].shift(15) - 1
    result['Momentum_3_18'] = result['ADJCLOSE'].shift(2) / result['ADJCLOSE'].shift(18) - 1
    result['Momentum_3_24'] = result['ADJCLOSE'].shift(2) / result['ADJCLOSE'].shift(24) - 1
    result['Momentum_6_12'] = result['ADJCLOSE'].shift(5) / result['ADJCLOSE'].shift(12) - 1
    result['Momentum_6_15'] = result['ADJCLOSE'].shift(5) / result['ADJCLOSE'].shift(15) - 1
    result['Momentum_6_18'] = result['ADJCLOSE'].shift(5) / result['ADJCLOSE'].shift(18) - 1
    result['Momentum_6_24'] = result['ADJCLOSE'].shift(5) / result['ADJCLOSE'].shift(24) - 1
    result['Momentum_12_18'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(18) - 1
    result['Momentum_12_24'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(24) - 1
    result['REVS_12_36'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(36) - 1
    result['REVS_12_48'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(48) - 1
    result['REVS_12_60'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(60) - 1
    result['REVS_24_36'] = result['ADJCLOSE'].shift(23) / result['ADJCLOSE'].shift(36) - 1
    result['REVS_24_48'] = result['ADJCLOSE'].shift(23) / result['ADJCLOSE'].shift(48) - 1
    result['REVS_24_60'] = result['ADJCLOSE'].shift(23) / result['ADJCLOSE'].shift(60) - 1
    result['REVS_37_60'] = result['ADJCLOSE'].shift(36) / result['ADJCLOSE'].shift(60) - 1
    # Path Dependent
    momentum_path = temp['ret_daily'].rolling(window=10).apply(lambda x: np.sqrt(np.sum(x * x)))
    momentum_path = momentum_path.resample('1m').last().ffill()
    result['REVS10_path'] = result['REVS10'] / momentum_path
    momentum_path = temp['ret_daily'].rolling(window=20).apply(lambda x: np.sqrt(np.sum(x * x)))
    momentum_path = momentum_path.resample('1m').last().ffill()
    result['REVS20_path'] = result['REVS20'] / momentum_path
    momentum_path = temp['ret_daily'].rolling(window=125).apply(lambda x: np.sqrt(np.sum(x * x)))
    momentum_path = momentum_path.resample('1m').last().ffill()
    result['Momentum_2_7_path'] = result['Momentum_2_7'] / momentum_path
    momentum_path = temp['ret_daily'].rolling(window=250).apply(lambda x: np.sqrt(np.sum(x * x)))
    momentum_path = momentum_path.resample('1m').last().ffill()
    result['Momentum_2_12_path'] = result['Momentum_2_12'] / momentum_path
    # PP Reversal
    average_price_60d = temp['PX_LAST'].rolling(window=60).mean()
    average_price_5d = temp['PX_LAST'].rolling(window=5).mean()
    daily_PPReversal = average_price_5d / average_price_60d
    result['PPReversal'] = daily_PPReversal.resample('1m').last().ffill()

    # Lottery Factors
    result['1m_average_price'] = temp['PX_LAST_RAW'].resample('1m').mean().ffill()
    result['MaxRet'] = temp['PX_LAST'].pct_change().resample('1m').max().ffill()

    # Volatility and Skewness Factors
    # Realized Volatility and Realized Skewness
    daily_std = temp['ret_daily'].rolling(window=23).std(ddof=0)
    result['RealizedVol_1M'] = daily_std.resample('1m').last().ffill()
    daily_std = temp['ret_daily'].rolling(window=67).std(ddof=0)
    result['RealizedVol_3M'] = daily_std.resample('1m').last().ffill()
    daily_std = temp['ret_daily'].rolling(window=125).std(ddof=0)
    result['RealizedVol_6M'] = daily_std.resample('1m').last().ffill()
    daily_std = temp['ret_daily'].rolling(window=252).std(ddof=0)
    result['RealizedVol_12M'] = daily_std.resample('1m').last().ffill()

    daily_sk = temp['ret_daily'].rolling(window=23).skew()
    result['RealizedSkew_1M'] = daily_sk.resample('1m').last().ffill()
    daily_sk = temp['ret_daily'].rolling(window=67).skew()
    result['RealizedSkew_3M'] = daily_sk.resample('1m').last().ffill()
    daily_sk = temp['ret_daily'].rolling(window=125).skew()
    result['RealizedSkew_6M'] = daily_sk.resample('1m').last().ffill()
    daily_sk = temp['ret_daily'].rolling(window=252).skew()
    result['RealizedSkew_12M'] = daily_sk.resample('1m').last().ffill()

    # Idiosyncratic Volatility and Skewness
    idio_raw = pd.merge(temp, mkt_dt.loc[:, ['market_index']], left_index=True,
                        right_index=True)
    idio_dt = idio_raw.groupby(['YYYY', 'MM']).apply(idiosyn)
    if idio_dt.shape[0] > 0:
        idio_dt = idio_dt.reset_index()
        idio_dt = idio_dt.loc[:, ['YYYY', 'MM', 'ivol', 'skew', 'beta', 'semivar']]
        idio_dt['YYYYMM'] = idio_dt['YYYY'] * 100 + idio_dt['MM']
        idio_dt['Date'] = idio_dt['YYYYMM'].map(lambda x: datetime(int(x / 100), x - int(x / 100) * 100, 1))
        idio_dt = idio_dt.set_index('Date')
        idio_dt = idio_dt.resample('1m').last()
        idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta', 'semivar']]
        result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
    else:
        result['ivol'] = np.nan
        result['skew'] = np.nan
        result['beta'] = np.nan
        result['semivar'] = np.nan
    result = result.rename(columns={'ivol': 'ivol_1', 'skew': 'skew_1', 'beta': 'beta_1', 'semivar': 'semivar_1'})

    idio_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=60)
        idio_raw_tmp = idio_raw[idio_raw.index <= ub]
        idio_raw_tmp = idio_raw_tmp[idio_raw_tmp.index > lb]
        if idio_raw_tmp.shape[0] >= 25:
            idio_dt_tmp = idiosyn(idio_raw_tmp)
            if type(idio_dt_tmp) == type(pd.DataFrame()):
                idio_dt_tmp.index = [idx]
                idio_dt_list.append(idio_dt_tmp)
    if len(idio_dt_list) > 0:
        idio_dt = pd.concat(idio_dt_list)
        idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta', 'semivar']]
        result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
    else:
        result['ivol'] = np.nan
        result['skew'] = np.nan
        result['beta'] = np.nan
    result = result.rename(columns={'ivol': 'ivol_2', 'skew': 'skew_2', 'beta': 'beta_2', 'semivar': 'semivar_2'})

    idio_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=180)
        idio_raw_tmp = idio_raw[idio_raw.index <= ub]
        idio_raw_tmp = idio_raw_tmp[idio_raw_tmp.index > lb]
        if idio_raw_tmp.shape[0] >= 90:
            idio_dt_tmp = idiosyn(idio_raw_tmp)
            if type(idio_dt_tmp) == type(pd.DataFrame()):
                idio_dt_tmp.index = [idx]
                idio_dt_list.append(idio_dt_tmp)
    if len(idio_dt_list) > 0:
        idio_dt = pd.concat(idio_dt_list)
        idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta', 'semivar']]
        result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
    else:
        result['ivol'] = np.nan
        result['skew'] = np.nan
        result['beta'] = np.nan
    result = result.rename(columns={'ivol': 'ivol_6', 'skew': 'skew_6', 'beta': 'beta_6', 'semivar': 'semivar_6'})

    idio_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=365)
        idio_raw_tmp = idio_raw[idio_raw.index <= ub]
        idio_raw_tmp = idio_raw_tmp[idio_raw_tmp.index > lb]
        if idio_raw_tmp.shape[0] >= 200:
            idio_dt_tmp = idiosyn(idio_raw_tmp)
            if type(idio_dt_tmp) == type(pd.DataFrame()):
                idio_dt_tmp.index = [idx]
                idio_dt_list.append(idio_dt_tmp)
    if len(idio_dt_list) > 0:
        idio_dt = pd.concat(idio_dt_list)
        idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta', 'semivar']]
        result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
    else:
        result['ivol'] = np.nan
        result['skew'] = np.nan
        result['beta'] = np.nan
    result = result.rename(
        columns={'ivol': 'ivol_12', 'skew': 'skew_12', 'beta': 'beta_12', 'semivar': 'semivar_12'})

    # Coskewness
    csk_raw = pd.merge(temp, mkt_dt.loc[:, ['market_index']], left_index=True, right_index=True)
    coskew_dt = csk_raw.groupby(['YYYY', 'MM']).apply(coskewness)
    if coskew_dt.shape[0] > 0:
        coskew_dt = pd.DataFrame(coskew_dt, columns=['coskew'])
        coskew_dt = coskew_dt.reset_index()
        coskew_dt['YYYYMM'] = coskew_dt['YYYY'] * 100 + coskew_dt['MM']
        coskew_dt['Date'] = coskew_dt['YYYYMM'].map(lambda x: datetime(int(x / 100), x - int(x / 100) * 100, 1))
        coskew_dt = coskew_dt.set_index('Date')
        coskew_dt = coskew_dt.resample('1m').last()
        coskew_dt = coskew_dt.loc[:, ['coskew']]
        result = pd.merge(left=result, right=coskew_dt, left_index=True, right_index=True, how='outer')
    else:
        result['coskew'] = np.nan
    result = result.rename(columns={'coskew': 'coskew_1'})

    coskew_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=60)
        csk_raw_tmp = csk_raw[csk_raw.index <= ub]
        csk_raw_tmp = csk_raw_tmp[csk_raw_tmp.index > lb]
        if csk_raw_tmp.shape[0] >= 25:
            coskew_dt_tmp = pd.DataFrame([coskewness(csk_raw_tmp)], columns=['coskew_2'])
            coskew_dt_tmp.index = [idx]
            coskew_dt_list.append(coskew_dt_tmp)
    if len(coskew_dt_list) > 0:
        coskew_dt = pd.concat(coskew_dt_list)
        coskew_dt = coskew_dt.loc[:, ['coskew_2']]
        result = pd.merge(left=result, right=coskew_dt, left_index=True, right_index=True, how='outer')
    else:
        result['coskew_2'] = np.nan

    coskew_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=180)
        csk_raw_tmp = csk_raw[csk_raw.index <= ub]
        csk_raw_tmp = csk_raw_tmp[csk_raw_tmp.index > lb]
        if csk_raw_tmp.shape[0] >= 90:
            coskew_dt_tmp = pd.DataFrame([coskewness(csk_raw_tmp)], columns=['coskew_6'])
            coskew_dt_tmp.index = [idx]
            coskew_dt_list.append(coskew_dt_tmp)
    if len(coskew_dt_list) > 0:
        coskew_dt = pd.concat(coskew_dt_list)
        coskew_dt = coskew_dt.loc[:, ['coskew_6']]
        result = pd.merge(left=result, right=coskew_dt, left_index=True, right_index=True, how='outer')
    else:
        result['coskew_6'] = np.nan

    coskew_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=365)
        csk_raw_tmp = csk_raw[csk_raw.index <= ub]
        csk_raw_tmp = csk_raw_tmp[csk_raw_tmp.index > lb]
        if csk_raw_tmp.shape[0] >= 200:
            coskew_dt_tmp = pd.DataFrame([coskewness(csk_raw_tmp)], columns=['coskew_12'])
            coskew_dt_tmp.index = [idx]
            coskew_dt_list.append(coskew_dt_tmp)
    if len(coskew_dt_list) > 0:
        coskew_dt = pd.concat(coskew_dt_list)
        coskew_dt = coskew_dt.loc[:, ['coskew_12']]
        result = pd.merge(left=result, right=coskew_dt, left_index=True, right_index=True, how='outer')
    else:
        result['coskew_12'] = np.nan

    # RSJ
    rsj_raw = pd.merge(temp, mkt_dt.loc[:, ['market_index']], left_index=True, right_index=True)
    rsj_dt = rsj_raw.groupby(['YYYY', 'MM']).apply(rsj)
    if rsj_dt.shape[0] > 0:
        rsj_dt = pd.DataFrame(rsj_dt, columns=['rsj'])
        rsj_dt = rsj_dt.reset_index()
        rsj_dt['YYYYMM'] = rsj_dt['YYYY'] * 100 + rsj_dt['MM']
        rsj_dt['Date'] = rsj_dt['YYYYMM'].map(lambda x: datetime(int(x / 100), x - int(x / 100) * 100, 1))
        rsj_dt = rsj_dt.set_index('Date')
        rsj_dt = rsj_dt.resample('1m').last()
        rsj_dt = rsj_dt.loc[:, ['rsj']]
        result = pd.merge(left=result, right=rsj_dt, left_index=True, right_index=True, how='outer')
    else:
        result['rsj'] = np.nan
    result = result.rename(columns={'rsj': 'rsj_1'})

    rsj_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=60)
        rsj_raw_tmp = rsj_raw[rsj_raw.index <= ub]
        rsj_raw_tmp = rsj_raw_tmp[rsj_raw_tmp.index > lb]
        if rsj_raw_tmp.shape[0] >= 25:
            rsj_dt_tmp = pd.DataFrame([coskewness(rsj_raw_tmp)], columns=['rsj_2'])
            rsj_dt_tmp.index = [idx]
            rsj_dt_list.append(rsj_dt_tmp)
    if len(rsj_dt_list) > 0:
        rsj_dt = pd.concat(rsj_dt_list)
        rsj_dt = rsj_dt.loc[:, ['rsj_2']]
        result = pd.merge(left=result, right=rsj_dt, left_index=True, right_index=True, how='outer')
    else:
        result['rsj_2'] = np.nan

    rsj_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=180)
        rsj_raw_tmp = rsj_raw[rsj_raw.index <= ub]
        rsj_raw_tmp = rsj_raw_tmp[rsj_raw_tmp.index > lb]
        if rsj_raw_tmp.shape[0] >= 90:
            rsj_dt_tmp = pd.DataFrame([coskewness(rsj_raw_tmp)], columns=['rsj_6'])
            rsj_dt_tmp.index = [idx]
            rsj_dt_list.append(rsj_dt_tmp)
    if len(rsj_dt_list) > 0:
        rsj_dt = pd.concat(rsj_dt_list)
        rsj_dt = rsj_dt.loc[:, ['rsj_6']]
        result = pd.merge(left=result, right=rsj_dt, left_index=True, right_index=True, how='outer')
    else:
        result['rsj_6'] = np.nan

    rsj_dt_list = []
    for idx in result.index.tolist():
        ub = idx
        lb = idx - timedelta(days=360)
        rsj_raw_tmp = rsj_raw[rsj_raw.index <= ub]
        rsj_raw_tmp = rsj_raw_tmp[rsj_raw_tmp.index > lb]
        if rsj_raw_tmp.shape[0] >= 200:
            rsj_dt_tmp = pd.DataFrame([coskewness(rsj_raw_tmp)], columns=['rsj_12'])
            rsj_dt_tmp.index = [idx]
            rsj_dt_list.append(rsj_dt_tmp)
    if len(rsj_dt_list) > 0:
        rsj_dt = pd.concat(rsj_dt_list)
        rsj_dt = rsj_dt.loc[:, ['rsj_12']]
        result = pd.merge(left=result, right=rsj_dt, left_index=True, right_index=True, how='outer')
    else:
        result['rsj_12'] = np.nan

    if result.shape[0] > 0:
        result = result.ffill()
        result.to_csv(os.path.join(addpath.data_path, "Ashare", "monthly_prior", "trading_factors", symbol + ".csv"))


if __name__ == "__main__":
    symbol_list = []
    selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks")
    portfolio_list = os.listdir(selected_stock_path)
    for portfolio in portfolio_list:
        selected = pd.read_csv(os.path.join(selected_stock_path, portfolio))
        symbol_list = symbol_list + selected['Stkcd'].tolist()
    symbol_list = list(set(symbol_list))

    pool = multiprocessing.Pool(10)
    # for symbol in [symbol_list[0]]::
    for symbol in symbol_list:
        # cal_factors(symbol)
        pool.apply_async(cal_factors, args=(symbol,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")
