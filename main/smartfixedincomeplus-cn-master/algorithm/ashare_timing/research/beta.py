from algorithm import addpath
import statsmodels.api as sm
import pandas as pd
import numpy as np
import os
import multiprocessing
from datetime import datetime, timedelta


def idiosyn(insample):
    temp_idiosyn = insample.loc[:, ['ret_daily', 'market_index']]
    temp_idiosyn = temp_idiosyn.dropna()
    temp_idiosyn = temp_idiosyn[~temp_idiosyn.isin([np.nan, np.inf, -np.inf]).any(1)]
    if temp_idiosyn.shape[0] > 10:
        X = sm.add_constant(temp_idiosyn.loc[:, 'market_index'])
        Y = temp_idiosyn['ret_daily']
        model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
        beta = model.params.iloc[1]
        output = pd.DataFrame([beta]).transpose()
        output.columns = ['beta']
        return output


def cal_factors(symbol):
    trading_data_path = os.path.join(addpath.data_path, "Ashare", "sw_trading_data", symbol + ".csv")

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

    result['ret_forward_1M'] = result['ADJCLOSE'].shift(-1) / result['ADJCLOSE'] - 1
    result['ret_forward_2M'] = result['ADJCLOSE'].shift(-2) / result['ADJCLOSE'] - 1
    result['ret_forward_3M'] = result['ADJCLOSE'].shift(-3) / result['ADJCLOSE'] - 1
    result['ret_forward_6M'] = result['ADJCLOSE'].shift(-6) / result['ADJCLOSE'] - 1

    result['ret_backward_1M'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(1) - 1
    result['ret_backward_2M'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(2) - 1
    result['ret_backward_6M'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(6) - 1
    result['ret_backward_12M'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(12) - 1

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
        idio_dt = idio_dt.loc[:, ['YYYY', 'MM', 'beta']]
        idio_dt['YYYYMM'] = idio_dt['YYYY'] * 100 + idio_dt['MM']
        idio_dt['Date'] = idio_dt['YYYYMM'].map(lambda x: datetime(int(x / 100), x - int(x / 100) * 100, 1))
        idio_dt = idio_dt.set_index('Date')
        idio_dt = idio_dt.resample('1m').last()
        idio_dt = idio_dt.loc[:, ['beta']]
        result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
    else:
        result['beta'] = np.nan
    result = result.rename(columns={'beta': 'beta_1'})

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
        idio_dt = idio_dt.loc[:, ['beta']]
        result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
    else:
        result['beta'] = np.nan
    result = result.rename(columns={'beta': 'beta_2'})

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
        idio_dt = idio_dt.loc[:, ['beta']]
        result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
    else:
        result['beta'] = np.nan
    result = result.rename(columns={'beta': 'beta_6'})

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
        idio_dt = idio_dt.loc[:, ['beta']]
        result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
    else:
        result['beta'] = np.nan
    result = result.rename(
        columns={'beta': 'beta_12'})

    if result.shape[0] > 0:
        result = result.ffill()
        result.to_csv(os.path.join(addpath.data_path, "Ashare", "market_timing", "sw_trading_factors", symbol + ".csv"))


def cs_factor_helper(date):
    factor_path = os.path.join(addpath.data_path, "Ashare", "market_timing", "sw_trading_factors")
    cs_factor_path = os.path.join(addpath.data_path, "Ashare", "market_timing", "sw_cs_trading_factors")

    factor_files = os.listdir(factor_path)
    result_list = []
    for factor_file in factor_files:
        try:
            tmp = pd.read_csv(os.path.join(factor_path, factor_file), index_col=0, parse_dates=[0])
            tmp = tmp.loc[date, :]
            tmp = pd.DataFrame(tmp)
            tmp.columns = [factor_file[:-4]]
            result_list.append(tmp)
        except:
            print("No data on " + date.strftime('%Y-%m-%d') + " for factors from symbol " + factor_file[:-4])

    result = pd.concat(result_list, axis=1)
    result = result.transpose()
    result = result.dropna(axis=0, how='all')
    result.to_csv(os.path.join(cs_factor_path, date.strftime('%Y-%m-%d') + '.csv'))


def cs_factor_main(start, end):
    formation_dates = pd.date_range(start, end, freq='1M')

    pool = multiprocessing.Pool(15)
    for date in formation_dates:
        pool.apply_async(cs_factor_helper, args=(date,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")




if __name__ == "__main__":
    trading_files = os.listdir(os.path.join(addpath.data_path, "Ashare", "sw_trading_data"))
    for file in trading_files:
        cal_factors(file[:-4])

    start = '2013-01-01'
    end = '2020-11-30'
    cs_factor_main(start, end)

    market_premium = pd.DataFrame()
    formation_dates = pd.date_range(start, end, freq='1M')
    for date in formation_dates:
        cs_factor_path = os.path.join(addpath.data_path, "Ashare", "market_timing", "sw_cs_trading_factors")
        sw_cs_trading_factors = pd.read_csv(os.path.join(cs_factor_path, date.strftime('%Y-%m-%d') + '.csv'))
        for hrz in [1, 2, 6, 12]:
            temp = sw_cs_trading_factors.loc[:, ['ret_backward_' + str(hrz) + 'M', 'beta_' + str(hrz)]]
            temp = temp.dropna()
            temp = temp[~temp.isin([np.nan, np.inf, -np.inf]).any(1)]
            X = sm.add_constant(temp.loc[:, 'beta_' + str(hrz)])
            Y = temp['ret_backward_' + str(hrz) + 'M']
            model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
            mkt_prm_tmp = model.params.iloc[1]
            mkt_synch = model.tvalues.iloc[1]
            market_premium.loc[date, 'mkt_premium_' + str(hrz)] = mkt_prm_tmp
            market_premium.loc[date, 'mkt_synch_' + str(hrz)] = mkt_synch

    mkt_dt = pd.read_csv(os.path.join(addpath.data_path, "Ashare", "monthly_prior", "market_index.csv"), parse_dates=[0], index_col=0)
    mkt_dt = mkt_dt.resample('1M').last().ffill()
    mkt_dt["market_index_fwd_1M"] = mkt_dt["SHSZ300 Index"].shift(-1) / mkt_dt["SHSZ300 Index"] - 1
    mkt_dt["market_index_fwd_2M"] = mkt_dt["SHSZ300 Index"].shift(-2) / mkt_dt["SHSZ300 Index"] - 1
    mkt_dt["market_index_fwd_3M"] = mkt_dt["SHSZ300 Index"].shift(-3) / mkt_dt["SHSZ300 Index"] - 1
    mkt_dt["market_index_fwd_6M"] = mkt_dt["SHSZ300 Index"].shift(-6) / mkt_dt["SHSZ300 Index"] - 1
    mkt_dt["market_index_fwd_12M"] = mkt_dt["SHSZ300 Index"].shift(-12) / mkt_dt["SHSZ300 Index"] - 1
    mkt_dt["market_index_bwd_1M"] = mkt_dt["SHSZ300 Index"] / mkt_dt["SHSZ300 Index"].shift(1) - 1
    mkt_dt["market_index_bwd_2M"] = mkt_dt["SHSZ300 Index"] / mkt_dt["SHSZ300 Index"].shift(2) - 1
    mkt_dt["market_index_bwd_6M"] = mkt_dt["SHSZ300 Index"] / mkt_dt["SHSZ300 Index"].shift(6) - 1
    mkt_dt["market_index_bwd_12M"] = mkt_dt["SHSZ300 Index"] / mkt_dt["SHSZ300 Index"].shift(12) - 1

    market_premium['market_index_fwd_1M'] = mkt_dt["market_index_fwd_1M"]
    market_premium['market_index_fwd_2M'] = mkt_dt["market_index_fwd_2M"]
    market_premium['market_index_fwd_3M'] = mkt_dt["market_index_fwd_3M"]
    market_premium['market_index_fwd_6M'] = mkt_dt["market_index_fwd_6M"]
    market_premium['market_index_fwd_12M'] = mkt_dt["market_index_fwd_12M"]

    market_premium['market_index_fwd_1M_dummy'] = np.where(market_premium["market_index_fwd_1M"] < 0, 1, 0)
    market_premium['market_index_fwd_2M_dummy'] = np.where(market_premium["market_index_fwd_2M"] < 0, 1, 0)
    market_premium['market_index_fwd_3M_dummy'] = np.where(market_premium["market_index_fwd_3M"] < 0, 1, 0)
    market_premium['market_index_fwd_6M_dummy'] = np.where(market_premium["market_index_fwd_6M"] < 0, 1, 0)
    market_premium['market_index_fwd_12M_dummy'] = np.where(market_premium["market_index_fwd_12M"] < 0, 1, 0)

    market_premium['market_index_bwd_1M'] = mkt_dt["market_index_bwd_1M"]
    market_premium['market_index_bwd_2M'] = mkt_dt["market_index_bwd_2M"]
    market_premium['market_index_bwd_6M'] = mkt_dt["market_index_bwd_6M"]
    market_premium['market_index_bwd_12M'] = mkt_dt["market_index_bwd_12M"]

    market_premium['market_index_premiumminusbwd_1M'] = market_premium['mkt_premium_1'] - mkt_dt["market_index_bwd_1M"]
    market_premium['market_index_premiumminusbwd_2M'] = market_premium['mkt_premium_2'] - mkt_dt["market_index_bwd_2M"]
    market_premium['market_index_premiumminusbwd_6M'] = market_premium['mkt_premium_6'] - mkt_dt["market_index_bwd_6M"]
    market_premium['market_index_premiumminusbwd_12M'] = market_premium['mkt_premium_12'] - mkt_dt["market_index_bwd_12M"]

    market_premium['market_index_premiumminusbwd_1M_diff_1M'] = market_premium['market_index_premiumminusbwd_1M'] - market_premium['market_index_premiumminusbwd_1M'].shift(1)
    market_premium['market_index_premiumminusbwd_1M_diff_2M'] = market_premium['market_index_premiumminusbwd_1M'] - market_premium['market_index_premiumminusbwd_1M'].shift(2)
    market_premium['market_index_premiumminusbwd_1M_diff_3M'] = market_premium['market_index_premiumminusbwd_1M'] - market_premium['market_index_premiumminusbwd_1M'].shift(3)

    market_premium['market_index_premiumminusbwd_2M_diff_1M'] = market_premium['market_index_premiumminusbwd_2M'] - market_premium['market_index_premiumminusbwd_2M'].shift(1)
    market_premium['market_index_premiumminusbwd_2M_diff_2M'] = market_premium['market_index_premiumminusbwd_2M'] - market_premium['market_index_premiumminusbwd_2M'].shift(2)
    market_premium['market_index_premiumminusbwd_2M_diff_3M'] = market_premium['market_index_premiumminusbwd_2M'] - market_premium['market_index_premiumminusbwd_2M'].shift(3)
    market_premium['market_index_premiumminusbwd_2M_diff_6M'] = market_premium['market_index_premiumminusbwd_2M'] - market_premium['market_index_premiumminusbwd_2M'].shift(6)

    market_premium['market_index_premiumminusbwd_6M_diff_1M'] = market_premium['market_index_premiumminusbwd_6M'] - market_premium['market_index_premiumminusbwd_6M'].shift(1)
    market_premium['market_index_premiumminusbwd_6M_diff_2M'] = market_premium['market_index_premiumminusbwd_6M'] - market_premium['market_index_premiumminusbwd_6M'].shift(2)
    market_premium['market_index_premiumminusbwd_6M_diff_3M'] = market_premium['market_index_premiumminusbwd_6M'] - market_premium['market_index_premiumminusbwd_6M'].shift(3)
    market_premium['market_index_premiumminusbwd_6M_diff_6M'] = market_premium['market_index_premiumminusbwd_6M'] - market_premium['market_index_premiumminusbwd_6M'].shift(6)

    market_premium.to_csv(os.path.join(addpath.data_path, "Ashare", "market_timing", "premium.csv"))
    corr = market_premium.corr()

    market_premium_ann = market_premium.resample('Y').last().ffill()
    corr_ann = market_premium_ann.corr()

    market_premium_sa = market_premium.resample('6M').last().ffill()
    corr_sa = market_premium_sa.corr()

    corr_list = []
    corr_av_list = []
    for mm in range(12):
        mkt_premium_tmp = market_premium[market_premium.index.month == mm + 1]
        corr_tmp = mkt_premium_tmp.corr()
        corr_tmp = corr_tmp.loc[['market_index_fwd_1M', 'market_index_fwd_3M', 'market_index_fwd_6M', 'market_index_fwd_12M', 'market_index_fwd_1M_dummy', 'market_index_fwd_3M_dummy', 'market_index_fwd_6M_dummy', 'market_index_fwd_12M_dummy'], :]
        corr_tmp = corr_tmp.drop(columns=['market_index_fwd_1M', 'market_index_fwd_2M', 'market_index_fwd_3M', 'market_index_fwd_6M', 'market_index_fwd_12M', 'market_index_fwd_1M_dummy', 'market_index_fwd_2M_dummy', 'market_index_fwd_3M_dummy', 'market_index_fwd_6M_dummy', 'market_index_fwd_12M_dummy'])
        corr_av_list.append(corr_tmp.copy())
        corr_tmp.index = corr_tmp.index.map(lambda x: x + "_" + str(mm + 1))
        corr_list.append(corr_tmp)
    corr_mm = pd.concat(corr_list)

    corr_av = corr_av_list[0]
    for i in range(11):
        corr_av = corr_av + corr_av_list[i + 1]
    corr_av = corr_av / 12