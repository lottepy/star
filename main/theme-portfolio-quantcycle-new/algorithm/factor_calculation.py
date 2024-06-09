import numpy as np
import pandas as pd
import os
import math
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
	if temp_ps_liq.shape[0] > 15:
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
	if temp_coskewness.shape[0] > 15:
		X = sm.add_constant(temp_coskewness.loc[:, ['market_index', 'm_sq']])
		Y = temp_coskewness['ret_daily']
		model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
		coskew = model.params.iloc[2]

		return coskew


def idiosyn(insample):
	temp_idiosyn = insample.loc[:, ['ret_daily', 'market_index']]
	temp_idiosyn = temp_idiosyn.dropna()
	temp_idiosyn = temp_idiosyn[~temp_idiosyn.isin([np.nan, np.inf, -np.inf]).any(1)]
	if temp_idiosyn.shape[0] > 15:
		X = sm.add_constant(temp_idiosyn.loc[:, 'market_index'])
		Y = temp_idiosyn['ret_daily']
		Y_mean=temp_idiosyn['ret_daily'].mean()
		lows=Y[Y<Y_mean]
		semivar=np.sum((lows-Y_mean)**2)/len(lows)
		model = sm.OLS(endog=Y, exog=X, missing='drop').fit()
		beta = model.params.iloc[1]
		residual = model.resid
		ivol = np.std(residual)
		skewness = skew(residual)
		output = pd.DataFrame([ivol, skewness, beta,semivar]).transpose()
		output.columns = ['ivol', 'skew', 'beta','semivar']
		return output


def cal_factors_us(symbol, rebalance_freq,input_path):
	us_fin_path = os.path.join(input_path, "Financial")
	us_tra_path = os.path.join(input_path, "trading")

	# Mkt factor
	mkt_dt = pd.read_csv(os.path.join(input_path,'reference', "market_index.csv"), parse_dates=[0], index_col=0)
	mkt_dt["market_index"] = mkt_dt["market_index"].pct_change()
	mkt_dt = mkt_dt.fillna(0)

	temp = pd.read_csv(os.path.join(us_tra_path, symbol + ".csv"), parse_dates=[0], index_col=0)
	temp = temp[['PX_LAST', 'PX_LAST_RAW', 'PX_VOLUME_RAW', 'EQY_SH_OUT_RAW','PX_LAST_SPLIT']]

	if temp.shape[0] > 0:
		temp['symbol'] = symbol
		temp['Date'] = temp.index
		temp['YYYY'] = temp['Date'].map(lambda x: x.year)
		temp['MM'] = temp['Date'].map(lambda x: x.month)
		temp = temp.fillna(method='ffill')
		temp['ret_daily'] = temp['PX_LAST'].pct_change()
		temp['TURN'] = temp['PX_VOLUME_RAW'] / temp['EQY_SH_OUT_RAW']
		# Liquidity Measures
		# Turnovers
		result = temp.loc[:, ['symbol', 'YYYY', 'MM']].resample('1m').last()
		result['TO_1M'] = temp['TURN'].resample('1m').mean()
		result['TO_1M'] = result['TO_1M'].ffill()
		result['TO_3M'] = result['TO_1M'].rolling(window=3, min_periods=1).apply(np.nanmean)
		result['TO_6M'] = result['TO_1M'].rolling(window=6, min_periods=3).apply(np.nanmean)
		result['TO_12M'] = result['TO_1M'].rolling(window=12, min_periods=6).apply(np.nanmean)
		result['Abnm_TO'] = result['TO_1M'] - result['TO_12M'].shift()
		result['LNTO_1M'] = result['TO_1M'].map(lambda x: np.log(x))
		# Amihud Illiquidity and BPST Illiquidity Shock
		temp['ILLIQ'] = temp['ret_daily'].map(lambda x: abs(x)) / (temp['PX_VOLUME_RAW'] * temp['PX_LAST_RAW']) * 1000000
		result['ILLIQ'] = temp['ILLIQ'].resample('1m').mean().ffill()
		result['LIQU'] = result['ILLIQ'] - result['ILLIQ'].rolling(window=12, min_periods=6).apply(np.nanmean).shift()
		# Pastor and Stambaugh liquidity
		ps_raw = pd.merge(temp, mkt_dt[['market_index']], left_index=True, right_index=True)
		ps_raw['ex_ret'] = ps_raw['ret_daily'] - ps_raw['market_index']
		ps_raw['ex_ret_f1'] = ps_raw['ex_ret'].shift(-1)
		ps_raw['signed_dvol'] = ps_raw['ex_ret'].map(lambda x: np.sign(x)) * ps_raw['PX_VOLUME_RAW'] * ps_raw['PX_LAST_RAW']
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
		result['EQY_SH_OUT_RAW'] = temp['EQY_SH_OUT_RAW'].resample('1m').last()
		result['EQY_SH_OUT_RAW'] = result['EQY_SH_OUT_RAW'].ffill()
		result['VOLUME_RAW'] = temp['PX_VOLUME_RAW'].resample('1m').sum()
		result['VOLUME_RAW'] = result['VOLUME_RAW'].ffill()
		result['CLOSE'] = temp['PX_LAST_RAW'].resample('1m').last()
		result['CLOSE'] = result['CLOSE'].ffill()
		result['ADJCLOSE'] = ADJCLOSE.resample('1m').last()
		result['ADJCLOSE'] = result['ADJCLOSE'].ffill()
		result['MARKET_CAP'] = result['CLOSE'] * result['EQY_SH_OUT_RAW']
		result['PX_LAST_SPLIT'] = temp['PX_LAST_SPLIT'].resample('1m').last()
		result['PX_LAST_SPLIT'] = result['PX_LAST_SPLIT'].ffill()

		reb_hrz = {
			"ANNUALLY": 12,
			"SEMIANNUALLY": 6,
			"QUARTERLY": 3,
			"BIMONTHLY": 2,
			"MONTHLY": 1,
		}
		result['ret_forward'] = result['ADJCLOSE'].shift(-reb_hrz[rebalance_freq]) / result['ADJCLOSE'] - 1

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
			idio_dt = idio_dt.loc[:, ['YYYY', 'MM', 'ivol', 'skew', 'beta','semivar']]
			idio_dt['YYYYMM'] = idio_dt['YYYY'] * 100 + idio_dt['MM']
			idio_dt['Date'] = idio_dt['YYYYMM'].map(lambda x: datetime(int(x / 100), x - int(x / 100) * 100, 1))
			idio_dt = idio_dt.set_index('Date')
			idio_dt = idio_dt.resample('1m').last()
			idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta','semivar']]
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
			idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta','semivar']]
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
			idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta','semivar']]
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
			idio_dt = idio_dt.loc[:, ['ivol', 'skew', 'beta','semivar']]
			result = pd.merge(left=result, right=idio_dt, left_index=True, right_index=True, how='outer')
		else:
			result['ivol'] = np.nan
			result['skew'] = np.nan
			result['beta'] = np.nan
		result = result.rename(columns={'ivol': 'ivol_12', 'skew': 'skew_12', 'beta': 'beta_12', 'semivar': 'semivar_12'})

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

		# Dividend Yield
		result['DY_12'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(12) - result['PX_LAST_SPLIT'] / result['PX_LAST_SPLIT'].shift(12)
		result['DY_6'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(6) - result['PX_LAST_SPLIT'] / result[
			'PX_LAST_SPLIT'].shift(6)
		result['DY_3'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(3) - result['PX_LAST_SPLIT'] / result[
			'PX_LAST_SPLIT'].shift(3)
		result['DY_1'] = result['ADJCLOSE'] / result['ADJCLOSE'].shift(1) - result['PX_LAST_SPLIT'] / result[
			'PX_LAST_SPLIT'].shift(1)

		temp2 = pd.DataFrame(index=pd.date_range(temp.index[0], temp.index[-1], freq='D'))
		temp2['EQY_SH_OUT_RAW'] = temp['EQY_SH_OUT_RAW']
		temp2['PX_LAST_SPLIT'] = temp['PX_LAST_SPLIT']
		temp2['PX_LAST_RAW'] = temp['PX_LAST_RAW']
		temp2 = temp2.ffill()
	else:
		result = pd.DataFrame()

	temp = pd.read_csv(os.path.join(us_fin_path, symbol + ".csv"), parse_dates=['datadate', 'rdq'], index_col='datadate')
	column_names = {
		'actq': 'CURRASSET',
		'apq': 'ARDR_ACCOUNTS_PAYABLE_TRADE',
		'atq': 'BS_TOT_ASSET',
		'ceqq': 'TOT_COMMON_EQY',
		'cheq': 'BS_CASH_CASH_EQUIVALENTS_AND_STI',
		'chq': 'BS_CASH_NEAR_CASH_ITEM',
		'ciq': 'IS_COMPREHENSIVE_INCOME',
		'cogsq': 'IS_COG_AND_SERVICES_SOLD',
		'cshfdq': 'IS_SH_FOR_DILUTED_EPS',
		'cshoq': 'BS_SH_OUT',
		'cshprq': 'IS_AVG_NUM_SH_FOR_EPS',
		'dd1q': 'BS_ST_PORTION_OF_LT_DEBT',
		'dlcq': 'BS_ST_BORROW',
		'dlttq': 'BS_LONG_TERM_BORROWINGS',
		'dpq': 'CF_DEPR_AMORT',
		'esopctq': 'ESOPCTQ',
		'gdwlq': 'BS_GOODWILL',
		'glaq': 'IS_GAIN_LOSS_ON_INVESTMENTS',
		'glceaq': 'ARD_GL_ON_SALE_OF_INVESTMENTS',
		'glcepq': 'ARD_GL_ON_SALE_OF_INVESTMENTS_PRETAX',
		'glivq': 'IS_INVEST_INCOME',
		'glpq': 'GL_PRETAX',
		'ibq': 'IS_INC_BEF_XO_ITEM',
		'intanq': 'BS_DISCLOSED_INTANGIBLES',
		'invtq': 'BS_INVENTORIES',
		'ivltq': 'BS_LONG_TERM_INVESTMENTS',
		'lctq': 'CURRLIAB',
		'lltq': 'LONG_TERM_LIAB',
		'ltq': 'BS_TOT_LIAB2',
		'niq': 'NET_INCOME',
		'npq': 'NOTE_PAYABLE',
		'oiadpq': 'IS_OPER_INC',
		'oibdpq': 'OPERATING_INCOME_BEFORE_DEPREC',
		'ppegtq': 'ARDR_PROPERTY_PLANT_EQUIP_GROSS',
		'ppentq': 'ARDR_PROPERTY_PLANT_EQUIP_NET',
		'pstkq': 'PREFERRED_EQUITYnMINORITY_INT',
		'rdipq': 'RD_INPROICESS',
		'rectq': 'BS_ACCT_NOTE_RCV',
		'req': 'BS_RETAIN_EARN',
		'revtq': 'ARD_TOTAL_REVENUES',
		'saleq': 'SALES_REV_TURN',
		'teqq': 'ARD_TOTAL_SHAREHOLDERS_EQUITY',
		'tstknq': 'BS_SH_TSY_STOCK',
		'tstkq': 'BS_AMT_OF_TSY_STOCK',
		'txditcq': 'BS_TOTAL_DEF_TAX_CREDIT',
		'txtq': 'IS_INC_TAX_EXP',
		'xaccq': 'ARD_ACCRUED_EXP_AND_OTHER',
		'xintq': 'IS_INT_EXPENSE',
		'xiq': 'XO_GL_NET_OF_TAX',
		'xoprq': 'IS_OPERATING_EXPN',
		'xrdq': 'IS_RD_EXPEND',
		'xsgaq': 'IS_SGnA_EXPENSE',
		'exchg': 'EXCH_CODE',
		'cik': 'CENTRAL_INDEX_KEY_NUMBER',
		'ggroup': 'GICS_INDUSTRY_GROUP',
		'gind': 'GICS_INDUSTRY',
		'gsector': 'GICS_SECTOR',
		'gsubind': 'GICS_SUB_INDUSTRY',
		'sic': 'SIC',
		'rdq':'ANNOUNCEMENT_DT'
	}

	if temp.shape[0] > 0:
		temp['symbol'] = symbol
		temp = temp.rename(columns=column_names)
		# temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].map(lambda x: np.nan if pd.isna(x) else datetime.strptime(x, "%Y-%m-%d"))
		temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT']
		if temp[['ANNOUNCEMENT_DT']].dropna().shape[0] > 0:
			temp['effective_date'] = np.where(pd.isna(temp['ANNOUNCEMENT_DT']), temp.index + timedelta(days=90),
											  temp['ANNOUNCEMENT_DT'])
		else:
			temp['effective_date'] = temp.index + timedelta(days=90)
		temp.index.name = 'date'
		temp = temp.reset_index()
		temp = temp.set_index('effective_date')
		if result.shape[0] > 0:
			temp['EQY_SH_OUT_RAW'] = temp2['EQY_SH_OUT_RAW']
			temp['PX_LAST_RAW'] = temp2['PX_LAST_RAW']
		else:
			temp['EQY_SH_OUT_RAW'] = np.nan
			temp['PX_LAST_RAW'] = np.nan
		temp = temp.reset_index()
		temp = temp.set_index('date')
		temp['datadate'] = temp.index
		temp = temp.resample('1M').last().ffill()

		temp['PREFERRED_EQUITYnMINORITY_INT'] = temp['PREFERRED_EQUITYnMINORITY_INT'].fillna(0)
		temp['TOT_EQUITY'] = temp['ARD_TOTAL_SHAREHOLDERS_EQUITY'] + temp['BS_TOTAL_DEF_TAX_CREDIT'].fillna(0) \
							 - temp['PREFERRED_EQUITYnMINORITY_INT'].fillna(0)

		temp['GROSS_PROFIT'] = temp['SALES_REV_TURN'] - temp['IS_COG_AND_SERVICES_SOLD']
		temp['EBITDA'] = temp['OPERATING_INCOME_BEFORE_DEPREC']
		temp['EBIT'] = temp['IS_OPER_INC']
		temp['EBT'] = temp['EBIT'] - temp['IS_INT_EXPENSE']

		temp['SALES_REV_TURN_SEMI'] = temp['SALES_REV_TURN'] + temp['SALES_REV_TURN'].shift(3)
		temp['IS_COG_AND_SERVICES_SOLD_SEMI'] = temp['IS_COG_AND_SERVICES_SOLD'] + temp['IS_COG_AND_SERVICES_SOLD'].shift(3)
		temp['GROSS_PROFIT_SEMI'] = temp['GROSS_PROFIT'] + temp['GROSS_PROFIT'].shift(3)
		temp['EBITDA_SEMI'] = temp['EBITDA'] + temp['EBITDA'].shift(3)
		temp['EBIT_SEMI'] = temp['EBIT'] + temp['EBIT'].shift(3)
		temp['EBT_SEMI'] = temp['EBT'] + temp['EBT'].shift(3)
		temp['NET_INCOME_SEMI'] = temp['NET_INCOME'] + temp['NET_INCOME'].shift(3)
		temp['IS_RD_EXPEND_SEMI'] = temp['IS_RD_EXPEND'] + temp['IS_RD_EXPEND'].shift(3)
		temp['IS_INT_EXPENSE_SEMI'] = temp['IS_INT_EXPENSE'] + temp['IS_INT_EXPENSE'].shift(3)

		temp['SALES_REV_TURN_TTM'] = temp['SALES_REV_TURN'] + temp['SALES_REV_TURN'].shift(3) + temp['SALES_REV_TURN'].shift(6) + temp['SALES_REV_TURN'].shift(9)
		temp['IS_COG_AND_SERVICES_SOLD_TTM'] = temp['IS_COG_AND_SERVICES_SOLD'] + temp['IS_COG_AND_SERVICES_SOLD'].shift(3) + temp['IS_COG_AND_SERVICES_SOLD'].shift(6) + temp['IS_COG_AND_SERVICES_SOLD'].shift(9)
		temp['GROSS_PROFIT_TTM'] = temp['GROSS_PROFIT'] + temp['GROSS_PROFIT'].shift(3) + temp['GROSS_PROFIT'].shift(6) + temp['GROSS_PROFIT'].shift(9)
		temp['EBITDA_TTM'] = temp['EBITDA'] + temp['EBITDA'].shift(3) + temp['EBITDA'].shift(6) + temp['EBITDA'].shift(9)
		temp['EBIT_TTM'] = temp['EBIT'] + temp['EBIT'].shift(3) + temp['EBIT'].shift(6) + temp['EBIT'].shift(9)
		temp['EBT_TTM'] = temp['EBT'] + temp['EBT'].shift(3) + temp['EBT'].shift(6) + temp['EBT'].shift(9)
		temp['NET_INCOME_TTM'] = temp['NET_INCOME'] + temp['NET_INCOME'].shift(3) + temp['NET_INCOME'].shift(6) + temp['NET_INCOME'].shift(9)
		temp['IS_RD_EXPEND_TTM'] = temp['IS_RD_EXPEND'] + temp['IS_RD_EXPEND'].shift(3) + temp['IS_RD_EXPEND'].shift(6) + temp['IS_RD_EXPEND'].shift(9)
		temp['IS_INT_EXPENSE_TTM'] = temp['IS_INT_EXPENSE'] + temp['IS_INT_EXPENSE'].shift(3) + temp['IS_INT_EXPENSE'].shift(6) + temp['IS_INT_EXPENSE'].shift(9)

		# Negative Indicating Factors
		# # Total Accruals
		temp['acc_cul_BS'] = temp['CURRASSET'].fillna(0) - temp['BS_CASH_CASH_EQUIVALENTS_AND_STI'].fillna(0) - \
					  temp['CURRLIAB'].fillna(0)+temp['BS_ST_BORROW'].fillna(0)
		tot_assets = temp['BS_TOT_ASSET']
		temp['NIexACC'] = temp['NET_INCOME']-(temp['acc_cul_BS'] - temp['acc_cul_BS'].shift(3))-temp['CF_DEPR_AMORT'].fillna(0)
		temp['NIexACConNI']=temp['NIexACC']/temp['NET_INCOME']
		temp['NIexACConNI_SEMI']=(temp['NIexACC']+temp['NIexACC'].shift(3))/temp['NET_INCOME_SEMI']
		temp['NIexACConNI_TTM']=(temp['NIexACC']+temp['NIexACC'].shift(3)+temp['NIexACC'].shift(6)+temp['NIexACC'].shift(9))\
								/temp['NET_INCOME_TTM']

		temp['NIexACConASSET']=temp['NIexACC']*2/(temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(3))
		temp['NIexACConASSET_SEMI']=(temp['NIexACC']+temp['NIexACC'].shift(3))*2/(temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(6))
		temp['NIexACConASSET_TTM']=(temp['NIexACC']+temp['NIexACC'].shift(3)+temp['NIexACC'].shift(6)+temp['NIexACC'].shift(9))*2/\
								   (temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(12))

		# Investment Rate
		# temp['INVESTRATE'] = temp['CF_CAP_EXPEND_PRPTY_ADD'].fillna(0) / tot_assets.shift(12)
		temp['ASSETGROWTH'] = (tot_assets - tot_assets.shift(12)) / tot_assets.shift(12)
		temp['BVGROWTH'] = temp['TOT_EQUITY'] / temp['TOT_EQUITY'].shift(12) - 1

		# solvency ratios
		temp['WORKING_CAPITAL'] = temp['CURRASSET'] - temp['CURRLIAB']
		temp['CURRENT_RATIO'] = temp['CURRASSET'] / temp['CURRLIAB']
		temp['QUICK_RATIO'] = temp['BS_CASH_CASH_EQUIVALENTS_AND_STI'] / temp['CURRLIAB']
		temp['CASH_RATIO'] = temp['BS_CASH_NEAR_CASH_ITEM'] / temp['CURRLIAB']

		#OPERATING CASHFLOW
		temp['OPCF']=temp['NET_INCOME']+temp['CF_DEPR_AMORT']-(temp['WORKING_CAPITAL']-temp['WORKING_CAPITAL'].shift(3))
		temp['OPCF_SEMI']=temp['NET_INCOME_SEMI']+temp['CF_DEPR_AMORT']+temp['CF_DEPR_AMORT'].shift(3)\
						  -(temp['WORKING_CAPITAL']-temp['WORKING_CAPITAL'].shift(6))
		temp['OPCF_TTM']=temp['NET_INCOME_TTM']+temp['CF_DEPR_AMORT']+temp['CF_DEPR_AMORT'].shift(3)+\
						 temp['CF_DEPR_AMORT'].shift(6)+temp['CF_DEPR_AMORT'].shift(9)-\
						 (temp['WORKING_CAPITAL']-temp['WORKING_CAPITAL'].shift(12))

		# Profitability
		temp['EBITDAOA'] = temp['EBITDA'] * 2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['EBITDAOE'] = temp['EBITDA'] * 2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
		temp['EBITOA'] = temp['EBIT'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['EBITOE'] = temp['EBIT'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
		temp['EBTDAOA'] = temp['EBT'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['EBTDAOE'] = temp['EBT'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
		temp['ROE'] = temp['NET_INCOME'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
		temp['ROA'] = temp['NET_INCOME'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['GPOA'] = temp['GROSS_PROFIT']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['GPOE'] = temp['GROSS_PROFIT']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))



		# temp['CFOA'] = temp['CF_CASH_FROM_OPER'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBITDAOA_SEMI'] = temp['EBITDA_SEMI'] * 2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['EBITDAOE_SEMI'] = temp['EBITDA_SEMI'] * 2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['EBITOA_SEMI'] = temp['EBIT_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['EBITOE_SEMI'] = temp['EBIT_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['EBTOA_SEMI'] = temp['EBT_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['EBTOE_SEMI'] = temp['EBT_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['ROE_SEMI'] = temp['NET_INCOME_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['ROA_SEMI'] = temp['NET_INCOME_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['GPOA_SEMI'] = temp['GROSS_PROFIT_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['GPOE_SEMI'] = temp['GROSS_PROFIT_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))

		temp['EBITDAOA_TTM'] = temp['EBITDA_TTM'] * 2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBITDAOE_TTM'] = temp['EBITDA_TTM'] * 2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['EBITOA_TTM'] = temp['EBIT_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBITOE_TTM'] = temp['EBIT_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['EBTOA_TTM'] = temp['EBT_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBTOE_TTM'] = temp['EBT_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['ROE_TTM'] = temp['NET_INCOME_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['ROA_TTM'] = temp['NET_INCOME_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['GPOA_TTM'] = temp['GROSS_PROFIT_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['GPOE_TTM'] = temp['GROSS_PROFIT_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))

		temp['OPCFOA']=temp['OPCF']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['OPCFOA_SEMI']=temp['OPCF_SEMI']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['OPCFOA_TTM']=temp['OPCF_TTM']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['OPCFOE']=temp['OPCF']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
		temp['OPCFOE_SEMI']=temp['OPCF_SEMI']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['OPCFOE_TTM']=temp['OPCF_TTM']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))

		# Margin
		temp['GROSS_PROFIT_MARGIN'] = temp['GROSS_PROFIT'] / temp['SALES_REV_TURN']
		temp['OPCF_MARGIN'] = temp['OPCF'] / temp['SALES_REV_TURN']
		temp['NET_MARGIN'] = temp['NET_INCOME'] / temp['SALES_REV_TURN']

		temp['GROSS_PROFIT_MARGIN_SEMI'] = temp['GROSS_PROFIT_SEMI'] / temp['SALES_REV_TURN_SEMI']
		temp['OPCF_MARGIN_SEMI'] = temp['OPCF_SEMI'] / temp['SALES_REV_TURN_SEMI']
		temp['NET_MARGIN_SEMI'] = temp['NET_INCOME_SEMI'] / temp['SALES_REV_TURN_SEMI']

		temp['GROSS_PROFIT_MARGIN_TTM'] = temp['GROSS_PROFIT_TTM'] / temp['SALES_REV_TURN_TTM']
		temp['OPCF_MARGIN_TTM'] = temp['OPCF_TTM'] / temp['SALES_REV_TURN_TTM']
		temp['NET_MARGIN_TTM'] = temp['NET_INCOME_TTM'] / temp['SALES_REV_TURN_TTM']

		# Growth
		temp['ROAGROWTH'] = (temp['NET_INCOME']-temp['NET_INCOME'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['REVOAGROWTH'] = (temp['SALES_REV_TURN']-temp['SALES_REV_TURN'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['OPCFOAGROWTH'] = (temp['OPCF']-temp['OPCF'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['GPOAGROWTH'] = (temp['GROSS_PROFIT']-temp['GROSS_PROFIT'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITDAOAGROWTH'] = (temp['EBITDA']-temp['EBITDA'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITOAGROWTH'] = (temp['EBIT']-temp['EBIT'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBTOAGROWTH'] = (temp['EBT']-temp['EBT'].shift(12))/temp['BS_TOT_ASSET'].shift(12)

		temp['ROEGROWTH'] = (temp['NET_INCOME']-temp['NET_INCOME'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['REVOEGROWTH'] = (temp['SALES_REV_TURN']-temp['SALES_REV_TURN'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['OPCFOEGROWTH'] = (temp['OPCF']-temp['OPCF'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['GPOEGROWTH'] = (temp['GROSS_PROFIT']-temp['GROSS_PROFIT'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITDAOEGROWTH'] = (temp['EBITDA']-temp['EBITDA'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITOEGROWTH'] = (temp['EBIT']-temp['EBIT'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBTOEGROWTH'] = (temp['EBT']-temp['EBT'].shift(12))/temp['TOT_EQUITY'].shift(12)

		temp['ROAGROWTH_SEMI'] = (temp['NET_INCOME_SEMI']-temp['NET_INCOME_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['REVOAGROWTH_SEMI'] = (temp['SALES_REV_TURN_SEMI']-temp['SALES_REV_TURN_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['OPCFOAGROWTH_SEMI'] = (temp['OPCF_SEMI']-temp['OPCF_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['GPOAGROWTH_SEMI'] = (temp['GROSS_PROFIT_SEMI']-temp['GROSS_PROFIT_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITDAOAGROWTH_SEMI'] = (temp['EBITDA_SEMI']-temp['EBITDA_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITOAGROWTH_SEMI'] = (temp['EBIT_SEMI']-temp['EBIT_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBTOAGROWTH_SEMI'] = (temp['EBT_SEMI']-temp['EBT_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)

		temp['ROEGROWTH_SEMI'] = (temp['NET_INCOME_SEMI']-temp['NET_INCOME_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['REVOEGROWTH_SEMI'] = (temp['SALES_REV_TURN_SEMI']-temp['SALES_REV_TURN_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['OPCFOEGROWTH_SEMI'] = (temp['OPCF_SEMI']-temp['OPCF_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['GPOEGROWTH_SEMI'] = (temp['GROSS_PROFIT_SEMI']-temp['GROSS_PROFIT_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITDAOEGROWTH_SEMI'] = (temp['EBITDA_SEMI']-temp['EBITDA_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITOEGROWTH_SEMI'] = (temp['EBIT_SEMI']-temp['EBIT_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBTOEGROWTH_SEMI'] = (temp['EBT_SEMI']-temp['EBT_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)

		temp['ROAGROWTH_TTM'] = (temp['NET_INCOME_TTM']-temp['NET_INCOME_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['REVOAGROWTH_TTM'] = (temp['SALES_REV_TURN_TTM']-temp['SALES_REV_TURN_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['OPCFOAGROWTH_TTM'] = (temp['OPCF_TTM']-temp['OPCF_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['GPOAGROWTH_TTM'] = (temp['GROSS_PROFIT_TTM']-temp['GROSS_PROFIT_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITDAOAGROWTH_TTM'] = (temp['EBITDA_TTM']-temp['EBITDA_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITOAGROWTH_TTM'] = (temp['EBIT_TTM']-temp['EBIT_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBTOAGROWTH_TTM'] = (temp['EBT_TTM']-temp['EBT_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)

		temp['ROEGROWTH_TTM'] = (temp['NET_INCOME_TTM']-temp['NET_INCOME_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['REVOEGROWTH_TTM'] = (temp['SALES_REV_TURN_TTM']-temp['SALES_REV_TURN_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['OPCFOEGROWTH_TTM'] = (temp['OPCF_TTM']-temp['OPCF_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['GPOEGROWTH_TTM'] = (temp['GROSS_PROFIT_TTM']-temp['GROSS_PROFIT_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITDAOEGROWTH_TTM'] = (temp['EBITDA_TTM']-temp['EBITDA_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITOEGROWTH_TTM'] = (temp['EBIT_TTM']-temp['EBIT_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBTOEGROWTH_TTM'] = (temp['EBT_TTM']-temp['EBT_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)

		# Earnings Variability
		def earnval(array):
			l = int(len(array) / 3)
			x = np.array([array[a * 3] for a in range(l)])
			y = np.nanstd(x)
			return y

		# temp['EARNVAR'] = temp['EARNGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROAVAR'] = temp['ROAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROEVAR'] = temp['ROEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOAVAR'] = temp['OPCFOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOEVAR'] = temp['OPCFOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOAVAR'] = temp['GPOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOEVAR'] = temp['GPOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDAVAR'] = temp['EBITDAOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDEVAR'] = temp['EBITDAOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)

		# temp['EARNVAR_SEMI'] = temp['EARNGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROAVAR_SEMI'] = temp['ROAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROEVAR_SEMI'] = temp['ROEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOAVAR_SEMI'] = temp['OPCFOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOEVAR_SEMI'] = temp['OPCFOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOAVAR_SEMI'] = temp['GPOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOEVAR_SEMI'] = temp['GPOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDAVAR_SEMI'] = temp['EBITDAOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDEVAR_SEMI'] = temp['EBITDAOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)

		# temp['EARNVAR_TTM'] = temp['EARNGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROAVAR_TTM'] = temp['ROAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROEVAR_TTM'] = temp['ROEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOAVAR_TTM'] = temp['OPCFOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOEVAR_TTM'] = temp['OPCFOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOAVAR_TTM'] = temp['GPOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOEVAR_TTM'] = temp['GPOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDAVAR_TTM'] = temp['EBITDAOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDEVAR_TTM'] = temp['EBITDAOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)

		#inventory turnover
		temp['INVENTORY_TO'] = temp['IS_COG_AND_SERVICES_SOLD'] * 2 / (temp['BS_INVENTORIES'] + temp['BS_INVENTORIES'].shift(3))
		temp['INVENTORY_TO_SEMI'] = temp['IS_COG_AND_SERVICES_SOLD_SEMI'] * 2 / (
					temp['BS_INVENTORIES'] + temp['BS_INVENTORIES'].shift(6))
		temp['INVENTORY_TO_TTM'] = temp['IS_COG_AND_SERVICES_SOLD_TTM'] * 2 / (
					temp['BS_INVENTORIES'] + temp['BS_INVENTORIES'].shift(12))

		#RnD RATIO
		temp['RnD_RATIO'] = temp['IS_RD_EXPEND'] / temp['SALES_REV_TURN']
		temp['RnD_onASSET'] = temp['IS_RD_EXPEND'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['RnD_onEQUITY'] = temp['IS_RD_EXPEND'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))

		temp['RnD_RATIO_SEMI'] = temp['IS_RD_EXPEND_SEMI'] / temp['SALES_REV_TURN_SEMI']
		temp['RnD_onASSET_SEMI'] = temp['IS_RD_EXPEND_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['RnD_onEQUITY_SEMI'] = temp['IS_RD_EXPEND_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))

		temp['RnD_RATIO_TTM'] = temp['IS_RD_EXPEND_TTM'] / temp['SALES_REV_TURN_TTM']
		temp['RnD_onASSET_TTM'] = temp['IS_RD_EXPEND_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['RnD_onEQUITY_TTM'] = temp['IS_RD_EXPEND_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))

		# Valuation
		if result.shape[0] > 0:
			temp['BTM'] = temp['TOT_EQUITY'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
			temp['ETP'] = temp['NET_INCOME'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
			temp['STP'] = temp['SALES_REV_TURN'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
			temp['ETP_SEMI'] = temp['NET_INCOME_SEMI'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
			temp['STP_SEMI'] = temp['SALES_REV_TURN_SEMI'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
			temp['ETP_TTM'] = temp['NET_INCOME_TTM'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
			temp['STP_TTM'] = temp['SALES_REV_TURN_TTM'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
			# temp['CFTP'] = temp['CF_CASH_FROM_OPER'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
		else:
			temp['BTM'] = np.nan
			temp['ETP'] = np.nan
			temp['STP'] = np.nan
			temp['ETP_SEMI'] = np.nan
			temp['STP_SEMI'] = np.nan
			temp['ETP_TTM'] = np.nan
			temp['STP_TTM'] = np.nan
			# temp['CFTP'] = np.nan

		# Leverage
		temp['LEV'] = temp['BS_TOT_LIAB2'] / temp['BS_TOT_ASSET']
		temp['LONGTERM_DEBTRATIO'] = temp['BS_LONG_TERM_BORROWINGS'] / temp['BS_TOT_ASSET']
		temp['LONGTERM_LIABRATIO'] = temp['LONG_TERM_LIAB'] / temp['BS_TOT_ASSET']

		# Interest Coverage
		temp['IE_Prop'] = np.where(temp['EBIT'] > 0, temp['IS_INT_EXPENSE'] / temp['EBIT'], np.where(pd.isna(temp['IS_INT_EXPENSE']), 0, 1))
		temp['IE_Prop_SEMI'] = np.where(temp['EBIT_SEMI'] > 0, temp['IS_INT_EXPENSE_SEMI'] / temp['EBIT_SEMI'],
								   np.where(pd.isna(temp['IS_INT_EXPENSE_SEMI']), 0, 1))
		temp['IE_Prop_TTM'] = np.where(temp['EBIT_TTM'] > 0, temp['IS_INT_EXPENSE_TTM'] / temp['EBIT_TTM'],
								   np.where(pd.isna(temp['IS_INT_EXPENSE_TTM']), 0, 1))

		#bankruptcy risk
		temp['Z_SCORE']=1.2*temp['WORKING_CAPITAL']/temp['BS_TOT_ASSET']+1.4*temp['BS_RETAIN_EARN']/temp['BS_TOT_ASSET']\
						+3.3*temp['EBIT']/temp['BS_TOT_ASSET']+0.6*(temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * \
						1000000/temp['BS_TOT_LIAB2']+1*temp['SALES_REV_TURN']/temp['BS_TOT_ASSET']

		# REITs Factors
		# temp['NET_OPER_INCOME_to_Assets'] = temp['NET_OPER_INCOME'] / temp['BS_TOT_ASSET']
		# temp['AFFO_to_Assets'] = (temp['CF_CASH_FROM_OPER'] - temp['CF_CAP_EXPEND_PRPTY_ADD']) / temp['BS_TOT_ASSET']

		temp = temp.set_index('effective_date')
		temp = temp.resample('1M').last().ffill()

		temp = temp.drop(columns=['symbol', 'EQY_SH_OUT_RAW'])

		if result.shape[0] > 0:
			result = pd.merge(left=result, right=temp, left_index=True, right_index=True, how='outer')
		else:
			result = temp

	if result.shape[0] > 0:
		result = result.ffill()

	result.to_csv(os.path.join(input_path, "Factors", symbol + ".csv"))
	print(os.path.join(input_path, "Factors", symbol + ".csv")+'writen')
	# return result





def ANNOUNCEMENT_DT_processing(array):
    array = str(int(array))
    return array[0:4] + '/' + array[4:6] + '/' + array[6:8]

def cal_factors_cn(symbol, rebalance_freq,input_path):
    cn_fin_path = os.path.join(input_path, "Financial")
    cn_tra_path = os.path.join(input_path, "trading")

    temp = pd.read_csv(os.path.join(cn_tra_path, symbol + ".csv"), parse_dates=[0], index_col=0)
    temp = temp[['PX_LAST', 'PX_LAST_RAW', 'PX_VOLUME_RAW', 'EQY_SH_OUT_RAW']]
    temp2 = pd.DataFrame(index=pd.date_range(temp.index[0], temp.index[-1], freq='D'))
    temp2['EQY_SH_OUT_RAW'] = temp['EQY_SH_OUT_RAW']
    temp2['PX_LAST_RAW'] = temp['PX_LAST_RAW']
    temp2['PX_LAST']=temp['PX_LAST']
    temp2 = temp2.ffill().bfill()
    result=pd.DataFrame()

    temp = pd.read_csv(os.path.join(cn_fin_path, symbol + ".csv"),parse_dates=[0],index_col=0)
    column_names = {
        'bps_adjust':'EQUITY_PERSHARE',
        'lt_borrow':'LONG_TERM_BORROW',
        'qfa_eps':'EPS',
        'qfa_net_cash_flows_oper_act':'CF_CASH_FROM_OPER',
        'qfa_net_profit_is':'NET_INCOME',
        'qfa_opprofit':'EBIT',
        'qfa_tot_oper_rev':'SALES_REV_TURN',
        'tot_assets':'BS_TOT_ASSET',
        'tot_equity':'TOT_EQUITY',
        'tot_liab':'BS_TOT_LIAB2'
    }

    if temp.shape[0] > 0:
        temp['symbol'] = symbol
        temp = temp.rename(columns=column_names)
        temp['ANNOUNCEMENT_DT'] = None
        # temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].map(lambda x: np.nan if pd.isna(x) else datetime.strptime(x, "%Y-%m-%d"))
        temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].dropna().apply(ANNOUNCEMENT_DT_processing)
        if temp[['ANNOUNCEMENT_DT']].dropna().shape[0] > 0:
            temp['effective_date'] = np.where(pd.isna(temp['ANNOUNCEMENT_DT']), temp.index + timedelta(days=90),
                                              temp['ANNOUNCEMENT_DT'])
        else:
            temp['effective_date'] = temp.index + timedelta(days=90)
        temp.index.name = 'date'
        temp['effective_date']=pd.to_datetime(temp['effective_date'], format='%Y/%m/%d', errors='ignore')
        temp = temp.reset_index()
        temp = temp.set_index('effective_date')

        temp['EQY_SH_OUT_RAW'] = temp2['EQY_SH_OUT_RAW']
        temp['PX_LAST_RAW'] = temp2['PX_LAST_RAW']
        temp['PX_LAST']=temp2['PX_LAST']

        temp = temp.reset_index()
        temp = temp.set_index('date')
        temp['datadate'] = temp.index
        temp = temp.resample('1M').last().ffill()

        temp['SALES_REV_TURN_SEMI'] = temp['SALES_REV_TURN'] + temp['SALES_REV_TURN'].shift(3)
        temp['SALES_REV_TURN_TTM'] = temp['SALES_REV_TURN'] + temp['SALES_REV_TURN'].shift(3)+temp['SALES_REV_TURN'].shift(6)+temp['SALES_REV_TURN'].shift(9)
        temp['EBIT_SEMI'] = temp['EBIT'] + temp['EBIT'].shift(3)
        temp['EBIT_TTM'] = temp['EBIT'] + temp['EBIT'].shift(3)+ temp['EBIT'].shift(6)+ temp['EBIT'].shift(9)
        temp['NET_INCOME_SEMI'] = temp['NET_INCOME'] + temp['NET_INCOME'].shift(3)
        temp['NET_INCOME_TTM'] = temp['NET_INCOME'] + temp['NET_INCOME'].shift(3)+ temp['NET_INCOME'].shift(6)+temp['NET_INCOME'].shift(9)
        temp['CF_CASH_FROM_OPER_SEMI'] = temp['CF_CASH_FROM_OPER'] + temp['CF_CASH_FROM_OPER'].shift(3)
        temp['CF_CASH_FROM_OPER_TTM'] = temp['CF_CASH_FROM_OPER'] + temp['CF_CASH_FROM_OPER'].shift(3)+ temp['CF_CASH_FROM_OPER'].shift(6)+temp['CF_CASH_FROM_OPER'].shift(9)

        # Investment Rate
        temp['ASSETGROWTH'] = (temp['BS_TOT_ASSET'] - temp['BS_TOT_ASSET'].shift(12)) / temp['BS_TOT_ASSET'].shift(12)
        temp['BVGROWTH'] = temp['TOT_EQUITY'] / temp['TOT_EQUITY'].shift(12) - 1

        # Profitability
        temp['REVOA'] = temp['SALES_REV_TURN'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
        temp['REVOE'] = temp['SALES_REV_TURN'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
        temp['EBITOA'] = temp['EBIT'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
        temp['EBITOE'] = temp['EBIT'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
        temp['ROE'] = temp['NET_INCOME'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
        temp['ROA'] = temp['NET_INCOME'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
        temp['OPCFOA']=temp['CF_CASH_FROM_OPER']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
        temp['OPCFOE']=temp['CF_CASH_FROM_OPER']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))

        temp['REVOA_SEMI'] = temp['SALES_REV_TURN_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
        temp['REVOE_SEMI'] = temp['SALES_REV_TURN_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
        temp['EBITOA_SEMI'] = temp['EBIT_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
        temp['EBITOE_SEMI'] = temp['EBIT_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
        temp['ROE_SEMI'] = temp['NET_INCOME_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
        temp['ROA_SEMI'] = temp['NET_INCOME_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
        temp['OPCFOA_SEMI']=temp['CF_CASH_FROM_OPER_SEMI']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
        temp['OPCFOE_SEMI']=temp['CF_CASH_FROM_OPER_SEMI']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))


        temp['REVOA_TTM'] = temp['SALES_REV_TURN_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
        temp['REVOE_TTM'] = temp['SALES_REV_TURN_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
        temp['EBITOA_TTM'] = temp['EBIT_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
        temp['EBITOE_TTM'] = temp['EBIT_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
        temp['ROE_TTM'] = temp['NET_INCOME_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
        temp['ROA_TTM'] = temp['NET_INCOME_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
        temp['OPCFOA_TTM']=temp['CF_CASH_FROM_OPER_TTM']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
        temp['OPCFOE_TTM']=temp['CF_CASH_FROM_OPER_TTM']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))

        # Margin
        temp['EBIT_MARGIN'] = temp['EBIT'] / temp['SALES_REV_TURN']
        temp['OPCF_MARGIN'] = temp['CF_CASH_FROM_OPER'] / temp['SALES_REV_TURN']
        temp['NET_MARGIN'] = temp['NET_INCOME'] / temp['SALES_REV_TURN']

        temp['EBIT_MARGIN_SEMI'] = temp['EBIT_SEMI'] / temp['SALES_REV_TURN_SEMI']
        temp['OPCF_MARGIN_SEMI'] = temp['CF_CASH_FROM_OPER_SEMI'] / temp['SALES_REV_TURN_SEMI']
        temp['NET_MARGIN_SEMI'] = temp['NET_INCOME_SEMI'] / temp['SALES_REV_TURN_SEMI']

        temp['EBIT_MARGIN_TTM'] = temp['EBIT_TTM'] / temp['SALES_REV_TURN_TTM']
        temp['OPCF_MARGIN_TTM'] = temp['CF_CASH_FROM_OPER_TTM'] / temp['SALES_REV_TURN_TTM']
        temp['NET_MARGIN_TTM'] = temp['NET_INCOME_TTM'] / temp['SALES_REV_TURN_TTM']

        # Growth
        temp['ROAGROWTH'] = (temp['NET_INCOME']-temp['NET_INCOME'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['REVOAGROWTH'] = (temp['SALES_REV_TURN']-temp['SALES_REV_TURN'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['OPCFOAGROWTH'] = (temp['CF_CASH_FROM_OPER']-temp['CF_CASH_FROM_OPER'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['EBITOAGROWTH'] = (temp['EBIT']-temp['EBIT'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['ROEGROWTH'] = (temp['NET_INCOME']-temp['NET_INCOME'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['REVOEGROWTH'] = (temp['SALES_REV_TURN']-temp['SALES_REV_TURN'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['OPCFOEGROWTH'] = (temp['CF_CASH_FROM_OPER']-temp['CF_CASH_FROM_OPER'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['EBITOEGROWTH'] = (temp['EBIT']-temp['EBIT'].shift(12))/temp['TOT_EQUITY'].shift(12)

        temp['ROAGROWTH_SEMI'] = (temp['NET_INCOME_SEMI']-temp['NET_INCOME_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['REVOAGROWTH_SEMI'] = (temp['SALES_REV_TURN_SEMI']-temp['SALES_REV_TURN_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['OPCFOAGROWTH_SEMI'] = (temp['CF_CASH_FROM_OPER_SEMI']-temp['CF_CASH_FROM_OPER_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['EBITOAGROWTH_SEMI'] = (temp['EBIT_SEMI']-temp['EBIT_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['ROEGROWTH_SEMI'] = (temp['NET_INCOME_SEMI']-temp['NET_INCOME_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['REVOEGROWTH_SEMI'] = (temp['SALES_REV_TURN_SEMI']-temp['SALES_REV_TURN_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['OPCFOEGROWTH_SEMI'] = (temp['CF_CASH_FROM_OPER_SEMI']-temp['CF_CASH_FROM_OPER_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['EBITOEGROWTH_SEMI'] = (temp['EBIT_SEMI']-temp['EBIT_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)


        temp['ROAGROWTH_TTM'] = (temp['NET_INCOME_TTM']-temp['NET_INCOME_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['REVOAGROWTH_TTM'] = (temp['SALES_REV_TURN_TTM']-temp['SALES_REV_TURN_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['OPCFOAGROWTH_TTM'] = (temp['CF_CASH_FROM_OPER_TTM']-temp['CF_CASH_FROM_OPER_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['EBITOAGROWTH_TTM'] = (temp['EBIT_TTM']-temp['EBIT_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
        temp['ROEGROWTH_TTM'] = (temp['NET_INCOME_TTM']-temp['NET_INCOME_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['REVOEGROWTH_TTM'] = (temp['SALES_REV_TURN_TTM']-temp['SALES_REV_TURN_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['OPCFOEGROWTH_TTM'] = (temp['CF_CASH_FROM_OPER_TTM']-temp['CF_CASH_FROM_OPER_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
        temp['EBITOEGROWTH_TTM'] = (temp['EBIT_TTM']-temp['EBIT_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)

        # Earnings Variability
        def earnval(array):
            l = int(len(array) / 3)
            x = np.array([array[a * 3] for a in range(l)])
            y = np.nanstd(x)
            return y

        # temp['EARNVAR'] = temp['EARNGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
        temp['ROAVAR'] = temp['ROAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
        temp['ROEVAR'] = temp['ROEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
        temp['REVVAR'] = temp['REVOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
        temp['REVVAR'] = temp['REVOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
        temp['OPCFOAVAR'] = temp['OPCFOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
        temp['OPCFOEVAR'] = temp['OPCFOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
        temp['EBITOAVAR'] = temp['EBITOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
        temp['EBITOEVAR'] = temp['EBITOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)

        temp['ROAVAR_SEMI'] = temp['ROAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
        temp['ROEVAR_SEMI'] = temp['ROEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
        temp['REVVAR_SEMI'] = temp['REVOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
        temp['REVVAR_SEMI'] = temp['REVOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
        temp['OPCFOAVAR_SEMI'] = temp['OPCFOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
        temp['OPCFOEVAR_SEMI'] = temp['OPCFOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
        temp['EBITOAVAR_SEMI'] = temp['EBITOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
        temp['EBITOEVAR_SEMI'] = temp['EBITOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)

        # temp['EARNVAR_TTM'] = temp['EARNGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
        temp['ROAVAR_TTM'] = temp['ROAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
        temp['ROEVAR_TTM'] = temp['ROEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
        temp['REVVAR_TTM'] = temp['REVOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
        temp['REVVAR_TTM'] = temp['REVOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
        temp['OPCFOAVAR_TTM'] = temp['OPCFOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
        temp['OPCFOEVAR_TTM'] = temp['OPCFOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
        temp['EBITOAVAR_TTM'] = temp['EBITOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
        temp['EBITOEVAR_TTM'] = temp['EBITOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)

        # Valuation
        temp['BTM'] = temp['TOT_EQUITY'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
        temp['ETP'] = temp['NET_INCOME'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
        temp['ETP_SEMI'] = temp['NET_INCOME_SEMI'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
        temp['STP'] = temp['SALES_REV_TURN'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
        temp['STP_SEMI'] = temp['SALES_REV_TURN_SEMI'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
        temp['ETP_TTM'] = temp['NET_INCOME_TTM'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
        temp['STP_TTM'] = temp['SALES_REV_TURN_TTM'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
        # temp['CFTP'] = temp['CF_CASH_FROM_OPER'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])

        # Leverage
        temp['LEV'] = temp['BS_TOT_LIAB2'] / temp['BS_TOT_ASSET']

        temp = temp.set_index('effective_date')
        temp = temp.resample('1M').last().ffill()
        if result.shape[0] > 0:
            result = pd.merge(left=result, right=temp, left_index=True, right_index=True, how='outer')
        else:
            result = temp

    if result.shape[0] > 0:
        result = result.ffill()

    result.to_csv(os.path.join(input_path, "Factors", symbol + ".csv"))
    print(os.path.join(input_path, "Factors", symbol + ".csv")+'written')
    # return result


if __name__ == "__main__":
    underlying_types=['CN_STOCK','US_STOCK']
    underlying_types=['CN_STOCK']
    for underlying_type in underlying_types:
        if underlying_type == "US_STOCK":
            input_path = os.path.join(addpath.data_path, "us_data")
        elif underlying_type == "HK_STOCK":
            input_path = os.path.join(addpath.data_path, "hk_data")
        elif underlying_type == "CN_STOCK":
            input_path = os.path.join(addpath.data_path, "cn_data")

        iu_path = os.path.join(input_path, "investment_univ")
        iu_files = os.listdir(iu_path)
        if '.DS_Store' in iu_files:
            iu_files.remove('.DS_Store')
        symbol_list = []
        for iu_file in iu_files:
            print(iu_file)
            tmp = pd.read_csv(os.path.join(iu_path, iu_file))
            tmp_list = tmp['symbol'].tolist()
            symbol_list = symbol_list + tmp_list
        symbol_list = list(set(symbol_list))
        print(len(symbol_list))

        rebalance_freq = 'MONTHLY'

        pool = multiprocessing.Pool()
        if underlying_type == "US_STOCK":
            for symbol in symbol_list:
                print(symbol)
                # pool.apply_async(cal_factors, args=(symbol, rebalance_freq,))
                # print('Sub-processes begins!')
                cal_factors_us(symbol, rebalance_freq,input_path)
            pool.close()
            pool.join()
            print('Sub-processes done!')
        elif underlying_type == "CN_STOCK":
            for symbol in symbol_list:
                print(symbol)
                # pool.apply_async(cal_factors, args=(symbol, rebalance_freq,))
                # print('Sub-processes begins!')
                cal_factors_cn(symbol, rebalance_freq,input_path)
            pool.close()
            pool.join()
            print('Sub-processes done!')


        factors_path = os.path.join(input_path, "Factors")
        factors_files = os.listdir(factors_path)
        factors = {}
        if '.DS_Store' in iu_files:
            iu_files.remove('.DS_Store')

        for factors_file in factors_files:
            print(factors_file)
            symbol=factors_file[:-4]
            temp=pd.read_csv(os.path.join(factors_path, factors_file), parse_dates=[0],index_col=0)
            if len(temp) > 0:
                factors[symbol]=temp

        for iu_file in iu_files:
            date=datetime(int(iu_file[0:4]),int(iu_file[5:7]),int(iu_file[8:10]))
            print(date)
            tmp = pd.read_csv(os.path.join(iu_path, iu_file))
            tmp_list = tmp['symbol'].tolist()
            dataout=[]

            inve_list = []
            for symbol in tmp_list:
                # print(symbol)
                if symbol in list(factors.keys()):
                    if date in factors[symbol].index.tolist():
                        temp_symbol=factors[symbol].loc[[date],:]
                        dataout.append(temp_symbol)
                        inve_list.append(symbol)
            # inve_list=pd.DataFrame(inve_list)
            # inve_list.to_csv(os.path.join(iu_path, iu_file),index=False)
            dataout=pd.concat(dataout)
            # dataout=dataout.drop(columns=['YYYY','MM'])
            data=dataout
            CSfactors_path = os.path.join(input_path, "CS_Factors")
            data['MARKET_CAP']=data['EQY_SH_OUT_RAW']*data['PX_LAST_RAW']
            data['CLOSE']=data['PX_LAST_RAW']
            data.set_index(['symbol'],inplace=True)
            data.to_csv(os.path.join(CSfactors_path,iu_file))


