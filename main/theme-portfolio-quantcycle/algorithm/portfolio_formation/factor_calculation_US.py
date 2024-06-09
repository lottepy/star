import numpy as np
import pandas as pd
import os
from datetime import datetime, timedelta
from scipy.stats import skew
import calendar
import statsmodels.api as sm
from algorithm import addpath

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


def cal_factors(symbol, trading_data, financial_data, reference_data, rebalance_freq):

	# Mkt factor
	mkt_dt = reference_data["market_index"].copy()
	mkt_dt["market_index"] = mkt_dt["market_index"].pct_change()
	mkt_dt = mkt_dt.fillna(0)

	temp = trading_data[symbol].copy()
	if temp.shape[0] > 0:
		temp['symbol'] = symbol
		temp['Date'] = temp.index
		temp['YYYY'] = temp['Date'].map(lambda x: x.year)
		temp['MM'] = temp['Date'].map(lambda x: x.month)
		temp = temp.fillna(method='ffill')
		temp['ret_daily'] = temp['PX_LAST'].pct_change()
		temp['TURN'] = temp['PX_VOLUME'] / temp['EQY_SH_OUT']
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
		temp['ILLIQ'] = temp['ret_daily'].map(lambda x: abs(x)) / (temp['PX_VOLUME'] * temp['PX_LAST']) * 1000000
		result['ILLIQ'] = temp['ILLIQ'].resample('1m').mean().ffill()
		result['LIQU'] = result['ILLIQ'] - result['ILLIQ'].rolling(window=12, min_periods=6).apply(np.nanmean).shift()
		# Pastor and Stambaugh liquidity
		ps_raw = pd.merge(temp, mkt_dt[['market_index']], left_index=True, right_index=True)
		ps_raw['ex_ret'] = ps_raw['ret_daily'] - ps_raw['market_index']
		ps_raw['ex_ret_f1'] = ps_raw['ex_ret'].shift(-1)
		ps_raw['signed_dvol'] = ps_raw['ex_ret'].map(lambda x: np.sign(x)) * ps_raw['PX_VOLUME'] * ps_raw['PX_LAST']
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
		result['MARKET_CAP'] = result['CLOSE'] * result['EQY_SH_OUT']

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
		result['REVS10'] = (ADJCLOSE / ADJCLOSE.shift(10) - 1).resample('1M').fillna(method='ffill')
		result['REVS20'] = (ADJCLOSE / ADJCLOSE.shift(20) - 1).resample('1M').fillna(method='ffill')
		result['Momentum_2_7'] = result['ADJCLOSE'].shift(1) / result['ADJCLOSE'].shift(7) - 1
		result['Momentum_2_12'] = result['ADJCLOSE'].shift(1) / result['ADJCLOSE'].shift(12) - 1
		result['Momentum_2_15'] = result['ADJCLOSE'].shift(1) / result['ADJCLOSE'].shift(15) - 1
		result['Momentum_12_24'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(24) - 1
		result['Momentum_12_36'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(36) - 1
		result['Momentum_12_48'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(48) - 1
		result['Momentum_12_60'] = result['ADJCLOSE'].shift(11) / result['ADJCLOSE'].shift(60) - 1
		result['Momentum_24_36'] = result['ADJCLOSE'].shift(23) / result['ADJCLOSE'].shift(36) - 1
		result['Momentum_24_48'] = result['ADJCLOSE'].shift(23) / result['ADJCLOSE'].shift(48) - 1
		result['Momentum_24_60'] = result['ADJCLOSE'].shift(23) / result['ADJCLOSE'].shift(60) - 1
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
		result['1m_average_price'] = temp['PX_LAST'].resample('1m').mean().ffill()
		result['MaxRet'] = temp['PX_LAST'].pct_change().resample('1m').max().ffill()

		# Volatility and Skewness Factors
		# Realized Volatility
		daily_std = temp['ret_daily'].rolling(window=23).std(ddof=0)
		result['RealizedVol_1M'] = daily_std.resample('1m').last().ffill()
		daily_std = temp['ret_daily'].rolling(window=67).std(ddof=0)
		result['RealizedVol_3M'] = daily_std.resample('1m').last().ffill()
		daily_std = temp['ret_daily'].rolling(window=125).std(ddof=0)
		result['RealizedVol_6M'] = daily_std.resample('1m').last().ffill()
		daily_std = temp['ret_daily'].rolling(window=252).std(ddof=0)
		result['RealizedVol_12M'] = daily_std.resample('1m').last().ffill()
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

		temp2 = pd.DataFrame(index=pd.date_range(temp.index[0], temp.index[-1], freq='D'))
		temp2['EQY_SH_OUT'] = temp['EQY_SH_OUT']
		temp2['PX_LAST'] = temp['PX_LAST']
		temp2 = temp2.ffill()
	else:
		result = pd.DataFrame()

	temp = financial_data[symbol].copy()

	if temp.shape[0] > 0:
		temp['symbol'] = symbol
		temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].map(lambda x: np.nan if pd.isna(x) else datetime.strptime(x, "%Y-%m-%d"))
		if temp[['ANNOUNCEMENT_DT']].dropna().shape[0] > 0:
			temp['effective_date'] = np.where(pd.isna(temp['ANNOUNCEMENT_DT']), temp.index + timedelta(days=90),
											  temp['ANNOUNCEMENT_DT'])
		else:
			temp['effective_date'] = temp.index + timedelta(days=90)
		temp.index.name = 'date'
		temp = temp.reset_index()
		temp = temp.set_index('effective_date')
		if result.shape[0] > 0:
			temp['EQY_SH_OUT'] = temp2['EQY_SH_OUT']
			temp['PX_LAST'] = temp2['PX_LAST']
		else:
			temp['EQY_SH_OUT'] = np.nan
			temp['PX_LAST'] = np.nan
		temp = temp.reset_index()
		temp = temp.set_index('date')
		temp = temp.resample('1M').last().ffill()

		# Dividend Yield
		# temp['DVDYD'] = temp['DIVIDEND'] / temp['EQY_SH_OUT'] / (temp['PB'] * temp['BPSADJUST'])

		# Dividend Growth
		# temp['DVDGR'] = temp['DIVIDEND'] / temp['DIVIDEND'].shift(12) - 1

		# Negative Indicating Factors
		# # Total Accruals
		# non_cash_nwc = temp['CURRASSET'].fillna(0) - temp['CASH'].fillna(0) - temp['CURRLIAB'].fillna(0)
		tot_assets = temp['BS_TOT_ASSET']
		# temp['TOTALACCRUAL'] = 2.0 * (non_cash_nwc - non_cash_nwc.shift(12)) / (tot_assets + tot_assets.shift(12))
		# Investment Rate
		# temp['INVESTRATE'] = temp['CF_CAP_EXPEND_PRPTY_ADD'].fillna(0) / tot_assets.shift(12)
		temp['ASSETGROWTH'] = (tot_assets - tot_assets.shift(12)) / tot_assets.shift(12)
		temp['BVGROWTH'] = temp['TOT_EQUITY'] / temp['TOT_EQUITY'].shift(12) - 1

		# Growth
		temp['EARNGROWTH'] = temp['NET_INCOME'] / temp['NET_INCOME'].shift(12) - 1
		temp['REVENUEGROWTH'] = temp['SALES_REV_TURN'] / temp['SALES_REV_TURN'].shift(12) - 1
		temp['REVENUEGROWTH_exINV'] = (temp['SALES_REV_TURN']-temp['INVENTORY']) / \
									  (temp['SALES_REV_TURN'].shift(12)-temp['INVENTORY'].shift(12)) - 1

		temp['OPTCASHGROWTH'] = temp['CF_CASH_FROM_OPER'] / temp['CF_CASH_FROM_OPER'].shift(12) - 1

		# Earnings Variability
		def earnval(array):
			l = int(len(array) / 3)
			x = np.array([array[a * 3] for a in range(l)])
			y = np.nanstd(x)
			return y

		temp['EARNVAR'] = temp['EARNGROWTH'].rolling(window=36, min_periods=30).apply(earnval)

		#operating income
		temp['EBITDA']=temp['SALES_REV_TURN']-temp['OPER_EXPENSE']+temp['DEPRnAMOR']
		temp['EBIT'] = temp['SALES_REV_TURN'] - temp['OPER_EXPENSE']
		temp['EBT'] = temp['EBIT']-temp['INTEREST_EXP']
		temp['EBITDAonASSET']=temp['EBITDA']*2/(temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(12))
		temp['EBITDAonEQUITY']=temp['EBITDA']*2/(temp['TOT_EQUITY']+temp['TOT_EQUITY'].shift(12))
		temp['EBITonASSET'] = temp['EBIT'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBITonEQUITY'] = temp['EBIT'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['EBTDAonASSET'] = temp['EBT'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBTDAonEQUITY'] = temp['EBT'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))

		# Profitability
		temp['ROE'] = temp['NET_INCOME']*2 / (temp['TOT_EQUITY']+temp['TOT_EQUITY'].shift(12))
		temp['ROA'] = temp['NET_INCOME']*2 / (temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(12))
		temp['CFOA'] = temp['CF_CASH_FROM_OPER']*2 / (temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(12))
		temp['GROSS_PROFIT'] = (temp['REVENUE_TOT']-temp['COST_SOLD']) / temp['REVENUE_TOT']


		# solvency ratios
		temp['WORKING_CAPITAL'] = temp['CUR_ASSET']-temp['CUR_LIAB']
		temp['WORKING_CAPITALonASSET'] = temp['WORKING_CAPITAL']/temp['BS_TOT_ASSET']
		temp['CURRENT_RATIO']=temp['CUR_ASSET']/temp['CUR_LIAB']
		temp['QUICK_RATIO']=temp['CASHnSTINVES']/temp['CUR_LIAB']
		temp['CASH_RATIO'] = temp['CASH'] / temp['CUR_LIAB']

		#longterm debt/liab
		temp['LONGTERM_DEBTRATIO'] = temp['LONGTERM_DEBT'] / temp['BS_TOT_ASSET']
		temp['LONGTERM_LIABRATIO'] = temp['LONGTERM_LIAB'] / temp['BS_TOT_ASSET']

		#inventory turnover
		temp['INVENTORY_TO'] = temp['OPER_EXPENSE']*2 / (temp['INVENTORY']+temp['INVENTORY'].shift(12))

		#RnD RATIO
		temp['RnD_RATIO'] = temp['RnD_EXPENSE'] / temp['SALES_REV_TURN']
		temp['RnD_onASSET'] = temp['RnD_EXPENSE']*2 / (temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(12))
		temp['RnD_onEQUITY'] = temp['RnD_EXPENSE']*2 / (temp['TOT_EQUITY']+temp['TOT_EQUITY'].shift(12))


		# Valuation
		if result.shape[0] > 0:
			temp['BTM'] = temp['TOT_EQUITY'] / (temp['EQY_SH_OUT'] * temp['PX_LAST']) * 1000000
			temp['ETP'] = temp['NET_INCOME'] / (temp['EQY_SH_OUT'] * temp['PX_LAST']) * 1000000
			temp['STP'] = temp['SALES_REV_TURN'] / (temp['EQY_SH_OUT'] * temp['PX_LAST']) * 1000000
			temp['CFTP'] = temp['CF_CASH_FROM_OPER'] / (temp['EQY_SH_OUT'] * temp['PX_LAST']) * 1000000
		else:
			temp['BTM'] = np.nan
			temp['ETP'] = np.nan
			temp['STP'] = np.nan
			temp['CFTP'] = np.nan

		# Leverage
		temp['LEV'] = temp['BS_TOT_LIAB2'] / temp['BS_TOT_ASSET']

		# Interest Coverage
		temp['IE_Prop'] = np.where(temp['EBIT'] > 0, temp['IS_INT_EXPENSE'] / temp['EBIT'], np.where(pd.isna(temp['IS_INT_EXPENSE']), np.nan, 1))

		# REITs Factors
		temp['NET_OPER_INCOME_to_Assets'] = temp['NET_OPER_INCOME'] / temp['BS_TOT_ASSET']
		temp['AFFO_to_Assets'] = (temp['CF_CASH_FROM_OPER'] - temp['CF_CAP_EXPEND_PRPTY_ADD']) / temp['BS_TOT_ASSET']

		temp = temp.set_index('effective_date')
		temp = temp.resample('1M').last().ffill()

		temp = temp.drop(columns=['symbol', 'EQY_SH_OUT'])

		if result.shape[0] > 0:
			result = pd.merge(left=result, right=temp, left_index=True, right_index=True, how='outer')
		else:
			result = temp

	if result.shape[0] > 0:
		result = result.ffill()

	return result

