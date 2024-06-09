import pandas as pd
import numpy as np
from datetime import datetime
import datetime as dt

from statsmodels.tsa.stattools import adfuller, acf, pacf
from statsmodels.stats.outliers_influence import variance_inflation_factor
import statsmodels.api as sm
from scipy.stats import kurtosis, skew, ttest_ind
import matplotlib.pyplot as plt
from dateutil.relativedelta import relativedelta
from collections import defaultdict
import os
import json


def timeit(method):
	def timed(*args, **kw):
		ts = datetime.now()
		result = method(*args, **kw)
		time_elapsed = datetime.now() - ts
		print('Time elapsed (hh:mm:ss) {} for function {}'.format(time_elapsed, method.__name__))
		return result

	return timed


class DATA_ANALYZER:

	def __init__(self, end_date):
		self.end_date = end_date
		self.ROOT_PATH = 'https://aqm-algo.oss-cn-hongkong.aliyuncs.com/0_DymonFx/macro_data/'

		self.ROOT_PATH_FAST = self.ROOT_PATH + 'macro_fast_data/'
		self.ROOT_PATH_SLOW = self.ROOT_PATH + 'macro_slow_data/'

		self.ROOT_LOCAL = 'D:\PycharmData\magnum_FX_project\PHASE II/'
		self.ROOT_PATH_SLOW_NEAT = self.ROOT_LOCAL + 'slow_neat/'

		self.path_fast_derived = self.ROOT_LOCAL + 'fast_derived/'
		self.path_fast_aligned = self.ROOT_LOCAL + 'fast_aligned/'
		self.path_fast_derived_cross = self.ROOT_LOCAL + 'fast_derived_cross/'
		self.path_fast_slice = self.ROOT_LOCAL + 'fast_slice/'
		self.path_fast_combineSlice = self.ROOT_LOCAL + 'fast_combineSlice/'

		self.path_slow_monthly_byIndicator_byDate = self.ROOT_LOCAL + 'slow_monthly_byIndicator_byDate/'
		self.path_slow_quarterly_byIndicator_byDate = self.ROOT_LOCAL + 'slow_quarterly_byIndicator_byDate/'
		self.path_slow_monthly_byDate = self.ROOT_LOCAL + 'slow_monthly_byDate/'
		self.path_slow_quarterly_byDate = self.ROOT_LOCAL + 'slow_quarterly_byDate/'
		self.path_slow_monthly_slice = self.ROOT_LOCAL + 'slow_monthly_slice/'
		self.path_slow_quarterly_slice = self.ROOT_LOCAL + 'slow_quarterly_slice/'
		self.path_slow_monthly_combineSlice = self.ROOT_LOCAL + 'slow_monthly_combineSlice/'
		self.path_slow_quarterly_combineSlice = self.ROOT_LOCAL + 'slow_quarterly_combineSlice/'

		self.path_next_period_return = self.ROOT_LOCAL + 'next_period_return/'

		self.path_model = self.ROOT_LOCAL + 'model/'
		self.file_expected_return = self.path_model + 'expected_return.csv'
		self.file_r_squared = self.path_model + 'r_squared.csv'
		self.file_model_coefficient = self.path_model + 'model_coefficient.json'

		self.pairs = ['AUDUSD', 'EURUSD', 'GBPUSD', 'NZDUSD', 'USDCAD',
					  'USDCHF', 'USDJPY', 'USDNOK', 'USDSEK', 'USDSGD',
					  'USDTHB', 'USDKRW', 'USDINR', 'USDIDR', 'USDTWD']  # 11 spot + 4 NDF

	@timeit
	def unify_tickers_byWeb(self):
		import shutil
		path_daily = 'Z:/alioss/0_DymonFx\daily_data/'
		path_spot = path_daily + 'spot_T150/'
		path_NDF = path_daily + 'ndf_T150/'
		path_new = self.ROOT_LOCAL + 'daily_data/'

		mapping = {'IHN+1M T150 Curncy.csv': 'USDIDR T150 Curncy.csv',
				   'IRN+1M T150 Curncy.csv': 'USDINR T150 Curncy.csv',
				   'KWN+1M T150 Curncy.csv': 'USDKRW T150 Curncy.csv',
				   'NTN+1M T150 Curncy.csv': 'USDTWD T150 Curncy.csv'}

		for file in os.listdir(path_spot):
			shutil.copyfile(path_spot + file, path_new + file)
		for file in os.listdir(path_NDF):
			shutil.copyfile(path_NDF + file, path_new + file.replace(file, mapping[file]))

	@timeit
	def unify_tickers_byAPI(self):
		path_new = self.ROOT_LOCAL + 'daily_data/'
		from utils.fx_client_lib.fx_client import get_oss_data
		data_full, time = get_oss_data(ticker_list=self.pairs, start_date='2018-01-01', end_date=self.end_date)
		dates = [datetime(int(d[0]), int(d[1]), int(d[2])) for d in time[:, -3:]]
		for pair_i in range(data_full.shape[1]):
			pair = self.pairs[pair_i]
			data = data_full[:, pair_i, :]
			data_df = pd.DataFrame(data, index=dates, columns=['PRICE_LAST', 'PRICE_BID', 'PRICE_ASK'])
			data_df.to_csv(path_new + pair + '.csv')

	def get_currency_list(self):
		pairs = self.pairs
		currencies = []
		ccy2pair = defaultdict(list)
		for pair in pairs:
			c1 = pair[:3]
			c2 = pair[-3:]
			currencies.append(c1)
			currencies.append(c2)
			ccy2pair[c1].append(pair)
			ccy2pair[c2].append(pair)
		currencies = list(set(currencies))
		currencies.sort()
		return currencies, pairs, ccy2pair

	@timeit
	def get_daily_FX_prices(self):
		ROOT_DAILY_FX_PRICE = self.ROOT_LOCAL + 'daily_data/'
		path_FX_close = ROOT_DAILY_FX_PRICE

		self.FX_close_dict = {}
		for pair in self.pairs:
			parser = lambda x: datetime.strptime(x, '%Y-%m-%d')
			close_df = pd.read_csv(path_FX_close + pair + '.csv',
								   index_col=0, parse_dates=[0], date_parser=parser)
			close_df = close_df['PRICE_LAST']
			# print(f'pair:{pair}, missing_pct:{1-len(close_df.dropna())/len(close_df)}')
			self.FX_close_dict[pair] = close_df

	# fig = plt.figure()
	# plt.plot(close_df)
	# plt.show()

	def FX_TSA(self):
		# stationary, unit-root,
		# trend, seasonality, noise
		# normal distribution
		# ACF, PACF
		for pair in self.pairs:
			close_df = self.FX_close_dict[pair]

			raw_result = adfuller(close_df.values, autolag='AIC')
			# print(f'pair: {pair}, p-value: {raw_result[1]}')
			diff_result = adfuller(np.diff(close_df.values), autolag='AIC')
			# print(f'pair: {pair}, p-value: {diff_result[1]}')

			lag_acf = acf(np.diff(close_df), nlags=5, qstat=True, alpha=0.05)
			lag_pacf = pacf(np.diff(close_df), nlags=5, method='ols')

			# print(f'\t lag_acf: {lag_acf[0]}; lag_pacf: {lag_pacf}')

			log_returns = np.diff(np.log(close_df))

			k = kurtosis(log_returns)
			s = skew(log_returns)

	# print(f'\t kurtosis={k}, skew={s}')

	# fig = plt.figure()
	# plt.hist(log_returns, bins=100)
	# plt.title(pair)
	# plt.show()

	def get_fast_indicators(self):
		df = pd.read_csv('data/macro_fast_indicators.csv', index_col=0)
		self.fast_indicators = []
		for type in df.columns:
			self.fast_indicators += list(df[type].values)

	def get_slow_indicators(self):
		df = pd.read_csv('data/macro_slow_indicators.csv', index_col=0)
		self.slow_indicators = list(df['Bloomber Ticker'].values)

	def get_fast_indicators_by_ccy(self, ccy):
		df = pd.read_excel('data/macro_fast_indicators.xlsx', sheet_name='ticker')
		df = df.loc[ccy]
		return list(df.values)

	def get_slow_indicators_by_ccy(self, ccy, freq=None):
		df = pd.read_csv('data/macro_slow_indicators.csv')
		df = df.loc[df['Currency'] == ccy]
		if freq is not None:
			df = df.loc[df['BBG FREQ'] == freq]
		return list(df['Bloomber Ticker'].values)

	@timeit
	def organize_slow(self):
		abbr = {
			'PX_LAST': 'last',
			'ECO_RELEASE_DT': 'release_date',
			'FIRST_REVISION': 'revision',
			'FIRST_REVISION_DATE': 'revision_date',
			'FORECAST_STANDARD_DEVIATION': 'survey_std',
			'ACTUAL_RELEASE': 'actual_release',
			'BN_SURVEY_NUMBER_OBSERVATIONS': 'survey_n',
			'BN_SURVEY_MEDIAN': 'survey_median',
			'date': 'econ_date'
		}
		# files = os.listdir(self.ROOT_PATH_SLOW)
		files = os.listdir('Z:/alioss/0_DymonFx\macro_data\macro_slow_data/')  # hard code
		for file in files:
			# print(file)
			raw_file = file
			file = file.replace('%', '%25')
			file = file.replace(' ', '%20')
			# print(self.ROOT_PATH_SLOW + file)
			full_df = pd.read_csv(self.ROOT_PATH_SLOW + file)
			if full_df.empty:
				continue

			raw_fields = ['PX_LAST', 'ECO_RELEASE_DT', 'FIRST_REVISION', 'FIRST_REVISION_DATE',
						  'FORECAST_STANDARD_DEVIATION', 'ACTUAL_RELEASE', 'BN_SURVEY_NUMBER_OBSERVATIONS',
						  'BN_SURVEY_MEDIAN']
			raw_df = full_df[['date'] + raw_fields].copy()
			raw_df['release_type'] = '4R'

			A_fields = ['A_' + f for f in raw_fields]
			A_df = full_df[['date'] + A_fields].copy()
			A_df.columns = ['date'] + raw_fields
			A_df['release_type'] = '1A'

			P_fields = ['P_' + f for f in raw_fields]
			P_df = full_df[['date'] + P_fields].copy()
			P_df.columns = ['date'] + raw_fields
			P_df['release_type'] = '2P'

			F_fields = ['F_' + f for f in raw_fields]
			F_df = full_df[['date'] + F_fields].copy()
			F_df.columns = ['date'] + raw_fields
			F_df['release_type'] = '3F'

			combine_df = pd.concat([raw_df, A_df, P_df, F_df])
			combine_df.columns = [abbr[c] if c in abbr.keys() else c for c in combine_df.columns]
			combine_df = combine_df.sort_values(by=['econ_date', 'release_type'])
			combine_df.to_csv(self.ROOT_PATH_SLOW_NEAT + raw_file)

	def main_fast_data_summary(self):
		# number, start, end, mean, median, std
		summary_dict_all = {}
		# for ticker_i, ticker in enumerate(self.fast_tickers[70:]):
		for ticker_i, ticker in enumerate(self.fast_indicators):

			try:
				parser = lambda x: datetime.strptime(x, '%Y-%m-%d')
				data_df = pd.read_csv(self.ROOT_PATH_FAST + ticker + '.csv', index_col=0, parse_dates=[0],
									  date_parser=parser)
				summary_dict = data_df['PX_LAST'].describe().to_dict()
				if data_df.shape[0] > 2:
					summary_dict['start'] = data_df.index[0].strftime('%Y-%m-%d')
					summary_dict['end'] = data_df.index[-1].strftime('%Y-%m-%d')
				else:
					summary_dict['start'] = None
					summary_dict['end'] = None

				# frequency day
				index_dates = data_df.index
				freq_day_sum = 0
				for i in np.arange(len(index_dates) - 1):
					s = index_dates[i]
					e = index_dates[i + 1]
					freq_day_sum += (e - s).days
				freq_day_mean = freq_day_sum / (len(index_dates) - 1)
				summary_dict['freq_day'] = freq_day_mean

				# stationarity
				stationarity_result = adfuller(data_df['PX_LAST'].values)
				p_value = stationarity_result[1]
				# print('ADF Statistic: %f' % result[0])
				# print('p-value: %f' % result[1])
				# print('Critical Values:')
				# for key, value in result[4].items():
				# 	print('\t%s: %.3f' % (key, value))
				summary_dict['raw_stat_p'] = p_value
				if summary_dict['raw_stat_p'] > 0.05:
					stationarity_result = adfuller(np.diff(data_df['PX_LAST'].values))
					p_value = stationarity_result[1]
					summary_dict['diff_stat_p'] = p_value

				# missing value
				summary_dict['missing_pct'] = 1 - len(data_df['PX_LAST'].dropna()) / len(data_df['PX_LAST'])

				summary_dict_all[ticker] = summary_dict

			# # plot
			# print(pd.Series(summary_dict))
			# fig = plt.figure()
			# data_df['PX_LAST'].plot()
			# plt.title(ticker)
			# plt.show()
			except Exception as e:
				print('ERROR', ticker, e)

		summary_df = pd.DataFrame(summary_dict_all).T
		# print(summary_df.to_string())
		summary_df.to_csv('data/out_fast_stats.csv')
		return

	def main_slow_data_summary(self):
		# number, start, end, mean, median, std
		summary_dict_all = {}
		# for ticker_i, ticker in enumerate(self.fast_tickers[70:]):
		for ticker_i, ticker in enumerate(self.slow_indicators):
			# print(ticker)
			try:
				parser = lambda x: datetime.strptime(x, '%Y-%m-%d')
				data_df = pd.read_csv(self.ROOT_PATH_SLOW + ticker + '.csv', index_col=0, parse_dates=[0],
									  date_parser=parser)
				summary_dict = data_df['PX_LAST'].describe().to_dict()
				if data_df.shape[0] > 2:
					summary_dict['start'] = data_df.index[0].strftime('%Y-%m-%d')
					summary_dict['end'] = data_df.index[-1].strftime('%Y-%m-%d')
				else:
					summary_dict['start'] = None
					summary_dict['end'] = None

				# frequency day
				index_dates = data_df.index
				freq_day_sum = 0
				for i in np.arange(len(index_dates) - 1):
					s = index_dates[i]
					e = index_dates[i + 1]
					freq_day_sum += (e - s).days
				freq_day_mean = freq_day_sum / (len(index_dates) - 1)
				summary_dict['freq_day'] = freq_day_mean

				# # stationarity
				# stationarity_result = adfuller(data_df['PX_LAST'].values)
				# p_value = stationarity_result[1]
				# # print('ADF Statistic: %f' % result[0])
				# # print('p-value: %f' % result[1])
				# # print('Critical Values:')
				# # for key, value in result[4].items():
				# # 	print('\t%s: %.3f' % (key, value))
				#
				# summary_dict['raw_stat_p'] = p_value
				# if summary_dict['raw_stat_p'] > 0.05:
				# 	stationarity_result = adfuller(np.diff(data_df['PX_LAST'].values))
				# 	p_value = stationarity_result[1]
				# 	summary_dict['diff_stat_p'] = p_value

				# missing value
				summary_dict['missing_pct'] = 1 - len(data_df['PX_LAST'].dropna()) / len(data_df['PX_LAST'])

				summary_dict_all[ticker] = summary_dict

			# # plot
			# print(pd.Series(summary_dict))
			# fig = plt.figure()
			# data_df['PX_LAST'].plot()
			# plt.title(ticker)
			# plt.show()
			except Exception as e:
				print('ERROR', ticker, e)

		summary_df = pd.DataFrame(summary_dict_all).T
		# print(summary_df.to_string())
		summary_df.to_csv('data/out_slow_stats.csv')
		return

	def main_rolling_correlation_interpair(self):
		lb_win = 252 * 9  # number of days
		step = 60
		# start_full = datetime(2010, 1, 1)
		start_full = datetime(2019, 9, 30)
		end_full = datetime(2019, 9, 30)
		test_dates = []
		test_date = start_full
		while True:
			if test_date > end_full:
				break
			test_dates.append(test_date)
			test_date = test_date + relativedelta(days=step)

		result_dict = {}
		whole_period_t_df = pd.DataFrame()
		for pair_i in self.pairs:
			for pair_j in self.pairs:
				if pair_i == pair_j:
					continue

				return_i = self.FX_close_dict[pair_i].pct_change()
				return_j = self.FX_close_dict[pair_j].pct_change()
				return_ij = pd.concat([return_i, return_j], axis=1)
				return_ij.columns = [pair_i, pair_j]
				return_ij = return_ij.dropna()

				result_corr_ts_dict = {}
				result_tvalue_ts_dict = {}
				for test_date in test_dates:
					sample_return = return_ij.loc[test_date - relativedelta(days=lb_win):test_date]
					lags = [-1, 0, 1]
					result_corr_ts_dict[test_date] = {}
					result_tvalue_ts_dict[test_date] = {}

					for lag in lags:
						sample_return_lag = sample_return.copy()
						sample_return_lag[pair_i] = sample_return[pair_i].shift(lag)
						sample_return_lag = sample_return_lag.dropna()
						cor = np.corrcoef(sample_return_lag[pair_i], sample_return_lag[pair_j])
						result_corr_ts_dict[test_date][lag] = cor[0, 1]

						regResult = sm.OLS(sample_return_lag.iloc[:, 0], sample_return_lag.iloc[:, 1]).fit()
						tvalue = float(regResult.tvalues)
						result_tvalue_ts_dict[test_date][lag] = tvalue

				# fig = plt.figure()
				# # plt.plot(pd.DataFrame(result_corr_ts_dict).T)
				# plt.plot(pd.DataFrame(result_tvalue_ts_dict).T)
				# # plt.legend(['c'+str(l) for l in lags] + ['t'+str(l) for l in lags])
				# plt.axhline(0)
				# plt.legend(['t'+str(l) for l in lags])
				# plt.title(pair_i.replace('T150 Curncy', '') + '_' + pair_j.replace('T150 Curncy', ''))
				# plt.show()

				tmp_df = pd.DataFrame(result_tvalue_ts_dict).T
				tmp_df.index = [pair_i.replace('T150 Curncy', '') + '_' + pair_j.replace('T150 Curncy', '')]
				whole_period_t_df = pd.concat([whole_period_t_df, tmp_df])

				result_dict[pair_i + '_' + pair_j] = result_corr_ts_dict

	# print(whole_period_t_df.to_string())
	# print()

	def simple_backtest_FAST(self, tmp, pair, indicator1, indicator2, transform):
		t0_ts = (tmp.iloc[:, 0] + 1).cumprod()
		tp = tmp.loc[tmp.iloc[:, 1] > 0]
		tp_ts = (tp.iloc[:, 0] + 1).cumprod()
		tn = tmp.loc[tmp.iloc[:, 1] < 0]
		tn_ts = (tn.iloc[:, 0] + 1).cumprod()
		all_ts = pd.concat([t0_ts, tp_ts, tn_ts], axis=1)
		all_ts = all_ts.fillna(method='ffill')
		all_ts.columns = ['raw', 'positive', 'negative']

		# fig = plt.figure()
		# plt.plot(all_ts)
		# plt.legend(all_ts.columns)
		# plt.title(f'{pair}_{indicator1}_{indicator2}_{transform}')
		# plt.show()
		return

	def predictability_FAST(self, pair, indicator1, indicator2, close_df, macro_df1, macro_df2):
		predict_dict = {}
		path_out = 'C:/Users\AQMadmin\Downloads\macro/fast_significant/'
		try:

			close_df_weekly = close_df.asfreq('W-Fri', method='ffill')
			macro_df1_weekly = macro_df1.asfreq('W-Fri', method='ffill')
			macro_df2_weekly = macro_df2.asfreq('W-Fri', method='ffill')
			macrodiff_df_weekly = (macro_df1 - macro_df2).asfreq('W-Fri', method='ffill')

			return_df_weekly = close_df_weekly.pct_change().iloc[1:]
			next_return_df_weekly = return_df_weekly.shift(-1)

			macro1_diff_weekly = macro_df1_weekly.diff().iloc[1:]
			macro2_diff_weekly = macro_df2_weekly.diff().iloc[1:]
			macrodiff_diff_weekly = macrodiff_df_weekly.diff().iloc[1:]

			macro1_pct_weekly = macro_df1_weekly.pct_change().iloc[1:]
			macro2_pct_weekly = macro_df2_weekly.pct_change().iloc[1:]
			macrodiff_pct_weekly = macrodiff_df_weekly.pct_change().iloc[1:]

			tmp = pd.concat([next_return_df_weekly, macro_df1_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macro1_level'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

			tmp = pd.concat([next_return_df_weekly, macro_df2_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macro2_level'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

			tmp = pd.concat([next_return_df_weekly, macrodiff_df_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macrodiff_level'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

			tmp = pd.concat([next_return_df_weekly, macro1_diff_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macro1_diff'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

			tmp = pd.concat([next_return_df_weekly, macro2_diff_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macro2_diff'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

			tmp = pd.concat([next_return_df_weekly, macrodiff_diff_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macrodiff_diff'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

			tmp = pd.concat([next_return_df_weekly, macro1_pct_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macro1_pct'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

			tmp = pd.concat([next_return_df_weekly, macro2_pct_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macro2_pct'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

			tmp = pd.concat([next_return_df_weekly, macrodiff_pct_weekly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			transform = 'macrodiff_pct'
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_FAST(tmp, pair, indicator1, indicator2, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator1}_{indicator2}.csv')

		except Exception as e:
			print(e)

		return predict_dict

	def main_relate_FX_with_macro_FAST(self):
		currencies, pairs, ccy2pair = self.get_currency_list()
		for pair in pairs:
			ccy1 = pair[:3]
			ccy2 = pair[-3:]
			close_df = self.FX_close_dict[pair]

			indicators1 = self.get_fast_indicators_by_ccy(ccy1)
			indicators2 = self.get_fast_indicators_by_ccy(ccy2)

			for indicator_i in range(len(indicators1)):
				indicator1 = indicators1[indicator_i]
				indicator2 = indicators2[indicator_i]
				if type(indicator1) != str or type(indicator2) != str:
					continue

				parser = lambda x: datetime.strptime(x, '%Y-%m-%d')
				macro_df1 = pd.read_csv(self.ROOT_PATH_FAST + indicator1 + '.csv',
										index_col=0,
										parse_dates=[0],
										date_parser=parser)
				macro_df2 = pd.read_csv(self.ROOT_PATH_FAST + indicator2 + '.csv',
										index_col=0,
										parse_dates=[0],
										date_parser=parser)
				print(f'pair={pair};ccy1={ccy1};ccy2={ccy2};indicator1={indicator1};indicator2={indicator2}')

				# fig, ax1 = plt.subplots()
				# ax2 = ax1.twinx()
				# ax1.plot(close_df, 'g-')
				# ax2.plot(macro_df1, 'b-')
				# ax2.plot(macro_df2, 'r-')
				# ax2.plot(macro_df1-macro_df2, 'k-')
				# ax1.set_ylabel(pair)
				# ax2.set_ylabel(indicator1 + '_' + indicator2)
				# plt.show()

				# predictability
				predict_dict = self.predictability_FAST(pair, indicator1, indicator2, close_df, macro_df1, macro_df2)
				print(pd.Series(predict_dict))
				print(pd.Series(predict_dict).abs().sort_values(ascending=False))
				print()

		return

	def simple_backtest_SLOW(self, tmp, pair, indicator, transform):
		t0_ts = (tmp.iloc[:, 0] + 1).cumprod()
		tp = tmp.loc[tmp.iloc[:, 1] > 0]
		tp_ts = (tp.iloc[:, 0] + 1).cumprod()
		tn = tmp.loc[tmp.iloc[:, 1] < 0]
		tn_ts = (tn.iloc[:, 0] + 1).cumprod()
		all_ts = pd.concat([t0_ts, tp_ts, tn_ts], axis=1)
		all_ts = all_ts.fillna(method='ffill')
		all_ts.columns = ['raw', 'positive', 'negative']

		fig = plt.figure()
		plt.plot(all_ts)
		plt.legend(all_ts.columns)
		plt.title(f'{pair}_{indicator}_{transform}')
		plt.show()

		return

	def main_relate_FX_with_macro_SLOW(self):
		currencies, pairs, ccy2pair = self.get_currency_list()
		for pair in pairs:
			ccy1 = pair[:3]
			ccy2 = pair[-3:]
			close_df = self.FX_close_dict[pair]

			indicators1 = self.get_slow_indicators_by_ccy(ccy1, 'Monthly')
			indicators2 = self.get_slow_indicators_by_ccy(ccy2, 'Monthly')
			indicators = indicators1 + indicators2

			for indicator_i in range(len(indicators)):
				indicator = indicators[indicator_i]
				parser = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
				macro_monthly_df = pd.read_csv(self.path_slow_monthly_byDate + indicator + '.csv',
											   index_col=0,
											   parse_dates=[0],
											   date_parser=parser)
				print(f'pair={pair};ccy1={ccy1};ccy2={ccy2};indicator={indicator}')

				# fig, ax1 = plt.subplots()
				# ax2 = ax1.twinx()
				# ax1.plot(close_df, 'g-')
				# ax2.plot(macro_df['PX_LAST'], 'b-')
				# ax1.set_ylabel(pair)
				# ax2.set_ylabel(indicator)
				# plt.show()

				# predictability

				test_fields = ['raw', 'diff', 'pct', 'surprise']
				for test_field in test_fields:
					if test_field not in macro_monthly_df.columns:
						continue
					predict_dict = self.predictability_SLOW(pair, indicator, test_field,
															close_df, macro_monthly_df[[test_field]])
					print(pd.Series(predict_dict))
					print(pd.Series(predict_dict).abs().sort_values(ascending=False))
					print()

		return

	def predictability_SLOW(self, pair, indicator, transform, close_df, macro_df):
		predict_dict = {}
		path_out = 'C:/Users\AQMadmin\Downloads\macro/slow_significant/'
		try:

			close_df_monthly = close_df.asfreq('W-Fri', method='ffill')
			macro_df_monthly = macro_df.asfreq('W-Fri', method='ffill')
			macro_df_monthly.index = [datetime(i.year, i.month, i.day) for i in macro_df_monthly.index]

			return_df_monthly = close_df_monthly.pct_change().iloc[1:]
			next_return_df_monthly = return_df_monthly.shift(-1)

			# macro_diff_monthly = macro_df_monthly.diff().iloc[1:]
			# macro_pct_monthly = macro_df_monthly.pct_change().iloc[1:]

			tmp = pd.concat([next_return_df_monthly, macro_df_monthly], axis=1).dropna()
			regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
			predict_dict[transform] = float(regResult.tvalues)
			if abs(float(regResult.tvalues)) > 3:
				self.simple_backtest_SLOW(tmp, pair, indicator, transform)
				tmp.to_csv(path_out + f'{transform}_{pair}_{indicator}.csv')
				print(pair, indicator, transform, float(regResult.tvalues))

		# tmp = pd.concat([next_return_df_monthly, macro_diff_monthly], axis=1).dropna()
		# regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
		# transform = 'macro_diff'
		# predict_dict[transform] = float(regResult.tvalues)
		# if abs(float(regResult.tvalues)) > 3:
		# 	self.simple_backtest_SLOW(tmp, pair, indicator, transform)
		# 	tmp.to_csv(path_out + f'{transform}_{pair}_{indicator}.csv')
		# 	print(pair, indicator, transform, float(regResult.tvalues))
		#
		# tmp = pd.concat([next_return_df_monthly, macro_pct_monthly], axis=1).dropna()
		# regResult = sm.OLS(tmp.iloc[:, 0], tmp.iloc[:, 1]).fit()
		# transform = 'macro_pct'
		# predict_dict[transform] = float(regResult.tvalues)
		# if abs(float(regResult.tvalues)) > 3:
		# 	self.simple_backtest_SLOW(tmp, pair, indicator, transform)
		# 	tmp.to_csv(path_out + f'{transform}_{pair}_{indicator}.csv')
		# 	print(pair, indicator, transform, float(regResult.tvalues))

		except Exception as e:
			print(e)

		return predict_dict

	@timeit
	def _derive_fast_factors_by_indicator(self, fast_tickers_df):
		for typ in fast_tickers_df.columns:
			for ccy in fast_tickers_df.index:
				if type(fast_tickers_df.loc[ccy, typ]) != str:
					continue
				ticker = fast_tickers_df.loc[ccy, typ]
				parser = lambda x: datetime.strptime(x, '%Y-%m-%d')
				df = pd.read_csv(self.ROOT_PATH_FAST + ticker + '.csv',
								 index_col=0, parse_dates=[0], date_parser=parser)

				if typ in ['1-month yield', '3-month yield', '2-year yield', '10-year yield']:
					# difference
					df.columns = ['raw']
					df = df.fillna(method='ffill')
					df['daily_diff'] = df['raw'].diff(1)
					df['weekly_diff'] = df['raw'].diff(5)
					df['monthly_diff'] = df['raw'].diff(22)
				elif typ in ['CDS', 'Domestic equity', 'Terms of trade', 'Citi economic surprise index']:
					# pct change
					df.columns = ['raw']
					df = df.fillna(method='ffill')
					df['daily_pct'] = (df['raw'] - df['raw'].shift(1)) / df['raw'].abs()
					df['weekly_pct'] = (df['raw'] - df['raw'].shift(5)) / df['raw'].abs()
					df['monthly_pct'] = (df['raw'] - df['raw'].shift(22)) / df['raw'].abs()
				else:
					raise NotImplementedError
				df.to_csv(self.path_fast_derived + ticker + '.csv')

	@timeit
	def _align_fast_factors_by_indicator(self, fast_tickers_df, fast_update_df, align_time):
		for typ in fast_tickers_df.columns:
			for ccy in fast_tickers_df.index:
				if type(fast_tickers_df.loc[ccy, typ]) != str:
					continue
				ticker = fast_tickers_df.loc[ccy, typ]
				update_time = fast_update_df.loc[ccy, typ]
				parser = lambda x: datetime.strptime(x, '%Y-%m-%d')
				df = pd.read_csv(self.path_fast_derived + ticker + '.csv',
								 index_col=0, parse_dates=[0], date_parser=parser)

				if typ in ['1-month yield', '3-month yield', '2-year yield', '10-year yield', 'CDS', 'Domestic equity']:
					# from trade
					if update_time <= align_time:
						# available at align_time
						df.index = [datetime(i.year, i.month, i.day,
											 align_time.hour, align_time.minute, align_time.second) for i in df.index]
					elif update_time > align_time:
						# not available at align_time
						df.index = [datetime(i.year, i.month, i.day,
											 align_time.hour, align_time.minute, align_time.second) for i in df.index]
						df = df.shift(1)
					else:
						raise NotImplementedError

				elif typ in ['Terms of trade', 'Citi economic surprise index']:
					# from economics
					# 1-day delay
					df.index = [datetime(i.year, i.month, i.day,
										 update_time.hour, update_time.minute, update_time.second) for i in df.index]
					df = df.shift(1)

					if update_time <= align_time:
						# available at align_time
						df.index = [datetime(i.year, i.month, i.day,
											 align_time.hour, align_time.minute, align_time.second) for i in df.index]
					elif update_time > align_time:
						# not available at align_time
						df.index = [datetime(i.year, i.month, i.day,
											 align_time.hour, align_time.minute, align_time.second) for i in df.index]
						df = df.shift(1)
					else:
						raise NotImplementedError

				else:
					raise NotImplementedError
				df.to_csv(self.path_fast_aligned + ticker + '.csv')

	@timeit
	def _derive_fast_factors_cross_pairs(self, fast_tickers_df):
		for typ in fast_tickers_df.columns:
			exist_pair = []
			for ccy1 in fast_tickers_df.index:
				for ccy2 in fast_tickers_df.index:
					if type(fast_tickers_df.loc[ccy1, typ]) != str or type(fast_tickers_df.loc[ccy2, typ]) != str:
						continue
					if ccy1 == ccy2:
						continue
					if ccy1 + ccy2 in exist_pair or ccy2 + ccy1 in exist_pair:
						continue
					exist_pair += [ccy1 + ccy2]

					if typ in ['3-month yield']:
						ticker1 = fast_tickers_df.loc[ccy1, typ]
						ticker2 = fast_tickers_df.loc[ccy2, typ]
						parser = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
						df1 = pd.read_csv(self.path_fast_aligned + ticker1 + '.csv',
										  index_col=0, parse_dates=[0], date_parser=parser)
						df2 = pd.read_csv(self.path_fast_aligned + ticker2 + '.csv',
										  index_col=0, parse_dates=[0], date_parser=parser)

						df = pd.DataFrame(df1['raw'] - df2['raw'])
						df.columns = ['raw']
						df = df.fillna(method='ffill')
						df['daily_diff'] = df['raw'].diff(1)
						df['weekly_diff'] = df['raw'].diff(5)
						df['monthly_diff'] = df['raw'].diff(22)
						df.to_csv(self.path_fast_derived_cross + f'IRS_{ccy1}_{ccy2}.csv')
					else:
						pass

	@timeit
	def _slice_fast_factor(self, align_time, start_full, end_full):
		slice_freq = 'W-Fri'
		weeks_full = pd.date_range(start_full, end_full, freq=slice_freq)
		weeks_full = [datetime(i.year, i.month, i.day, align_time.hour, align_time.minute, 59) for i in weeks_full]
		parser = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

		# fast align tickers
		in_path = self.path_fast_aligned
		out_path = self.path_fast_slice
		for file in os.listdir(in_path):
			ticker = file.replace('.csv', '')
			df = pd.read_csv(in_path + file, index_col=0, parse_dates=[0], date_parser=parser)
			df.columns = [ticker + '|' + c for c in df.columns]
			dummy_df = pd.DataFrame(index=weeks_full, columns=df.columns)
			df = pd.concat([df, dummy_df])
			df = df.sort_index()
			df = df.fillna(method='ffill')
			df = df.loc[weeks_full].dropna(how='all')
			df.to_csv(out_path + ticker + '.csv')

		# derive cross tickers
		in_path = self.path_fast_derived_cross
		out_path = self.path_fast_slice
		for file in os.listdir(in_path):
			ticker = file.replace('.csv', '')
			df = pd.read_csv(in_path + file, index_col=0, parse_dates=[0], date_parser=parser)
			df.columns = [ticker + '|' + c for c in df.columns]
			dummy_df = pd.DataFrame(index=weeks_full, columns=df.columns)
			df = pd.concat([df, dummy_df])
			df = df.sort_index()
			df = df.fillna(method='ffill')
			df = df.loc[weeks_full].dropna(how='all')
			df.to_csv(out_path + ticker + '.csv')

	@timeit
	def _combine_fast_slice(self):
		parser = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
		in_path = self.path_fast_slice
		out_path = self.path_fast_combineSlice
		files_slice = os.listdir(in_path)
		combine_df = pd.DataFrame()
		for file_slice in files_slice:
			# print('_combine_fast_slice', file_slice)
			df = pd.read_csv(in_path + file_slice, index_col=0, parse_dates=[0], date_parser=parser)
			combine_df = pd.concat([combine_df, df], axis=1)
		combine_df.index = [datetime(i.year, i.month, i.day, i.hour, i.minute, 0) for i in combine_df.index]
		fields = [c for c in combine_df.columns if '_date' not in c]
		combine_df = combine_df[fields]
		combine_df.to_csv(out_path + 'combine.csv')

	@timeit
	def _generate_fast_factors(self, align_time=dt.time(14, 0, 0)):
		fast_tickers_df = pd.read_excel('data/macro_fast_indicators.xlsx', sheet_name='ticker')
		fast_update_df = pd.read_excel('data/macro_fast_indicators.xlsx', sheet_name='update_time')
		self._derive_fast_factors_by_indicator(fast_tickers_df)
		self._align_fast_factors_by_indicator(fast_tickers_df, fast_update_df, align_time)
		self._derive_fast_factors_cross_pairs(fast_tickers_df)
		self._slice_fast_factor(align_time=dt.time(14, 0, 0),
								start_full=datetime(2018, 1, 1),
								end_full=datetime.strptime(self.end_date, '%Y-%m-%d'))
		self._combine_fast_slice()

	# @timeit
	def _derive_slow_factors_monthly(self, ticker, df_full, indx_unit):
		# print(ticker)
		start_month_full = datetime(2018, 8, 31)
		start_month_full = max(np.min(df_full['econ_date']), start_month_full)
		end_month_full = datetime.strptime(self.end_date, '%Y-%m-%d')

		months_full = pd.date_range(start_month_full, end_month_full, freq='M')

		bactestable_result = dict()

		for month in months_full:
			df = df_full.loc[df_full['release_date'] <= month]
			if df.empty:
				continue
			df = df.dropna(subset=['actual_release', 'release_date'], how='any')
			if df.empty:
				continue
			start_month = np.min(df['econ_date'])
			end_month = month
			months = pd.date_range(start_month, end_month, freq='M')
			if len(months) == 0:
				continue
			parsed_dict = {}
			for mth in months:
				df_mth = df.loc[df['econ_date'] == mth]
				df_mth = df_mth.sort_values(by='release_type', ascending=False)
				if df_mth.empty:
					tmp = pd.Series(index=df.columns)
					parsed_dict[mth.strftime('%Y-%m-%d')] = tmp
				else:
					parsed_dict[mth.strftime('%Y-%m-%d')] = df_mth.iloc[0]

			parsed_df = pd.DataFrame(parsed_dict).T
			parsed_df['raw'] = parsed_df['actual_release']
			if indx_unit == 'diff':
				parsed_df['diff'] = parsed_df['raw'].diff()
			elif indx_unit == 'pct':
				parsed_df['pct'] = (parsed_df['raw'] - parsed_df['raw'].shift(1)) / parsed_df['raw'].abs()
			else:
				raise NotImplementedError
			parsed_df['surprise'] = parsed_df['raw'] - parsed_df['survey_median']
			# print(month)
			# print(parsed_df.to_string())
			parsed_df.to_csv(
				self.path_slow_monthly_byIndicator_byDate + ticker + '_' + month.strftime('%Y-%m-%d') + '.csv')

			# backtestable result
			bt_df = parsed_df.loc[parsed_df['release_date'] <= end_month]
			if len(bactestable_result.keys()) > 0:
				last_release_date = max(bactestable_result.keys())
				bt_df = bt_df.loc[parsed_df['release_date'] > last_release_date]
				bt_df = bt_df.sort_values(by='release_date', ascending=True)
				for i, v in enumerate(bt_df.index):
					release_date = bt_df.loc[v, 'release_date']
					bactestable_result[release_date] = bt_df.loc[v]
			else:
				bt_df = bt_df.sort_values(by='release_date', ascending=False).iloc[0]
				bactestable_result[bt_df['release_date']] = bt_df

		return pd.DataFrame(bactestable_result).T

	# @timeit
	def _derive_slow_factors_quarterly(self, ticker, df_full, indx_unit):
		# print(ticker)
		start_month_full = datetime(2018, 8, 31)
		start_month_full = max(np.min(df_full['econ_date']), start_month_full)
		end_month_full = datetime.strptime(self.end_date, '%Y-%m-%d')
		# start_month_full = np.min(df_full['econ_date'])
		months_full = pd.date_range(start_month_full, end_month_full, freq='M')

		bactestable_result = dict()

		for month in months_full:
			df = df_full.loc[df_full['release_date'] <= month]
			df = df.dropna(subset=['actual_release', 'release_date'], how='any')
			if df.empty:
				continue
			start_month = np.min(df['econ_date'])
			end_month = month
			quarters = pd.date_range(start_month, end_month, freq='3M')
			if len(quarters) == 0:
				continue
			parsed_dict = {}
			for qrt in quarters:
				df_qrt = df.loc[df['econ_date'] == qrt]
				df_qrt = df_qrt.sort_values(by='release_type', ascending=False)
				if df_qrt.empty:
					tmp = pd.Series(index=df.columns)
					parsed_dict[qrt.strftime('%Y-%m-%d')] = tmp
				else:
					parsed_dict[qrt.strftime('%Y-%m-%d')] = df_qrt.iloc[0]

			parsed_df = pd.DataFrame(parsed_dict).T
			parsed_df['raw'] = parsed_df['actual_release']
			if indx_unit == 'diff':
				parsed_df['diff'] = parsed_df['raw'].diff()
			elif indx_unit == 'pct':
				parsed_df['pct'] = (parsed_df['raw'] - parsed_df['raw'].shift(1)) / parsed_df['raw'].abs()
			else:
				raise NotImplementedError
			parsed_df['surprise'] = parsed_df['raw'] - parsed_df['survey_median']
			# print(month)
			# print(parsed_df.to_string())
			parsed_df.to_csv(
				self.path_slow_quarterly_byIndicator_byDate + ticker + '_' + month.strftime('%Y-%m-%d') + '.csv')

			# backtestable result
			bt_df = parsed_df.loc[parsed_df['release_date'] <= end_month]
			if len(bactestable_result.keys()) > 0:
				last_release_date = max(bactestable_result.keys())
				bt_df = bt_df.loc[parsed_df['release_date'] > last_release_date]
				bt_df = bt_df.sort_values(by='release_date', ascending=True)
				for i, v in enumerate(bt_df.index):
					release_date = bt_df.loc[v, 'release_date']
					bactestable_result[release_date] = bt_df.loc[v]
			else:
				bt_df = bt_df.sort_values(by='release_date', ascending=False).iloc[0]
				bactestable_result[bt_df['release_date']] = bt_df

		return pd.DataFrame(bactestable_result).T

	@timeit
	def _derive_slow_factors_by_indicator(self, slow_tickers_df):
		for i, v in enumerate(slow_tickers_df.index):
			try:
				ccy = slow_tickers_df.loc[v, 'Currency']
				ticker = slow_tickers_df.loc[v, 'Bloomber Ticker']
				update_time = slow_tickers_df.loc[v, 'BBG UPDATE TIME']

				# if ticker != 'AUBAY Index':
				# 	continue
				parser = lambda x: np.nan if type(x) != str else datetime.strptime(x, '%Y-%m-%d')
				df = pd.read_csv(self.ROOT_PATH_SLOW_NEAT + ticker + '.csv',
								 index_col=0,
								 parse_dates=['econ_date', 'release_date'],
								 date_parser=parser)
				freq = slow_tickers_df.loc[v, 'BBG FREQ']
				indx_unit = slow_tickers_df.loc[v, 'BBG INDX UNITS']
				if freq == 'Monthly':
					if indx_unit in ['Rate', 'Percent']:
						df = self._derive_slow_factors_monthly(ticker, df, 'diff')
					elif indx_unit in ['Value', 'Quantity', 'Volume']:
						df = self._derive_slow_factors_monthly(ticker, df, 'pct')
					else:
						raise NotImplementedError
					df.index = [datetime(i.year, i.month, i.day, update_time.hour, update_time.minute,
										 update_time.second) for i in df.index]
					df.to_csv(self.path_slow_monthly_byDate + ticker + '.csv')
				elif freq == 'Quarterly':
					if indx_unit in ['Rate', 'Percent']:
						df = self._derive_slow_factors_quarterly(ticker, df, 'diff')
					elif indx_unit in ['Value', 'Quantity', 'Volume']:
						df = self._derive_slow_factors_quarterly(ticker, df, 'pct')
					else:
						raise NotImplementedError
					df.index = [datetime(i.year, i.month, i.day, update_time.hour, update_time.minute,
										 update_time.second) for i in df.index]
					df.to_csv(self.path_slow_quarterly_byDate + ticker + '.csv')
				elif freq == 'Weekly':
					print('Weekly', ticker)
				else:
					raise NotImplementedError
			except Exception as e:
				print('ERROR:', slow_tickers_df.loc[v, 'Bloomber Ticker'], e)

	@timeit
	def _slice_slow_factor(self, slow_tickers_df, align_time, start_full, end_full):
		slice_freq = 'W-Fri'
		weeks_full = pd.date_range(start_full, end_full, freq=slice_freq)
		weeks_full = [datetime(i.year, i.month, i.day, align_time.hour, align_time.minute, 59) for i in weeks_full]

		for i, v in enumerate(slow_tickers_df.index):
			# if True:
			try:
				ccy = slow_tickers_df.loc[v, 'Currency']
				ticker = slow_tickers_df.loc[v, 'Bloomber Ticker']
				update_time = slow_tickers_df.loc[v, 'BBG UPDATE TIME']
				freq = slow_tickers_df.loc[v, 'BBG FREQ']
				indx_unit = slow_tickers_df.loc[v, 'BBG INDX UNITS']
				print(ticker)
				parser = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

				if freq == 'Monthly':
					in_path = self.path_slow_monthly_byDate
					out_path = self.path_slow_monthly_slice
				elif freq == 'Quarterly':
					in_path = self.path_slow_quarterly_byDate
					out_path = self.path_slow_quarterly_slice
				elif freq == 'Weekly':
					print('Weekly', ticker)
					continue
				else:
					raise NotImplementedError

				df = pd.read_csv(in_path + ticker + '.csv', index_col=0, parse_dates=[0], date_parser=parser)
				if indx_unit in ['Rate', 'Percent']:
					change_field = 'diff'
				elif indx_unit in ['Value', 'Quantity', 'Volume']:
					change_field = 'pct'
				else:
					raise NotImplementedError
				df = df[['econ_date', 'release_date', 'raw', change_field, 'surprise']]
				df.columns = [ticker + '|' + c for c in df.columns]
				dummy_df = pd.DataFrame(index=weeks_full, columns=df.columns)
				df = pd.concat([df, dummy_df])
				df = df.sort_index()
				df = df.fillna(method='ffill')
				df = df.loc[weeks_full].dropna(how='all')
				df.to_csv(out_path + ticker + '.csv')
			except Exception as e:
				print('ERROR', slow_tickers_df.loc[v, 'Bloomber Ticker'], e)

	@timeit
	def _combine_slow_slice(self):
		parser = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')

		# quarterly
		in_path = self.path_slow_quarterly_slice
		out_path = self.path_slow_quarterly_combineSlice
		files_slice = os.listdir(in_path)
		combine_df = pd.DataFrame()
		for file_slice in files_slice:
			print('quarter', file_slice)
			df = pd.read_csv(in_path + file_slice, index_col=0, parse_dates=[0], date_parser=parser)
			combine_df = pd.concat([combine_df, df], axis=1)
		combine_df.index = [datetime(i.year, i.month, i.day, i.hour, i.minute, 0) for i in combine_df.index]
		fields = [c for c in combine_df.columns if '_date' not in c]
		combine_df = combine_df[fields]
		combine_df.to_csv(out_path + 'combine.csv')

		# monthly
		in_path = self.path_slow_monthly_slice
		out_path = self.path_slow_monthly_combineSlice
		files_slice = os.listdir(in_path)
		combine_df = pd.DataFrame()
		for file_slice in files_slice:
			# print('month', file_slice)
			df = pd.read_csv(in_path + file_slice, index_col=0, parse_dates=[0], date_parser=parser)
			combine_df = pd.concat([combine_df, df], axis=1)
		combine_df.index = [datetime(i.year, i.month, i.day, i.hour, i.minute, 0) for i in combine_df.index]
		fields = [c for c in combine_df.columns if '_date' not in c]
		combine_df = combine_df[fields]
		combine_df.to_csv(out_path + 'combine.csv')

	@timeit
	def _generate_slow_factors(self):
		slow_tickers_df = pd.read_csv('data/macro_slow_indicators.csv')
		slow_tickers_df['BBG UPDATE TIME'] = [t.time() for t in pd.to_datetime((slow_tickers_df['BBG UPDATE TIME']),
																			   format='%I:%M:%S %p')]
		self._derive_slow_factors_by_indicator(slow_tickers_df)
		self._slice_slow_factor(slow_tickers_df,
								align_time=dt.time(14, 0, 0),
								start_full=datetime(2018, 1, 1),
								end_full=datetime.strptime(self.end_date, '%Y-%m-%d'))
		self._combine_slow_slice()

	@timeit
	def generate_factors(self):
		self._generate_fast_factors()
		self._generate_slow_factors()

	@timeit
	def generate_future_returns(self):
		slice_freq = 'W-Fri'
		result_df = pd.DataFrame()
		result_pairs = []
		for pair in self.FX_close_dict:
			close_df = self.FX_close_dict[pair]
			close_df.index = [datetime(i.year, i.month, i.day, 14, 0, 0) for i in close_df.index]
			close_df = close_df.asfreq(slice_freq, method='ffill')
			return_df = close_df.pct_change().iloc[1:]
			next_return_df = return_df.shift(-1)
			result_pairs.append(pair)
			result_df = pd.concat([result_df, next_return_df], axis=1)
		result_df.columns = result_pairs
		result_df.to_csv(self.path_next_period_return + 'weekly_return.csv')
		return

	def model_core(self, next_returns_df, factors_df):
		next_returns_df = next_returns_df.iloc[:-1]  # last expected return is oos
		factors_df_oss = factors_df.iloc[-1]
		factors_df = factors_df.iloc[:-1]

		combine_df = pd.concat([next_returns_df, factors_df], axis=1)

		# select significant factors
		sig_factors = []
		t_value_dict = {}
		for factor in factors_df.columns:
			try:
				regResult = sm.OLS(combine_df.iloc[:, 0], combine_df[factor]).fit()
				if abs(float(regResult.tvalues)) > 3:
					# print(f'pair={combine_df.columns[0]},factor={factor}, t-value={round(float(regResult.tvalues), 4)}')
					sig_factors.append(factor)
					t_value_dict[factor] = float(regResult.tvalues)
			except Exception as e:
				# print('ERROR:', f'pair={combine_df.columns[0]},factor={factor}')
				continue

		# multi-collinearity test
		if len(sig_factors) > 1:
			# print('Before VIF test', f'# factor = {len(sig_factors)}')
			VIF_flag = True
			VIF_threshould = 10
			while VIF_flag:
				n_factor = len(sig_factors)
				VIF_list = []
				for i in np.arange(n_factor):
					VIF = variance_inflation_factor(combine_df[sig_factors].values, i)
					VIF_list.append(VIF)
					if np.isnan(VIF) or VIF >= VIF_threshould:
						correlated_factor = sig_factors[i]
				if np.sum(np.array(VIF_list) < VIF_threshould) == n_factor:
					VIF_flag = False
				else:
					sig_factors = [ftr for ftr in sig_factors if ftr not in [correlated_factor]]
				if len(sig_factors) == 0:
					return np.nan, np.nan, {}
				elif len(sig_factors) == 1:
					VIF_flag = False
			# print('After VIF test', f'# factor = {len(sig_factors)}')

		# select most significant factors
		max_n_factor = 5
		if len(sig_factors) > max_n_factor:
			sig_factors = list(pd.Series(t_value_dict).abs().sort_values(ascending=False).index[:max_n_factor])

		# linear prediction model
		if len(sig_factors) == 0:
			return np.nan, np.nan, {}
		regResult = sm.OLS(combine_df.iloc[:, 0], combine_df[sig_factors]).fit()
		# print(regResult.summary())
		model_coefficients = regResult.params
		r_squared = regResult.rsquared
		factors_df_oss = factors_df_oss[sig_factors]
		model_result = pd.concat([model_coefficients.rename('coef'), factors_df_oss.rename('factor')], axis=1)
		expected_return = np.sum(model_result['coef'] * model_result['factor'])

		return expected_return, r_squared, model_coefficients.to_dict()

	@timeit
	def main_prediction_model(self):
		start_full = datetime(2018, 11, 1)
		end_full = datetime.strptime(self.end_date + ' 14:00:00', '%Y-%m-%d %H:%M:%S')

		parser = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
		factors_monthly_df = pd.read_csv(self.path_slow_monthly_combineSlice + 'combine.csv',
										 index_col=0, parse_dates=[0], date_parser=parser)
		factors_quarterly_df = pd.read_csv(self.path_slow_quarterly_combineSlice + 'combine.csv',
										   index_col=0, parse_dates=[0], date_parser=parser)
		factors_fast_df = pd.read_csv(self.path_fast_combineSlice + 'combine.csv',
									  index_col=0, parse_dates=[0], date_parser=parser)

		factors_df = pd.concat([factors_fast_df, factors_monthly_df, factors_quarterly_df], axis=1)
		all_indicators = list(factors_df.columns)
		next_returns_df = pd.read_csv(self.path_next_period_return + 'weekly_return.csv',
									  index_col=0, parse_dates=[0], date_parser=parser)

		trading_dates_full = next_returns_df.index
		trading_dates = [d for d in trading_dates_full if d >= start_full and d <= end_full]

		expected_return_dict = {}
		r_squared_dict = {}
		model_coefficient_dict = {}
		for trade_date in trading_dates:
			expected_return_dict[trade_date] = {}
			r_squared_dict[trade_date] = {}
			model_coefficient_dict[trade_date.strftime('%Y-%m-%d')] = {}

			lb_window = 365  # days
			lb_from_trade_date = trade_date - relativedelta(days=lb_window)
			print(f'{lb_from_trade_date}, {trade_date}')
			sample_factors_df = factors_df.loc[lb_from_trade_date:trade_date]
			sample_next_returns_df = next_returns_df.loc[lb_from_trade_date:trade_date]

			for pair in self.pairs:
				ccy1 = pair[:3]
				ccy2 = pair[-3:]
				indicators_slow1 = self.get_slow_indicators_by_ccy(ccy=ccy1)
				indicators_slow2 = self.get_slow_indicators_by_ccy(ccy=ccy2)
				indicators_fast1 = self.get_fast_indicators_by_ccy(ccy=ccy1)
				indicators_fast2 = self.get_fast_indicators_by_ccy(ccy=ccy2)
				indicators = indicators_slow1 + indicators_slow2 + indicators_fast1 + indicators_fast2

				related_indicators = []
				for ind in all_indicators:
					if ind.split('|')[0] in indicators or f'IRS_{ccy1}_{ccy2}' in ind or f'IRS_{ccy2}_{ccy1}' in ind:
						related_indicators.append(ind)
				expected_return, r_squared, model_coefficients = self.model_core(sample_next_returns_df[pair],
																				 sample_factors_df[related_indicators])
				expected_return_dict[trade_date][pair] = expected_return
				r_squared_dict[trade_date][pair] = r_squared
				model_coefficient_dict[trade_date.strftime('%Y-%m-%d')][pair] = model_coefficients

		expected_return_df = pd.DataFrame(expected_return_dict)
		r_squared_df = pd.DataFrame(r_squared_dict)
		expected_return_df.to_csv(self.file_expected_return)
		r_squared_df.to_csv(self.file_r_squared)
		with open(self.file_model_coefficient, 'w') as fp:
			json.dump(model_coefficient_dict, fp)
		print()

	def evaluate_prediction_model(self):
		parser = lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
		next_returns_df = pd.read_csv(self.path_next_period_return + 'weekly_return.csv',
									  index_col=0, parse_dates=[0], date_parser=parser)

		expected_return_df = pd.read_csv(self.file_expected_return, index_col=0)
		expected_return_df.columns = pd.to_datetime(expected_return_df.columns, format='%Y-%m-%d %H:%M:%S')

		trading_dates = expected_return_df.columns

		result_dict = {}

		for td_i, trade_date in enumerate(trading_dates):

			# exp_return = expected_return_df[trade_date]
			# rlz_return = next_returns_df.loc[trade_date]

			lb_n = 52
			if td_i <= lb_n:
				continue
			exp_return = expected_return_df[trading_dates[td_i - lb_n:td_i]].mean(axis=1)
			rlz_return = next_returns_df.loc[trading_dates[td_i - lb_n:td_i]].mean(axis=0)

			combine_return = pd.concat([exp_return, rlz_return], axis=1)
			combine_return.columns = ['expected_return', 'realized_return']
			combine_return = combine_return.applymap(lambda x: np.nan if np.isinf(np.abs(x)) else x)
			combine_return = combine_return.dropna()
			combine_return = (combine_return['expected_return'] * combine_return['realized_return'] > 0).astype(int)

			result_dict[trade_date.strftime('%Y-%m-%d')] = combine_return

		mean_win_raito = pd.DataFrame(result_dict).mean(axis=1)
		print(mean_win_raito.to_string())
		print()


def main_data_verification():
	# data verification
	data_analyzer.FX_TSA()
	data_analyzer.main_rolling_correlation_interpair()

	data_analyzer.get_fast_indicators()
	data_analyzer.get_slow_indicators()
	data_analyzer.main_fast_data_summary()
	data_analyzer.main_slow_data_summary()

	data_analyzer.main_relate_FX_with_macro_FAST()
	data_analyzer.main_relate_FX_with_macro_SLOW()


@timeit
def main_data_preparation():
	data_analyzer.unify_tickers_byAPI()
	data_analyzer.get_daily_FX_prices()
	data_analyzer.organize_slow()

	data_analyzer.generate_factors()
	data_analyzer.generate_future_returns()

def concat():
	path_old = 'D:\PycharmData\magnum_FX_project\PHASE II\model\model_output_asof20191204/'
	path_new = 'D:\PycharmData\magnum_FX_project\PHASE II\model/'
	path_combine = 'D:\PycharmData\magnum_FX_project\PHASE II\model/updated/'

	files = ['expected_return.csv', 'r_squared.csv']

	for file in files:
		df_old = pd.read_csv(path_old + file, index_col=0)
		df_new = pd.read_csv(path_new + file, index_col=0)
		df_old = df_old.T
		df_new = df_new.T
		df_old = df_old.loc[:'2019-09-30']
		df_new = df_new.loc['2019-10-01':]
		df_combine = pd.concat([df_old, df_new])
		df_combine.T.to_csv(path_combine + file)

if __name__ == '__main__':
	data_analyzer = DATA_ANALYZER(end_date='2020-01-31')
	# main_data_verification()
	main_data_preparation()

	# predict
	data_analyzer.main_prediction_model()
	# data_analyzer.evaluate_prediction_model()

	concat()

	print('end.')


