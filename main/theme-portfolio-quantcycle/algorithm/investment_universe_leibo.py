'''
Screen out investment universe on each rebalancing date based on:
1. Size

The MV of each individual security refers to the average of month-end MVs for the past 12
months of any review period.

Securities in the universe are sorted in descending order of MV and the cumulative MV
coverage is calculated at each security. Securities among those that constitute the
cumulative MV coverage of top 90% are eligible.

2. Liquidity

12-month Annual Traded Value Ratio (ATVR) > 20%,
3-month ATVR > 20%
turnover:
For each security, turnover in the past 24 months is assessed for eight quarterly sub-periods
The turnover requirement adopts a scoring approach, with details as follows:
(a) for each quarterly sub-period, securities in the universe are sorted in descending order
of aggregate turnover and the cumulative aggregate turnover coverage is calculated at
each security. A security will be regarded as passing the turnover requirement in that
period if it is among the top 90% of the cumulative aggregate turnover coverage
(b) two points will be assigned for each ‘pass’ achieved over the latest four sub-periods,
and one point will be assigned for each ‘pass’ attained over the previous four
sub-periods
The highest score for turnover requirement is 12 points. Securities should obtain at least 8
points to meet the turnover requirement.

3. Minimum Length of Trading Requirement - Trading history >= 2 year

4. Minimum per share price == HKD 1.00


'''

import pandas as pd
from datetime import datetime
import os
from algorithm import addpath
import calendar
import pickle
import multiprocessing

def investment_universe(symbol_list, trading_data_total, formation_date, underlying_type, market_cap_threshold):
	trading_data = trading_data_total.copy()
	min_price = 5
	screening_criteria_list = []
	for symbol in symbol_list:
		temp = trading_data[symbol]
		temp=temp[['PX_LAST','PX_LAST_RAW','PX_VOLUME_RAW','EQY_SH_OUT_RAW']]
		if temp.shape[0] == 0:
			continue
		temp['MARKET_CAP'] = temp['PX_LAST_RAW'] * temp['EQY_SH_OUT_RAW']
		temp['ret_daily'] = temp['PX_LAST'].pct_change()
		rst = pd.DataFrame()
		rst['symbol'] = [symbol]
		rst['MARKET_CAP'] = temp['MARKET_CAP'].iloc[-1]

		# Trading history
		trading_hist_judge = temp[temp.index < datetime(formation_date.year - 2, formation_date.month,
																 calendar.monthrange(formation_date.year - 2,
																					 formation_date.month)[1])]
		trading_hist_judge = trading_hist_judge[~pd.isnull(trading_hist_judge['PX_VOLUME_RAW'])]
		trading_hist_judge = trading_hist_judge[~pd.isnull(trading_hist_judge['MARKET_CAP'])]
		if trading_hist_judge.shape[0] >= 1:
			rst['enought_trading_history'] = True
			temp = temp[temp.index > datetime(formation_date.year - 2, formation_date.month,
													   calendar.monthrange(formation_date.year - 2,
																		   formation_date.month)[1])]
			temp = temp.ffill()
			temp = temp[temp.index <= formation_date]
			temp = temp[~pd.isnull(temp['PX_LAST_RAW'])]

			# minimum price
			if temp.shape[0] > 0:
				price_raw = temp.iloc[-1]
				if price_raw['PX_LAST_RAW'] >= min_price:
					rst['above_min_price'] = True
				else:
					rst['above_min_price'] = False
			else:
				continue

			# 3-month ATVR and 12-month ATVR
			temp['dvolume'] = temp['PX_VOLUME_RAW'] * temp['PX_LAST_RAW']
			atvr_raw = pd.DataFrame()
			atvr_raw['dvolume_mean'] = temp['dvolume'].resample('1M').mean()
			atvr_raw['dvolume_med'] = temp['dvolume'].resample('1M').median()
			atvr_raw['n_days_month'] = temp['dvolume'].resample('1M').count()
			atvr_raw['MARKET_CAP_end'] = temp['MARKET_CAP'].resample('1M').last()
			atvr_raw['mth_med_tvr'] = atvr_raw['dvolume_med'] * atvr_raw['n_days_month'] / atvr_raw['MARKET_CAP_end']
			atvr_raw = atvr_raw[atvr_raw.index >= datetime(formation_date.year - 1, formation_date.month,
																	calendar.monthrange(formation_date.year - 1,
																						formation_date.month)[1])]

			rst['ATVR_12m'] = atvr_raw['mth_med_tvr'].mean() * 12
			atvr_raw = atvr_raw.iloc[-3:]
			rst['ATVR_3m'] = atvr_raw['mth_med_tvr'].mean() * 12
			# 1-mon Average Daily Value
			atvr_raw = atvr_raw.iloc[-1:]
			rst['ADV'] = atvr_raw['dvolume_mean'].mean() *1

			#volatility
			# rst['RealizedVol_1M'] = temp['ret_daily'].rolling(window=23).std(ddof=0)[-1]
			# rst['RealizedVol_12M'] = temp['ret_daily'].rolling(window=252).std(ddof=0)[-1]

		else:
			rst['enought_trading_history'] = False
			rst['above_min_price'] = None
			rst['ATVR_12m'] = None
			rst['ATVR_3m'] = None
			rst['ADV'] = None
			# rst['RealizedVol_1M']=None
			# rst['RealizedVol_12M']=None

		screening_criteria_list.append(rst)

	screening_criteria = pd.concat(screening_criteria_list)
	screening_criteria = screening_criteria.dropna()

	# Exclude stocks in the least 1% of market cap
	screening_criteria = screening_criteria.sort_values(by='MARKET_CAP', ascending=False)
	screening_criteria['market_cap_prop'] = screening_criteria['MARKET_CAP'] / screening_criteria['MARKET_CAP'].sum()
	screening_criteria['cum_market_prop'] = screening_criteria['market_cap_prop'].cumsum()
	screening_criteria = screening_criteria[screening_criteria['cum_market_prop'] < market_cap_threshold]

	# Exclude stocks without enough trading history or price below minimum price required
	screening_criteria = screening_criteria[screening_criteria['enought_trading_history'] == True]
	screening_criteria = screening_criteria[screening_criteria['above_min_price'] == True]
	if underlying_type=="US_STOCK":
		screening_criteria = screening_criteria[screening_criteria['ATVR_12m'] >= 0.2]
		screening_criteria = screening_criteria[screening_criteria['ATVR_3m'] >= 0.2]
	else:
		screening_criteria = screening_criteria[screening_criteria['ATVR_12m'] >= 0.15]
		screening_criteria = screening_criteria[screening_criteria['ATVR_3m'] >= 0.15]
	if underlying_type == "US_STOCK":
		screening_criteria = screening_criteria[screening_criteria['ADV'] >= 1000000]
	elif underlying_type == "CN_STOCK":
		screening_criteria = screening_criteria[screening_criteria['ADV'] >= 8000000]
	# screening_criteria = screening_criteria[screening_criteria['RealizedVol_1M']<\
	# 										screening_criteria['RealizedVol_1M'].quantile(0.9)]
	# screening_criteria = screening_criteria[screening_criteria['RealizedVol_12M']<\
	# 										screening_criteria['RealizedVol_12M'].quantile(0.9)]

	investment_univ = screening_criteria['symbol'].tolist()

	return investment_univ

def investment_univ_helper(stock_list, formation_date, us_path, trading_data_total):
	trading_data_total = trading_data_total_1.copy()
	trading_data_total.update(trading_data_total_2)
	trading_data_total.update(trading_data_total_3)
	trading_data_total.update(trading_data_total_4)

	symbol_list = stock_list
	trading_data = trading_data_total
	underlying_type = 'US_STOCK'
	market_cap_threshold = 0.975    #cn 0.99
	investment_univ = investment_universe(symbol_list, trading_data, formation_date, underlying_type,
										  market_cap_threshold)
	print(investment_univ)
	investment_univ = pd.DataFrame(investment_univ, columns=['symbol'])
	investment_univ.to_csv(os.path.join(us_path, "investment_univ", formation_date.strftime('%Y-%m-%d') + ".csv"),
						   index=False)
	print(formation_date.strftime('%Y-%m-%d') + ".csv"+"written")

if __name__ == "__main__":
	us_path = os.path.join(addpath.data_path,"12_31_data", "us_data")
	stock_list = []   #pd.read_csv(os.path.join(us_path, "symbol_list.csv"))['symbol'].tolist()
	trading_path=os.path.join(us_path, "trading")
	os.chdir(trading_path)
	csv_name_list = os.listdir()
	trading_data_total = {}
	for ticker in csv_name_list:
		ticker=ticker[:-4]
		print(ticker)
		stock_list.append(ticker)
		data_in=pd.read_csv(os.path.join(us_path, "trading", ticker+'.csv'), parse_dates=[0], index_col=0)
		trading_data_total[ticker] = data_in

	with open(os.path.join(us_path, "trading_date_dict.pickle"), "wb") as fp:  # Pickling
		pickle.dump(trading_data_total, fp, protocol=pickle.HIGHEST_PROTOCOL)
	with open(os.path.join(us_path, "trading_date_dict.pickle"), "rb") as fp:  # Pickling
		trading_data_total = pickle.load(fp)

	trading_data_total_keys = list(trading_data_total.keys())
	trading_data_total_1 = {}
	for i in range(1500):
		print(i)
		trading_data_total_1[trading_data_total_keys[i]] = trading_data_total[trading_data_total_keys[i]]
	trading_data_total_2 = {}
	for i in range(1500, 3000, 1):
		print(i)
		trading_data_total_2[trading_data_total_keys[i]] = trading_data_total[trading_data_total_keys[i]]
	trading_data_total_3 = {}
	for i in range(3000, 4500, 1):
		print(i)
		trading_data_total_3[trading_data_total_keys[i]] = trading_data_total[trading_data_total_keys[i]]
	trading_data_total_4 = {}
	for i in range(4500, len(trading_data_total_keys), 1):
		print(i)
		trading_data_total_4[trading_data_total_keys[i]] = trading_data_total[trading_data_total_keys[i]]
	trading_data_total_5 = {}

	# formation_dates = pd.date_range('2016-11-01', '2020-08-31', freq='1M')
	formation_dates = pd.date_range('2020-09-30', '2020-12-31', freq='1M')
	pool = multiprocessing.Pool()
	for formation_date in formation_dates:
		print(formation_date)
		# pool.apply_async(investment_univ_helper, args=(stock_list, formation_date, us_path, trading_data_total_1,
		# 											   trading_data_total_2, trading_data_total_3, trading_data_total_4,
		# 											   trading_data_total_5))
		# print('Sub-processes begins!')
		investment_univ_helper(stock_list, formation_date, us_path, trading_data_total)
	pool.close()
	pool.join()
	print('Sub-processes done!')
