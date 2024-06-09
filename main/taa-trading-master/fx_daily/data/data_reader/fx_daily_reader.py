import os
import pandas as pd
import numpy as np
from tqdm import tqdm
from datetime import datetime
from fx_daily.data.config.config import ConfigManager

# cp = ConfigManager('data/config/config.ini')
# WINDOW_SIZE = cp.configs['window_size']


def load_data(parse_path, symbol, WINDOW_SIZE):
	# 1、先读取T150汇率数据
	T150_PATH = os.path.join(parse_path, 'T150_daily_data')
	select_columns = ['PRICE_LAST', 'PRICE_BID', 'PRICE_ASK']
	# for sym in tqdm(symbol):
	for sym in symbol:
		sym_path = os.path.join(T150_PATH, f'{sym}.csv')
		df = pd.read_csv(sym_path, index_col=0, parse_dates=True)
		df = df[select_columns]
		df.columns = [f"{f}_{sym}" for f in df.columns]
		df = df.astype(np.float64)
		# print("df: ", df.shape)
		if "res" not in locals():
			res = df
		else:
			res = res.join(df, how='outer')
			# print("res: ", res.shape)
	del df
	res.sort_index(inplace=True)
	res.ffill(inplace=True)

	# 2、读取对usd的汇率
	left_ccy = [sym[:3] for sym in symbol]
	right_ccy = [sym[3:] for sym in symbol]
	all_ccy = list(set(left_ccy + right_ccy))
	# 把USD放到最后一个(可能有更好的写法)
	if 'USD' in all_ccy:
		all_ccy.remove('USD')
	all_ccy.append('USD')

	# 得到ccy_pair position转换成ccy的矩阵
	ccy_matrix_df = pd.DataFrame(np.zeros((len(all_ccy), len(symbol))), index=all_ccy, columns=symbol)
	for sym in symbol:
		left_ccy = sym[:3]
		right_ccy = sym[3:]
		ccy_matrix_df.loc[left_ccy, sym] = 1
		ccy_matrix_df.loc[right_ccy, sym] = -1

	FX_AGAINST_USD_PATH = os.path.join(parse_path, 'FX_AGAINST_USD_PRICE_DAILY_T150.csv')
	FX_AGAINST_USD_df = pd.read_csv(FX_AGAINST_USD_PATH, index_col=0, parse_dates=True)
	FX_AGAINST_USD_df = FX_AGAINST_USD_df[all_ccy]

	# 3、读取currency的 interest rate
	Rate_df_list = []
	for field in ['LAST', 'BID', 'ASK']:
		FX_ON_RATE_PATH = os.path.join(parse_path, f'FX_ON_RATE_{field}.csv')
		FX_ON_RATE_df = pd.read_csv(FX_ON_RATE_PATH, index_col=0, parse_dates=True)
		all_ccy_column = [f'{ccy}_{field}' for ccy in all_ccy]
		FX_ON_RATE_df = FX_ON_RATE_df[all_ccy_column]
		Rate_df_list.append(FX_ON_RATE_df)
	Rate_df = pd.concat(Rate_df_list, axis=1)

	# 现在要保证res、FX_AGAINST_USD_df、Rate_df的长度一致
	res_column = res.columns
	FX_AGAINST_USD_column = FX_AGAINST_USD_df.columns
	Rate_column = Rate_df.columns

	all_df = res.join(FX_AGAINST_USD_df, how='outer')
	all_df = all_df.join(Rate_df, how='outer')
	all_df = all_df.sort_index().ffill()

	# 在这里判断一下时间
	datelist = all_df.index.tolist()
	#start_index = np.where(datelist >= start)[0][0]
	bid_ask_column = [col for col in res_column if ('BID' in col) | ('ASK' in col)]
	# last_column = [col for col in res_column if 'LAST' in col]
	consider_column = list(set(all_df.columns.tolist()) - set(bid_ask_column))
	first_valid_index = all_df.dropna(subset=consider_column).index[0]
	start_index = datelist.index(first_valid_index)-WINDOW_SIZE+1
	#first_window_index = all_df.dropna(subset=consider_column).index[WINDOW_SIZE-1]
	#all_df.index.tolist().index(first_valid_index)
	# assert datelist[start_index] >= first_valid_index, \
	# 	f"your start day has NAN value, suggest you start after {first_valid_index}\n"\
	# 	f"however, if you want your lookback window doesn't have any NAN, suggest you start after {first_window_index}"
	# back_start_index = start_index - WINDOW_SIZE + 1
	# 	# back_end_index = np.where(datelist <= end)[0][-1]
	all_df = all_df.iloc[start_index:]

	res = all_df[res_column]
	FX_AGAINST_USD_df = all_df[FX_AGAINST_USD_column]
	Rate_df = all_df[Rate_column]
	time_df = pd.DataFrame(all_df.index)

	# 把df转成array
	# 1、汇率
	res_column = []
	for sym in symbol:
		sym_column = [col + f"_{sym}" for col in select_columns]
		res_column += sym_column
	res_data = res[res_column]
	FxArray = res_data.values.reshape(res_data.shape[0], len(symbol), -1)

	# 2、对美元汇率
	FxUSDArray = FX_AGAINST_USD_df.values.reshape(FX_AGAINST_USD_df.shape[0], -1)

	# 3、利率
	rate_column = []
	rate_select = ['LAST', 'BID', 'ASK']
	for ccy in all_ccy:
		sym_column = [f"{ccy}_{field}" for field in rate_select]
		rate_column += sym_column
	Rate_df = Rate_df[rate_column]
	RateArray = Rate_df.values.reshape(Rate_df.shape[0], len(all_ccy), -1)

	# 4、时间
	time_df.rename(columns={0: 'date'}, inplace=True)
	time_df['timestamp'] = time_df['date'].apply(lambda x: datetime.timestamp(x))
	time_df['weekday'] = time_df['date'].apply(lambda x: x.weekday())
	time_df['year'] = time_df['date'].apply(lambda x: x.year)
	time_df['month'] = time_df['date'].apply(lambda x: x.month)
	time_df['day'] = time_df['date'].apply(lambda x: x.day)
	timeArray = time_df[['timestamp', 'weekday', 'year', 'month', 'day']].values

	return FxArray, FxUSDArray, RateArray, timeArray, ccy_matrix_df
