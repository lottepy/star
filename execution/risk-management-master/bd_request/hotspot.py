import pandas as pd
import numpy as np
from datetime import datetime


def compute_correlation(df, start, end):
	data = df.loc[start: end]
	dr = data.pct_change().fillna(0.)
	corr = dr.corr()
	corr.to_csv('../data/market_corr.csv')
	return corr


def get_performance(df, start, end):
	data = df.loc[start: end]
	pv = data / data.iloc[0, :]
	pv.to_csv('../data/index_performance.csv')
	return pv


def compute_rolling_corr(df, assets):
	data = df[assets]
	dr = data.pct_change().fillna(0.)
	roll_corr = pd.rolling_corr(dr[assets[0]], dr[assets[1]], window=252).dropna()
	roll_corr.to_csv('../data/rolling_corr.csv')
	return roll_corr


def dd_analysis(df):
	df['lag-5'] = df['SPX Index'].shift(5)
	df['5d-return'] = df['SPX Index'] / df['lag-5'] - 1.

	recovery_dt = datetime(1990, 1, 1)
	record = []
	date_set = list(df.index.values)

	for i, row in df.iterrows():
		if i < recovery_dt:
			continue

		if row['5d-return'] < -0.0477:
			tmp = df.loc[i:]
			tmp['return'] = tmp['SPX Index'] / row['lag-5'] - 1.
			if np.any(tmp['return'].gt(0.)):
				recovery_dt = tmp[tmp['return'].gt(0.)].index[0]
				recovery_time = date_set.index(recovery_dt.to_datetime64()) - date_set.index(i.to_datetime64())
				record.append([i, row['SPX Index'], row['lag-5'], recovery_dt, recovery_time])
			else:
				record.append([i, row['SPX Index'], row['lag-5'], "?", "?"])

	result = pd.DataFrame(record, columns=['Date', 'P_t', 'P_t-5', 'Recovery Date', 'Recovery BDays'])
	result = result.set_index('Date')
	result.to_csv('../data/spx_dd_analysis.csv')

	return result


def fund_analysis(df):
	up_thresh = 0.2
	down_thresh = -0.3
	amount = 0.5
	df['dr'] = df['110023.OF'].pct_change().fillna(0.)
	df['R'] = df['110023.OF'] / df['110023.OF'].iloc[0] - 1.
	df['MDD'] = (df['110023.OF'].div(df['110023.OF'].cummax()) - 1.)
	first_monitor = True
	cap = 100.
	record = []
	position = 0.
	init_position = True
	cash_position = False
	ref_price = df['110023.OF'].iloc[0]

	for i, row in df.iterrows():
		if first_monitor:
			position = cap / row['110023.OF']
			pv = row['110023.OF'] * position
			first_monitor = False
			record.append([i, row['110023.OF'], position, pv])
		else:
			cond_up = row['110023.OF'] / ref_price - 1.
			if cond_up > up_thresh and (init_position or cash_position):
				if position == 0.:
					position = cap / row['110023.OF']
					pv = row['110023.OF'] * position
					record.append([i, row['110023.OF'], position, pv])
					break
				else:
					position = position + cap * amount / row['110023.OF']
				pv = row['110023.OF'] * position
				record.append([i, row['110023.OF'], position, pv])
				init_position = False
			elif row['MDD'] < down_thresh and not cash_position:
				pv = row['110023.OF'] * position
				position = 0.
				record.append([i, row['110023.OF'], position, pv])
				cash_position = True
				ref_price = row['110023.OF']
			else:
				pv = row['110023.OF'] * position
				record.append([i, row['110023.OF'], position, pv])

	result = pd.DataFrame(record, columns=['Date', 'Price', 'Position', 'PortfolioValue'])
	result = result.set_index('Date')

	result.to_csv('../data/110023.OF-stupid.csv')

	return result


def fund_analysis_down_in(df):
	up_thresh = 0.2
	down_thresh = -0.3
	amount = 0.5
	df['dr'] = df['110023.OF'].pct_change().fillna(0.)
	df['R'] = df['110023.OF'] / df['110023.OF'].iloc[0] - 1.
	df['MDD'] = (df['110023.OF'].div(df['110023.OF'].cummax()) - 1.)
	first_monitor = True
	cap = 100.
	record = []
	position = 0.
	init_position = True
	cash_position = False
	ref_price = df['110023.OF'].iloc[0]
	tmp = df

	for i, row in df.iterrows():
		if first_monitor:
			position = cap / row['110023.OF']
			pv = row['110023.OF'] * position
			first_monitor = False
			record.append([i, row['110023.OF'], position, pv])
		else:
			cond_up = row['110023.OF'] / ref_price - 1.
			dd = (tmp['110023.OF'].div(tmp['110023.OF'].cummax()) - 1.).loc[i]
			if dd < down_thresh:
				position = position + cap * amount / row['110023.OF']
				pv = row['110023.OF'] * position
				record.append([i, row['110023.OF'], position, pv])
				ref_price = row['110023.OF']
				tmp = df.loc[i:]
			else:
				pv = row['110023.OF'] * position
				record.append([i, row['110023.OF'], position, pv])

	result = pd.DataFrame(record, columns=['Date', 'Price', 'Position', 'PortfolioValue'])
	result = result.set_index('Date')

	result.to_csv('../data/110023.OF-smart.csv')

	return result


if __name__ == '__main__':
	start = '2014-09-12'
	end = '2018-10-10'
	assets = ['SPX Index', 'ACDER Index']

	market_index = pd.read_csv('../data/market_index.csv', index_col=0, parse_dates=True)
	sp500 = pd.read_csv('../data/sp500.csv', index_col=0, parse_dates=True)
	fund = pd.read_csv('../data/huarun.csv', index_col=0, parse_dates=True)

	# market_corr = compute_correlation(market_index, start, end)
	# print market_corr.to_string()
	#
	# index_performance = get_performance(market_index, start, end)
	# print index_performance.tail().to_string()

	# rolling_corr = compute_rolling_corr(market_index, assets)
	# result = dd_analysis(sp500.loc['2008-10-10':])
	result = fund_analysis(fund)
	# result = fund_analysis_down_in(fund)
	print result.to_string()