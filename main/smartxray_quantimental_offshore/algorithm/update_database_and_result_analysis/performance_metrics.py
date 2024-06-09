import pandas as pd
import numpy as np
import datetime
import math
from os import makedirs
from os.path import exists, join
from algorithm.addpath import result_path

def metrics(data, window):
	RF = 0.0
 
	report = pd.DataFrame(None, index=data.columns.values)
	start = data.index[0]
	end = data.index[-1]
	variables = list(data.columns)
 
	pv = data + 1.
	daily_return = pv.pct_change().dropna()
	daily_return = daily_return[daily_return[variables[0]] != 0]
	if window == 'YTD':
		data_tmp = pd.concat(
			[pd.DataFrame(data[data['YYYY'] != end.year].iloc[-1, :]).transpose(), data[data['YYYY'] == end.year]])
		data_tmp = data_tmp.loc[:, variables]
		period_tmp = (data_tmp.index[-1] - data_tmp.index[0]).days
		report['Total Return_YTD'] = (1 + data_tmp.loc[data_tmp.index[-1], variables]) / (
					1 + data_tmp.loc[data_tmp.index[0], variables]) - 1
		report['Return p.a._YTD'] = np.power(report['Total Return_YTD'] + 1., 365. / period_tmp) - 1
		pv_YTD = data_tmp + 1.
		daily_return_YTD = pv_YTD.pct_change().dropna()
		daily_return_YTD = daily_return_YTD[daily_return_YTD[variables[0]] != 0]
		report['Volatility_YTD'] = daily_return_YTD.std() * math.sqrt(252)
		report['Sharpe Ratio_YTD'] = (report['Return p.a._YTD'] - RF) / report['Volatility_YTD']
		report['Max Drawdown_YTD'] = (pv_YTD.div(pv_YTD.cummax()) - 1.).min()
		report['Max Daily Drop_YTD'] = daily_return.min()
		report['Calmar Ratio_YTD'] = report['Return p.a._YTD'] / abs(report['Max Drawdown_YTD'])
		report['99% VaR_YTD'] = daily_return_YTD.mean() - daily_return_YTD.std() * 2.32
	else:
		data_tmp = data.iloc[-window - 1:, :]
		data_tmp = data_tmp.loc[:, variables]
		period_tmp = (data_tmp.index[-1] - data_tmp.index[0]).days
		report['Total Return_' + str(window)] = (1 + data_tmp.loc[data_tmp.index[-1], variables]) / (
					1 + data_tmp.loc[data_tmp.index[0], variables]) - 1
		report['Return p.a._' + str(window)] = np.power(report['Total Return_' + str(window)] + 1.,
														365. / period_tmp) - 1
		pv_tmp = data_tmp + 1.
		daily_return_tmp = pv_tmp.pct_change().dropna()
		daily_return_tmp = daily_return_tmp[daily_return_tmp[variables[0]] != 0]
		report['Volatility_' + str(window)] = daily_return_tmp.std() * math.sqrt(252)
		report['Sharpe Ratio_' + str(window)] = (report['Return p.a._' + str(window)] - RF) / report[
			'Volatility_' + str(window)]
		report['Max Drawdown_' + str(window)] = (pv_tmp.div(pv_tmp.cummax()) - 1.).min()
		report['Max Daily Drop_' + str(window)] = daily_return.min()
		report['Calmar Ratio_' + str(window)] = report['Return p.a._' + str(window)] / abs(
			report['Max Drawdown_' + str(window)])
		report['99% VaR_' + str(window)] = daily_return_tmp.mean() - daily_return_tmp.std() * 2.32
	return report

if __name__ == '__main__':
	# rebalance_freq = '6M'
	# model_freq = '12M'
	CALENDAR_DAYS = 365.
	# BUSINESS_DAYS = 252.
	BUSINESS_DAYS = 250.
	# RF = 2.37563 / 100
	RF = 0. / 100

	# path = join(result_path, 'backtest', "Result" + "_" + rebalance_freq + "-" + model_freq + ".csv")
	todatabase_path = join(result_path, 'todatabase', 'backtest_overall.csv')

	data = pd.read_csv(todatabase_path, index_col=0, parse_dates=True)
	data = data.resample('1d').last().ffill()
	report = pd.DataFrame(None, index=data.columns.values)
	# start = datetime.strptime(data.index.values[0], '%m/%d/%Y')
	# end = datetime.strptime(data.index.values[-1], '%m/%d/%Y')
	start = data.index[0]
	end = data.index[-1]
	period = (end - start).days
	variables = list(data.columns)
	data['YYYY'] = data.index.map(lambda x: x.year)

	if data.iloc[0, 0] > 0.5:
		data = data - 1.
	pv = data + 1.
	daily_return = pv.pct_change().dropna()
	daily_return = daily_return[daily_return[variables[0]] != 0]

	#
	# report['Total Return'] = data.loc[data.index[-1], variables]
	# report['Return p.a.'] = np.power(report['Total Return'] + 1., 365. / period) - 1
	# report['Volatility'] = daily_return.std() * math.sqrt(BUSINESS_DAYS)
	# report['Sharpe Ratio'] = (report['Return p.a.'] - RF) / report['Volatility']
	# report['Max Drawdown'] = (pv.div(pv.cummax()) - 1.).min()
	# report['Max Daily Drop'] = daily_return.min()
	# report['Calmar Ratio'] = report['Return p.a.'] / abs(report['Max Drawdown'])
	# report['99% VaR'] = daily_return.mean() - daily_return.std() * 2.32
	#
	# metrics(365 * 5)
	# metrics(365 * 3)
	# metrics("YTD")
	# metrics(365)
	# metrics(180)
	# metrics(90)
	# metrics(30)
	report = metrics(data, 365)
	print(report.to_string())

	output_path = join(result_path, 'performance_metrics')
	if exists(output_path):
		pass
	else:
		makedirs(output_path)

	report.to_csv(join(output_path, 'stats.csv'))

	rolling_q = daily_return.rolling(500).quantile(0.01)
	rolling_q.to_csv(join(output_path, 'rolling_quantile.csv'))
