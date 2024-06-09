import pandas as pd
import numpy as np
import datetime
import math
import os

CALENDAR_DAYS = 365.
BUSINESS_DAYS = 252.
# RF = 2.37563 / 100
RF = 0. / 100

path = r'C:\Users\PC\Documents\Aqumon\SmartQuantimental Allocation CN\固收+\策略结构\Bond Fund Pool.csv'
infos = pd.read_csv(path)
infos = infos.rename(columns={'SecId': 'SECID'})
infos = infos.dropna(subset=['SECID'])
infos = infos.drop_duplicates(subset='SECID')
fund_list = infos['SECID'].tolist()

read_path = r'C:\Users\PC\PycharmProjects\magnum-smartfixedincomeplus-cn\data\Backtest\fund_nav'
data_list = []
for fund in fund_list:
	if os.path.exists(read_path + "\\" + fund + ".csv"):
		trade_dt_tmp = pd.read_csv(read_path + "\\" + fund + ".csv", parse_dates=['date'], index_col='date')
		trade_dt_tmp = trade_dt_tmp[['close']]
		trade_dt_tmp = trade_dt_tmp.rename(columns={'close': fund})
		data_list.append(trade_dt_tmp)
	else:
		print("No data for " + fund)
data = pd.concat(data_list, axis=1)
data.to_csv(r'C:\Users\PC\Documents\Aqumon\SmartQuantimental Allocation CN\固收+\策略结构\Bond Fund Pool NAV.csv')
end = data.index[-1]
data = data.resample('1d').last().ffill()
report = pd.DataFrame(None, index=data.columns.values)
variables = list(data.columns)
data['YYYY'] = data.index.map(lambda x: x.year)

def metrics(window):
	if window == 'YTD':
		data_tmp = pd.concat([pd.DataFrame(data[data['YYYY'] != end.year].iloc[-1, :]).transpose(), data[data['YYYY'] == end.year]])
		data_tmp = data_tmp.loc[:, variables]
		period_tmp = (data_tmp.index[-1] - data_tmp.index[0]).days
		report['Total Return_YTD'] = data_tmp.loc[data_tmp.index[-1], variables] / data_tmp.loc[data_tmp.index[0], variables] - 1
		report['Return p.a._YTD'] = np.power(report['Total Return_YTD'] + 1., 365. / period_tmp) - 1
		pv_YTD = data_tmp
		daily_return_YTD = pv_YTD.pct_change().dropna()
		daily_return_YTD = daily_return_YTD[daily_return_YTD[variables[0]] != 0]
		report['Volatility_YTD'] = daily_return_YTD.std() * math.sqrt(365)
		report['Sharpe Ratio_YTD'] = (report['Return p.a._YTD'] - RF) / report['Volatility_YTD']
		report['Max Drawdown_YTD'] = (pv_YTD.div(pv_YTD.cummax()) - 1.).min()
		report['Max Daily Drop_YTD'] = daily_return_YTD.min()
		report['Calmar Ratio_YTD'] = report['Return p.a._YTD'] / abs(report['Max Drawdown_YTD'])
		report['99% VaR_YTD'] = daily_return_YTD.mean() - daily_return_YTD.std() * 2.32
	else:
		data_tmp = data.iloc[-window - 1:, :]
		data_tmp = data_tmp.loc[:, variables]
		period_tmp = (data_tmp.index[-1] - data_tmp.index[0]).days
		report['Total Return_' + str(window)] = data_tmp.loc[data_tmp.index[-1], variables] / data_tmp.loc[data_tmp.index[0], variables] - 1
		report['Return p.a._' + str(window)] = np.power(report['Total Return_' + str(window)] + 1., 365. / period_tmp) - 1
		pv_tmp = data_tmp
		daily_return_tmp = pv_tmp.pct_change().dropna(how='all')
		daily_return_tmp = daily_return_tmp[daily_return_tmp[variables[0]] != 0]
		report['Volatility_' + str(window)] = daily_return_tmp.std() * math.sqrt(365)
		report['Sharpe Ratio_' + str(window)] = (report['Return p.a._' + str(window)] - RF) / report['Volatility_' + str(window)]
		report['Max Drawdown_' + str(window)] = (pv_tmp.div(pv_tmp.cummax()) - 1.).min()
		report['Max Daily Drop_' + str(window)] = daily_return_tmp.min()
		report['Calmar Ratio_' + str(window)] = report['Return p.a._' + str(window)] / abs(report['Max Drawdown_' + str(window)])
		report['99% VaR_' + str(window)] = daily_return_tmp.mean() - daily_return_tmp.std() * 2.32

metrics(365 * 5)
metrics(365 * 3)
# metrics("YTD")
metrics(365)
# metrics(180)
# metrics(90)
# metrics(30)

def year_metrics(year):
    bg = datetime.datetime(year-1, 12, 31)
    ed = datetime.datetime(year, 12, 31)
    dt_rg = pd.date_range(bg, ed, freq='D')
    dt_rg = [dt for dt in dt_rg if dt in data.index]
    data_tmp = data.loc[dt_rg, variables]
    period_tmp = (data_tmp.index[-1] - data_tmp.index[0]).days
    report['Total Return_' + str(year)] = data_tmp.loc[data_tmp.index[-1], variables] / data_tmp.loc[data_tmp.index[0], variables] - 1
    report['Return p.a._' + str(year)] = np.power(report['Total Return_' + str(year)] + 1., 365. / period_tmp) - 1
    pv_tmp = data_tmp.copy()
    daily_return_tmp = pv_tmp.pct_change().dropna(how='all')
    daily_return_tmp = daily_return_tmp[daily_return_tmp[variables[0]] != 0]
    report['Volatility_' + str(year)] = daily_return_tmp.std() * math.sqrt(365)
    report['Sharpe Ratio_' + str(year)] = (report['Return p.a._' + str(year)] - RF) / report[
        'Volatility_' + str(year)]
    report['Max Drawdown_' + str(year)] = (pv_tmp.div(pv_tmp.cummax()) - 1.).min()
    report['Max Daily Drop_' + str(year)] = daily_return_tmp.min()
    report['Calmar Ratio_' + str(year)] = report['Return p.a._' + str(year)] / abs(
        report['Max Drawdown_' + str(year)])
    report['99% VaR_' + str(year)] = daily_return_tmp.mean() - daily_return_tmp.std() * 2.32


year_metrics(2020)
year_metrics(2019)
year_metrics(2018)
year_metrics(2017)
# year_metrics(2016)
# year_metrics(2015)

report = pd.concat([infos.set_index('SECID'), report], sort=False, axis=1)
report.to_csv(path.replace('.csv', '_stats.csv'), encoding='UTF-8-sig')

