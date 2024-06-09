import pandas as pd
import numpy as np
import os
import math
from algorithm import addpath
from constants import *
import datetime

input_path = os.path.join(addpath.data_path, 'cn_data')
portfolio_name='CN_Trend'
cum_return=pd.read_csv(os.path.join(addpath.result_path,portfolio_name,'cumulative_return.csv'))
cum_return['date']=pd.to_datetime(cum_return['datetime'],format='%Y/%m/%d')
cum_return.set_index('date',inplace=True)
cum_return=cum_return.drop(columns=['datetime'])

'''
reference=pd.read_csv(os.path.join(input_path, 'reference', 'market_index.csv'))
market_index_path = os.path.join(input_path, "reference", "market_index.csv")
reference['date']=pd.to_datetime(reference['date'],format='%Y/%m/%d')
reference.set_index('date',inplace=True)
reference=reference.loc[cum_return.index[0]:cum_return.index[-1],:]
reference=reference.resample('1D').ffill()
reference=reference/reference.iloc[0,0]-1
data=pd.concat([cum_return,reference],axis=1)
data['ex_return']=data.iloc[:,0]-data.iloc[:,1]
data = data.rename(columns={'pv': portfolio_name})
data=data[['ex_return']]


data.to_csv(os.path.join(addpath.result_path, portfolio_name, 'excess_return.csv'))

report = pd.DataFrame(None, index=[portfolio_name])
start = data.index[0]
end = data.index[-1]
period = (end - start).days
variables = list(['ex_return'])
data['YYYY'] = data.index.map(lambda x: x.year)

report['Total Return'] = data.loc[data.index[-1], variables]
report['Return p.a.'] = np.power(report['Total Return'] + 1., 365. / period) - 1
pv = data + 1.
daily_return = pv.pct_change().dropna()
daily_return = daily_return[daily_return[variables[0]] != 0]
report['Volatility'] = daily_return.std() * math.sqrt(252)
report['Sharpe Ratio'] = (report['Return p.a.'] - RF) / report['Volatility']
report['Max Drawdown'] = (pv.div(pv.cummax()) - 1.).min()
report['Max Daily Drop'] = daily_return.min()
report['Calmar Ratio'] = report['Return p.a.'] / abs(report['Max Drawdown'])
report['99% VaR'] = daily_return.mean() - daily_return.std() * 2.32

def metrics(window):
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

# if (data.index[-1] - data.index[0]).days >= 365 * 5 + 1:
#     metrics(365 * 5)
#
# if (data.index[-1] - data.index[0]).days >= 365 * 3:
#     metrics(365 * 3)
#
# if data.YYYY[-1] - data.YYYY[0] >= 1:
#     metrics("YTD")
#
# if (data.index[-1] - data.index[0]).days >= 365:
#     metrics(365)
#
# if (data.index[-1] - data.index[0]).days >= 180:
#     metrics(180)
# if (data.index[-1] - data.index[0]).days >= 90:
#     metrics(90)
# if (data.index[-1] - data.index[0]).days >= 30:
#     metrics(30)

def metrics_year(year):
    if year == 'YTD':
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
        data_tmp = data[data['YYYY'] == year]
        data_tmp = data_tmp.loc[:, variables]
        period_tmp = (data_tmp.index[-1] - data_tmp.index[0]).days
        report['Total Return'+str(year)] = (1 + data_tmp.loc[data_tmp.index[-1], variables]) / (
                1 + data_tmp.loc[data_tmp.index[0], variables]) - 1
        report['Return p.a.'+str(year)] = np.power(report['Total Return'+str(year)] + 1., 365. / period_tmp) - 1
        pv_YTD = data_tmp + 1.
        daily_return_YTD = pv_YTD.pct_change().dropna()
        daily_return_YTD = daily_return_YTD[daily_return_YTD[variables[0]] != 0]
        report['Volatility'+str(year)] = daily_return_YTD.std() * math.sqrt(252)
        report['Sharpe Ratio'+str(year)] = (report['Return p.a.'+str(year)] - RF) / report['Volatility'+str(year)]
        report['Max Drawdown'+str(year)] = (pv_YTD.div(pv_YTD.cummax()) - 1.).min()
        report['Max Daily Drop'+str(year)] = daily_return.min()
        report['Calmar Ratio'+str(year)] = report['Return p.a.'+str(year)] / abs(report['Max Drawdown'+str(year)])
        report['99% VaR'+str(year)] = daily_return_YTD.mean() - daily_return_YTD.std() * 2.32




metrics_year(2016)
metrics_year(2017)
metrics_year(2018)
metrics_year(2019)
metrics_year('YTD')



report.to_csv(os.path.join(addpath.result_path, portfolio_name, 'performance metrics_year.csv'))



'''
# import datetime
#
#
# symbol='300558.SZ'
# date=datetime.datetime(2020,6,30)
# df_path = os.path.join(input_path, "trading", symbol+'.csv')
# df=pd.read_csv(df_path)
# df['date']=pd.to_datetime(df['date'],format='%Y/%m/%d')
# df.set_index('date',inplace=True)
# price=df.loc[date,'PX_LAST_RAW']
# print(price)
#
# start=datetime.datetime(2015,12,31)
# end=datetime.datetime(2020,6,30)
# rebalance_freq='QUARTERLY'
#
# from algorithm.utils.helpers import cal_rebalancing_dates
# rebalancing_dates = cal_rebalancing_dates(start, end, rebalance_freq)
# data=pd.read_csv(os.path.join(addpath.result_path, portfolio_name, 'backtest_u_0_strategy_id_virtual_0.csv'))
# data['datetime']=pd.to_datetime(data['datetime'],format='%Y/%m/%d')
# data.set_index('datetime',inplace=True)
# data=data.resample('1Q').last()
# data1=abs(data-data.shift())
# data.iloc[1:,:]=data1.iloc[1:,:]
# turnover=0.
# df=data.iloc[0,:]
#
#
# symbol_list=['002557.SZ', '002726.SZ', '601169.SH', '002092.SZ', '002468.SZ', '000889.SZ', '000065.SZ', '002387.SZ', '600180.SH', '300026.SZ', '300058.SZ', '600000.SH', '002268.SZ', '600273.SH', '300339.SZ', '002414.SZ', '000627.SZ', '600893.SH', '601808.SH', '002123.SZ', '000800.SZ', '002665.SZ', '002773.SZ', '002191.SZ', '601006.SH', '600511.SH', '600011.SH', '601069.SH', '002044.SZ', '603609.SH', '000021.SZ', '000830.SZ', '300315.SZ', '600803.SH', '600985.SH', '002099.SZ', '600596.SH', '600787.SH', '600776.SH', '600167.SH', '000936.SZ', '002714.SZ', '300394.SZ', '002607.SZ', '300418.SZ', '600028.SH', '600588.SH', '002202.SZ', '000831.SZ', '002812.SZ', '600999.SH', '000550.SZ', '600406.SH', '600019.SH', '600742.SH', '603678.SH', '300457.SZ', '002563.SZ', '601158.SH', '002157.SZ', '300133.SZ', '002013.SZ', '300118.SZ', '002301.SZ', '600486.SH', '600754.SH', '300292.SZ', '300043.SZ', '600104.SH', '000969.SZ', '300159.SZ', '002402.SZ', '002493.SZ', '600132.SH', '002010.SZ', '600271.SH', '600392.SH', '300233.SZ', '601088.SH', '002294.SZ', '600547.SH', '002020.SZ', '000975.SZ', '600383.SH', '600548.SH', '000681.SZ', '600183.SH', '000961.SZ', '603883.SH', '601800.SH', '002138.SZ', '601618.SH', '600111.SH', '300113.SZ', '002299.SZ', '002609.SZ', '600681.SH', '002466.SZ', '002390.SZ', '002110.SZ', '300009.SZ', '600094.SH', '000156.SZ', '002151.SZ', '603118.SH', '000568.SZ', '300123.SZ', '300458.SZ', '002446.SZ', '600038.SH', '002626.SZ', '000538.SZ', '000818.SZ', '300202.SZ', '002171.SZ', '601699.SH', '002326.SZ', '601021.SH', '000008.SZ', '000006.SZ', '000676.SZ', '600848.SH', '002745.SZ', '000976.SZ', '002697.SZ', '603508.SH', '601377.SH', '002653.SZ', '300054.SZ', '002155.SZ', '300168.SZ', '600643.SH', '300166.SZ', '600298.SH', '000910.SZ', '600549.SH', '000671.SZ', '601016.SH', '000059.SZ', '601328.SH', '600897.SH', '000090.SZ', '000413.SZ', '002195.SZ', '600161.SH', '002481.SZ', '601633.SH', '600072.SH', '002135.SZ', '600516.SH', '000977.SZ', '600150.SH', '002042.SZ', '002573.SZ', '600346.SH', '603806.SH', '603108.SH', '002118.SZ', '600369.SH', '603589.SH', '603338.SH', '000829.SZ', '000062.SZ', '600160.SH', '000301.SZ', '002063.SZ', '600580.SH', '300498.SZ', '000990.SZ', '600422.SH', '300212.SZ', '600996.SH', '002670.SZ']





# for symbol in symbol_list:
#     tmp_td_path = os.path.join(input_path, "trading", symbol + ".csv")
#     trading_data=pd.read_csv(tmp_td_path, parse_dates=[0], index_col=0)
#     close=trading_data.loc[start:end,'PX_LAST_RAW']
#     close.resample('1Q').last()
#     sum_symbol=close * data[symbol + '_position']
#     turnover=turnover+sum_symbol.dropna().sum()
# for date in rebalancing_dates[1:]:
#     for symbol in symbol_list:
#         tmp_td_path = os.path.join(input_path, "trading", symbol + ".csv")
#         trading_data = pd.read_csv(tmp_td_path, parse_dates=[0], index_col=0)
#         close = trading_data.loc[date, 'PX_LAST_RAW']
#         df = data.loc[date, :]
#         df_last=data.loc[datetime.datetime(date.year,date.month,date.day-1), :]
#         turnover = turnover + close *abs( df[symbol + '_position']-df_last[symbol + '_position'])
# print(turnover)
# print(CASH)
# print(turnover/CASH)
# print(turnover/CASH/4.5)



cum_return=pd.read_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/results/US_Profit/cumreturn_all.csv')
# cum_return=pd.to_datetime(cum_return.index,format='%y-%m-%d',errors='ignore')

# reference_return=pd.read_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/us_data/reference/market_index.csv')
# # reference_return.index=pd.to_datetime(reference_return.index.tolist(),format='%y/%m/%d',errors='ignore')
# date_start=cum_return['datetime'][0]
# date_start=date_start[0:4]+'/'+date_start[5:7]+'/'+date_start[8:10]
# index_start=reference_return.index[reference_return['date']==date_start]
# date_end=cum_return.iloc[-1]['datetime']
# date_end=date_end[0:4]+'/'+date_end[5:7]+'/'+date_end[8:10]
# index_end=reference_return.index[reference_return['date']==date_end]

# reference_return=reference_return.loc[index_start[0]:(index_end[0]+1),:]
# reference_return=reference_return['market_index']/reference_return['market_index'][index_start[0]]
y=cum_return.iloc[:,1]
x=cum_return.iloc[:,-1]

import statsmodels.api as sm
import matplotlib.pyplot as plt
x1 = sm.add_constant(x)
model = sm.OLS(y, x1).fit()
print(model.summary())

predicts = model.predict()
plt.scatter(x, y, label='realized values')
plt.plot(x, predicts, color = 'red', label='predicted values')
plt.legend()
plt.show()