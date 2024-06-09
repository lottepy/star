import pandas as pd
import numpy as np
import os
import math
from algorithm import addpath
from datetime import timedelta

def result_metrics_calculation(pv):
    # path = os.path.join(addpath.result_path, portfolio_name, 'backtest_u_0_strategy_id_virtual_0.csv')
    # data = pd.read_csv(path, index_col='datetime', parse_dates=['datetime'])
    # data = data[['pv']]
    # data['pv'] = data['pv'] + cash
    # data = data.resample('1d').last().ffill()
    # data.iloc[0,0] = cash
    # data['pv'] = data['pv'] / cash - 1
    # data = data.rename(columns={'pv': portfolio_name})
    #
    # data.to_csv(os.path.join(addpath.result_path, portfolio_name, 'cumulative_return.csv'))
    RF = 0
    data=pv.copy()
    report = pd.DataFrame(None, index=data.columns.values)
    start = data.index[0]
    end = data.index[-1]
    period = (end - start).days
    variables = list(data.columns)
    data['YYYY'] = data.index.map(lambda x: x.year)

    report['Total Return'] = data.loc[data.index[-1], variables]-1
    report['Return p.a.'] = np.power(report['Total Return']+1, 365. / period) - 1
    pv = data[variables]
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
            report['Total Return_YTD'] = (data_tmp.loc[data_tmp.index[-1], variables]) / (
                        data_tmp.loc[data_tmp.index[0], variables]) - 1
            report['Return p.a._YTD'] = np.power(report['Total Return_YTD'] + 1., 365. / period_tmp) - 1
            pv_YTD = data_tmp
            daily_return_YTD = pv_YTD.pct_change().dropna()
            daily_return_YTD = daily_return_YTD[daily_return_YTD[variables[0]] != 0]
            report['Volatility_YTD'] = daily_return_YTD.std() * math.sqrt(252)
            report['Sharpe Ratio_YTD'] = (report['Return p.a._YTD'] - RF) / report['Volatility_YTD']
            report['Max Drawdown_YTD'] = (pv_YTD.div(pv_YTD.cummax()) - 1.).min()
            report['Max Daily Drop_YTD'] = daily_return.min()
            report['Calmar Ratio_YTD'] = report['Return p.a._YTD'] / abs(report['Max Drawdown_YTD'])
            report['99% VaR_YTD'] = daily_return_YTD.mean() - daily_return_YTD.std() * 2.32
        else:
            window_day = timedelta(window+1)
            data_tmp = data[data.index>=(data.index[-1]-window_day)]
            data_tmp = data_tmp.loc[:, variables]
            period_tmp = (data_tmp.index[-1] - data_tmp.index[0]).days
            report['Total Return_' + str(window)] = (data_tmp.loc[data_tmp.index[-1], variables]) / (
                        data_tmp.loc[data_tmp.index[0], variables]) - 1
            report['Return p.a._' + str(window)] = np.power(report['Total Return_' + str(window)] + 1.,
                                                            365. / period_tmp) - 1
            pv_tmp = data_tmp
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

    def metrics_year(year):
        if year == 'YTD':
            data_tmp = pd.concat(
                [pd.DataFrame(data[data['YYYY'] != end.year].iloc[-1, :]).transpose(), data[data['YYYY'] == end.year]])
            data_tmp = data_tmp.loc[:, variables]
            period_tmp = (data_tmp.index[-1] - data_tmp.index[0]).days
            report['Total Return_YTD'] = (data_tmp.loc[data_tmp.index[-1], variables]) / (
                    data_tmp.loc[data_tmp.index[0], variables]) - 1
            report['Return p.a._YTD'] = np.power(report['Total Return_YTD'] + 1., 365. / period_tmp) - 1
            pv_YTD = data_tmp
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
            report['Total Return' + str(year)] = (data_tmp.loc[data_tmp.index[-1], variables]) / (
                    data_tmp.loc[data_tmp.index[0], variables]) - 1
            report['Return p.a.' + str(year)] = np.power(report['Total Return' + str(year)] + 1., 365. / period_tmp) - 1
            pv_YTD = data_tmp
            daily_return_YTD = pv_YTD.pct_change().dropna()
            daily_return_YTD = daily_return_YTD[daily_return_YTD[variables[0]] != 0]
            report['Volatility' + str(year)] = daily_return_YTD.std() * math.sqrt(252)
            report['Sharpe Ratio' + str(year)] = (report['Return p.a.' + str(year)] - RF) / report[
                'Volatility' + str(year)]
            report['Max Drawdown' + str(year)] = (pv_YTD.div(pv_YTD.cummax()) - 1.).min()
            report['Max Daily Drop' + str(year)] = daily_return.min()
            report['Calmar Ratio' + str(year)] = report['Return p.a.' + str(year)] / abs(
                report['Max Drawdown' + str(year)])
            report['99% VaR' + str(year)] = daily_return_YTD.mean() - daily_return_YTD.std() * 2.32

    if (data.index[-1] - data.index[0]).days >= 365 * 5 + 1:
        metrics(365 * 5)

    if (data.index[-1] - data.index[0]).days >= 365 * 3:
        metrics(365 * 3)

    if (data.index[-1] - data.index[0]).days >= 365 * 2:
        metrics(365 * 2)

    if (data.index[-1] - data.index[0]).days >= 365:
        metrics(365)

    if data.YYYY[-1] - data.YYYY[0] >= 1:
        metrics("YTD")

    if (data.index[-1] - data.index[0]).days >= 180:
        metrics(180)
    if (data.index[-1] - data.index[0]).days >= 90:
        metrics(90)
    if (data.index[-1] - data.index[0]).days >= 30:
        metrics(30)

    if len(data[data.YYYY==2016])>0:
        metrics_year(2016)
    if len(data[data.YYYY==2017])>0:
        metrics_year(2017)
    if len(data[data.YYYY==2018])>0:
        metrics_year(2018)
    if len(data[data.YYYY==2019])>0:
        metrics_year(2019)

    # report.to_csv(os.path.join(addpath.result_path, portfolio_name, 'performance metrics.csv'))
    return report
