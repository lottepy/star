import pandas as pd
import numpy as np
import os
from algorithm import addpath
from constants import *

def result_metrics_calculation(datain):
    pv = datain.copy()
    pv.iloc[0, :] = 1
    data = datain - 1

    report = pd.DataFrame(None, index=data.columns.values)
    start = data.index[0]
    end = data.index[-1]
    period = (end - start).days
    variables = list(data.columns)
    data['YYYY'] = data.index.map(lambda x: x.year)

    report['Total Return'] = data.loc[data.index[-1], variables]
    report['Return p.a.'] = np.power(report['Total Return'] + 1., 365. / period) - 1
    daily_return = pv.pct_change().dropna()
    daily_return = daily_return[daily_return[variables[0]] != 0]
    report['Volatility'] = daily_return.std() * (252 ** 0.5)
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
            report['Volatility_YTD'] = daily_return_YTD.std() * (252 ** 0.5)
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
            report['Volatility_' + str(window)] = daily_return_tmp.std() * (252 ** 0.5)
            report['Sharpe Ratio_' + str(window)] = (report['Return p.a._' + str(window)] - RF) / report[
                'Volatility_' + str(window)]
            report['Max Drawdown_' + str(window)] = (pv_tmp.div(pv_tmp.cummax()) - 1.).min()
            report['Max Daily Drop_' + str(window)] = daily_return.min()
            report['Calmar Ratio_' + str(window)] = report['Return p.a._' + str(window)] / abs(
                report['Max Drawdown_' + str(window)])
            report['99% VaR_' + str(window)] = daily_return_tmp.mean() - daily_return_tmp.std() * 2.32

    if (data.index[-1] - data.index[0]).days >= 365 * 5 + 1:
        metrics(365 * 5)

    if (data.index[-1] - data.index[0]).days >= 365 * 3:
        metrics(365 * 3)

    if data.YYYY[-1] - data.YYYY[0] >= 1:
        metrics("YTD")

    if (data.index[-1] - data.index[0]).days >= 365:
        metrics(365)

    if (data.index[-1] - data.index[0]).days >= 180:
        metrics(180)
    if (data.index[-1] - data.index[0]).days >= 90:
        metrics(90)
    if (data.index[-1] - data.index[0]).days >= 30:
        metrics(30)

    return report