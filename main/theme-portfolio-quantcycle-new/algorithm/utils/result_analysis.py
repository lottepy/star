import pandas as pd
import numpy as np
import os
import math
from algorithm import addpath
from constants import *
from datetime import datetime, timedelta


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
    data=pv
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

    if len(data[data.YYYY==2016])>1:
        metrics_year(2016)
    if len(data[data.YYYY==2017])>1:
        metrics_year(2017)
    if len(data[data.YYYY==2018])>1:
        metrics_year(2018)
    if len(data[data.YYYY==2019])>1:
        metrics_year(2019)

    # report.to_csv(os.path.join(addpath.result_path, portfolio_name, 'performance metrics.csv'))
    return report


if __name__ == "__main__":
    # file_name_list=['sgm_basic.xlsx', 'sgm.xlsx', 'sgm_pure_bond.xlsx','sg_basic.xlsx', 'sg.xlsx', 'sg_pure_bond.xlsx']
    #
    # for file in file_name_list:
    #     print(file)
    #     pv=pd.read_excel(r'/Users/lizhirui/Desktop/test/strategy_kit/'+file,sheet_name='pv',parse_dates=[0],index_col=0)
    #     pv=pv+1
    #     if file in ['sgm_basic.xlsx', 'sgm.xlsx', 'sgm_pure_bond.xlsx']:
    #         pv=pv[pv.index>=datetime(2018,9,30)]
    #         pv = pv[pv.index <= datetime(2020, 12, 31)]
    #         pv=pv/pv.iloc[0,:]
    #     elif file in ['sg_basic.xlsx', 'sg.xlsx', 'sg_pure_bond.xlsx']:
    #         pv=pv[pv.index>=datetime(2017,11,30)]
    #         pv = pv[pv.index <= datetime(2020, 12, 31)]
    #         pv=pv/pv.iloc[0,:]
    #     report=result_metrics_calculation(pv)
    #     report.to_csv(r'/Users/lizhirui/Desktop/test/上线至今/'+file[:-5]+'_report.csv')
    #     pv.to_csv(r'/Users/lizhirui/Desktop/test/上线至今/'+file[:-5]+'.csv')



    # read_path=r'/Users/lizhirui/Desktop/test/price'
    # os.chdir(read_path)
    # file_name_list = os.listdir()
    # file_name_list.remove('report')
    # file_name_list.remove('.DS_Store')
    #
    # for file in file_name_list:
    #     print(file)
    #     pv=pd.read_csv(r'/Users/lizhirui/Desktop/test/price/'+file,parse_dates=[0],index_col=0)
    #     pv=pv.ffill().bfill()
    #
    #     pv=pv/pv.iloc[0,:]
    #     report=result_metrics_calculation(pv)
    #     report.to_csv(r'/Users/lizhirui/Desktop/test/price/report/'+file[:-4]+'_report.csv')


    backtest_path=r'/Users/lizhirui/Desktop/test/backtesting'
    tracking_path=r'/Users/lizhirui/Desktop/test/tracking'
    os.chdir(backtest_path)
    backtest_list = os.listdir()
    os.chdir(tracking_path)
    tracking_list = os.listdir()

    # pv_backtest_file = backtest_path+'/pv_backtest.xlsx'
    # pv_backtest_writer = pd.ExcelWriter(pv_backtest_file, engine="xlsxwriter")
    # report_backtest_file = backtest_path+'/report_backtest.xlsx'
    # report_backtest_writer = pd.ExcelWriter(report_backtest_file, engine="xlsxwriter")
    #
    # backtest_list=['sgm_basic.csv', 'US_Profit_B.csv', 'CN_Value.csv', 'US_Profit_S.csv', 'sg_pure_bond.csv', 'US_Safety_S.csv', 'CN_Quality.csv', 'US_Tech_S.csv', 'US_Safety_B.csv', 'sgm.csv', 'US_Tech_B.csv', 'sgm_pure_bond.csv', 'HK_Hstech_S.csv', 'HK_Hstech_B.csv', 'sg.csv', 'US_5G.csv', 'sg_basic.csv']
    #
    # for file in backtest_list:
    #     print(file[:-4])
    #     pv=pd.read_csv(os.path.join(backtest_path,file),parse_dates=[0],index_col=0)
    #     if file[:-4] in ['sgm_basic', 'sgm', 'sgm_pure_bond']:
    #         pv=pv+1
    #         pv = pv[pv.index <= datetime(2020, 12, 31)]
    #     elif file[:-4] in ['sg_basic', 'sg', 'sg_pure_bond']:
    #         pv=pv+1
    #         pv = pv[pv.index <= datetime(2020, 12, 31)]
    #     pv=pv/pv.iloc[0,:]
    #     report=result_metrics_calculation(pv)
    #     pv = pv.drop(columns=['YYYY'])
    #     pv.to_excel(pv_backtest_writer,sheet_name=file[:-4])
    #     # report=report[['Total Return_YTD','Return p.a._YTD','Volatility_YTD','Sharpe Ratio_YTD','Max Drawdown_YTD','Max Daily Drop_YTD']]
    #     report.to_excel(report_backtest_writer,sheet_name=file[:-4])
    # report_backtest_writer.save()
    # pv_backtest_writer.save()


    pv_tracking_file = tracking_path+'/pv_tracking.xlsx'
    pv_tracking_writer = pd.ExcelWriter(pv_tracking_file, engine="xlsxwriter")
    report_tracking_file = tracking_path+'/report_tracking.xlsx'
    report_tracking_writer = pd.ExcelWriter(report_tracking_file, engine="xlsxwriter")

    # tracking_list=['sgm_basic.xlsx', 'sg_pure_bond.xlsx', 'sgm_pure_bond.xlsx', 'sg_basic.xlsx', 'sg.xlsx', 'sgm.xlsx']
    tracking_list=['sg.xlsx', 'sgm.xlsx']

    for file in tracking_list:
        print(file[:-5])
        pv=pd.read_excel(os.path.join(tracking_path,file),sheet_name='pv',parse_dates=[0],index_col=0)
        if file[:-5] in ['sgm_basic', 'sgm', 'sgm_pure_bond']:
            pv=pv+1
            pv = pv[pv.index >= datetime(2016, 3, 1)]
            pv = pv[pv.index <= datetime(2020, 12, 31)]
        elif file[:-5] in ['sg_basic', 'sg', 'sg_pure_bond']:
            pv=pv+1
            pv = pv[pv.index >= datetime(2016, 3, 1)]
            pv = pv[pv.index <= datetime(2020, 12, 31)]
        pv=pv/pv.iloc[0,:]
        report=result_metrics_calculation(pv)
        pv = pv.drop(columns=['YYYY'])
        pv.to_excel(pv_tracking_writer,sheet_name=file[:-4])
        # report=report[['Total Return_YTD','Return p.a._YTD','Volatility_YTD','Sharpe Ratio_YTD','Max Drawdown_YTD','Max Daily Drop_YTD']]
        report.to_excel(report_tracking_writer,sheet_name=file[:-4])
    pv_tracking_writer.save()
    report_tracking_writer.save()
