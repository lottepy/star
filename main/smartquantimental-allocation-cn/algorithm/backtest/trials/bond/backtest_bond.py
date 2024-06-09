
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from algorithm import addpath
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader
from os.path import dirname, abspath, join, exists
from algorithm.hshare_select.portfolio_formation import portfolio_formation
import math
import itertools
import warnings
warnings.filterwarnings("ignore")


def create_backtest_engine_files(formation_dates, base_ccy, cash, commission):
    # Create symbol list
    portfolio_path = os.path.join(addpath.data_path, "bond", "portfolio")
    rebalancing_dates = formation_dates
    bt_symbol_list = []
    for date in rebalancing_dates:
        tmp = pd.read_csv(os.path.join(portfolio_path, date.strftime("%Y-%m-%d") + ".csv"), index_col=0)
        tmp_list = tmp.index.tolist()
        bt_symbol_list = bt_symbol_list + tmp_list
    bt_symbol_list = list(set(bt_symbol_list))


    #################################################
    # create stock_info for backtest ##
    #################################################

    for symbol in bt_symbol_list:
        create_ohlc(symbol)

    # create symbol info for backtest
    symbol_feature_df = pd.DataFrame(bt_symbol_list, columns=['symbol'])
    symbol_feature_df['csv_name']=bt_symbol_list
    symbol_feature_df['instrument_type'] = 'CN_Stock'
    symbol_feature_df['base_ccy'] = base_ccy
    symbol_feature_df['trading_currency'] = base_ccy
    symbol_feature_df.set_index('symbol',inplace=True)
    symbol_feature_df['bbg_code'] = np.nan
    symbol_feature_df['back_up_bbg_code'] = np.nan
    symbol_feature_df['source'] = 'self'
    symbol_feature_df['symboltype'] = 'CN_Stock'
    info_path = os.path.join(addpath.data_path, 'bundles',"bond",'info')
    if os.path.exists(info_path):
        pass
    else:
        os.makedirs(info_path)
    symbol_feature_df.to_csv(os.path.join(info_path,'info.csv'))

    ######################################
    # create strategy_pool for backtest  #
    ######################################

    REB_FREQ = {
        "ANNUALLY": "12M",
        "SEMIANNUALLY": "6M",
        "QUARTERLY": "3M",
        "BIMONTHLY": "2M",
        "MONTHLY": "1M",
        "BIWEEKLY": "2W",
        "WEEKLY": "1W",
        "DAILY": "1D"
    }


    symbol_dict = {
        "STOCKS": bt_symbol_list
    }
    start=formation_dates[0]
    end=formation_dates[len(formation_dates)-1]
    params_dict = {
        "start_year": float(start.year),
        "start_month": float(start.month),
        "start_day": float(start.day),
        "end_year": float(end.year),
        "end_month": float(end.month),
        "end_day": float(end.day),
    }

    import itertools
    all_combinations = list(itertools.product(["algorithm.backtest.trials.bond.bond_backtest_core"],['bond_backtest_core'], [json.dumps(params_dict)], [json.dumps(symbol_dict)]))
    strategy_allocation = pd.DataFrame(all_combinations, columns=["strategy_module", "strategy_name", "params", "symbol"])

    output_path2 = os.path.join(addpath.data_path, 'bundles',"bond")
    if os.path.exists(output_path2):
        pass
    else:
        os.makedirs(output_path2)
    strategy_allocation.to_csv(os.path.join(output_path2, "strategy_allocation.csv"))






    ######################
    # Create config json #
    ######################

    input_json_path = os.path.join(addpath.config_path, "backtest_engine_config_template", "parameters_stock_daily.json")
    output_json_path = os.path.join(output_path2, "parameters.json")

    # json_item = json.load(open(input_json_path))
    # params = json_item.copy()
    params = {}
    params['start_year'] = int(start.year)
    params['start_month'] = int(start.month)
    params['start_day'] = int(start.day)
    params['end_year'] = int(end.year)
    params['end_month'] = int(end.month)
    params['end_day'] = int(end.day)
    params['main_data'] = ["STOCKS"]

    params['data'] = {
        "STOCKS": {
            "DataCenter": "LocalCSV",
            "DataCenterArgs":{
                "main_dir": os.path.join(addpath.data_path, 'bundles',"bond", "daily_data"),
                "fxrate_dir": os.path.join(addpath.data_path, 'bundles',"bond","fxrate"),
                "int_dir": os.path.join(addpath.data_path, 'bundles',"bond", "interest"),
                # "ref_dir":portfolio_path,
                "info": os.path.join(info_path,'info.csv')
            },
            "Fields": "OHLC",
            "Frequency": "DAILY"
        }
    }

    params['account'] = {
        'cash': cash,
        'commission': commission
    }

    params['algo'] = {
        'freq': 'DAILY',
        'window_size': {"main": 10},
        'ref_window_time': 10,
        'strategy_pool_path': os.path.join(output_path2, "strategy_allocation.csv"),
        'base_ccy': "LOCAL"
    }
    params['optimization'] = {
        "numba_parallel": False,
    }
    params['dm_pickle'] = {
      "save_dir": os.path.join(addpath.data_path, 'bundles',"bond",),
      "save_name":'bond_backtest' + "_dm_pickle",
      "to_pkl": True,
      "from_pkl": True
    }

    output_path3 = os.path.join(addpath.data_path, 'bundles',"bond")
    if os.path.exists(output_path3):
        pass
    else:
        os.makedirs(output_path3)

    params['result_output'] = {
        "flatten": False,
        "save_dir": os.path.join(addpath.data_path, 'bundles',"bond",'backtest_result'),
        "save_name": 'backtest_result',
        "save_n_workers": 1,
        "status_dir": os.path.join(addpath.data_path, 'bundles',"bond",'backtest_result'),
        "status_name": 'backtest_result'
    }

    jsondata = json.dumps(params, indent=4, separators=(',', ': '))

    # with open(output_json_path, 'w') as jsonFile:
    #     json.dump(params, jsonFile)
    f = open(output_json_path, 'w')
    f.write(jsondata)
    f.close()
    return output_json_path,strategy_allocation


def create_ohlc(symbol):

    input_path = os.path.join(addpath.data_path, "bond", "trading", symbol + ".csv")
    data_in = pd.read_csv(input_path, parse_dates=[0], index_col=0)

    # data_in=data_in.resample('1D').last().ffill()
    output = data_in.loc[:, ['CLOSE']]
    output['open'] = output['CLOSE']
    output['high'] = output['CLOSE']
    output['low'] = output['CLOSE']
    output['close'] = output['CLOSE']
    output = output.loc[:, ['open', 'high', 'low', 'close']]
    output.index = output.index.map(lambda x: x.strftime('%Y-%m-%d'))
    output.index.name = 'date'
    output_path = os.path.join(addpath.data_path, 'bundles',"bond", "daily_data")
    if os.path.exists(output_path):
        pass
    else:
        os.makedirs(output_path)
    output.to_csv(os.path.join(output_path, symbol + ".csv"))

    interest_data = pd.DataFrame(index=output.index)
    div_path = os.path.join(addpath.data_path, "bond", "dividend", symbol + ".csv")
    dividend_in=pd.read_csv(div_path,parse_dates=[0], index_col=0)
    interest_data['interest_rate'] = -dividend_in['div']
    interest_path= os.path.join(addpath.data_path, 'bundles',"bond", "interest")
    if os.path.exists(interest_path):
        pass
    else:
        os.makedirs(interest_path)
    interest_data.to_csv(os.path.join(interest_path, symbol + ".csv"))

    fxrate_data = pd.DataFrame(index=output.index)
    fxrate_data['fx_rate'] = 1
    fxrate_path= os.path.join(addpath.data_path, 'bundles',"bond", "fxrate")
    if os.path.exists(fxrate_path):
        pass
    else:
        os.makedirs(fxrate_path)
    fxrate_data.to_csv(os.path.join(fxrate_path, symbol + ".csv"))

def result_metrics_calculation(pv):
    data=pv-1.
    RF = 0
    report = pd.DataFrame(None, index=data.columns.values)
    start = data.index[0]
    end = data.index[-1]
    period = (end - start).days
    variables = list(data.columns)
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
            report['Total Return' + str(year)] = (1 + data_tmp.loc[data_tmp.index[-1], variables]) / (
                    1 + data_tmp.loc[data_tmp.index[0], variables]) - 1
            report['Return p.a.' + str(year)] = np.power(report['Total Return' + str(year)] + 1., 365. / period_tmp) - 1
            pv_YTD = data_tmp + 1.
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



def backtest_bond():
    cash = 100000000
    commission=0.003
    base_ccy='CNY'
    formation_dates = pd.read_csv(os.path.join(addpath.config_path, 'formation_date_bond.csv'), parse_dates=[0])[
        'formation_date']
    formation_dates = formation_dates[formation_dates < datetime.today()]
    strategy_config_path,strategy_allocation=create_backtest_engine_files(formation_dates, base_ccy, cash,commission)

    run_once(strategy_config_path, strategy_allocation)
    # inspect results
    reader = ResultReader(join(addpath.data_path, 'bundles',"bond",'backtest_result','backtest_result'))
    reader.to_csv(join(addpath.result_path,'ew_bond','csv_result'))
    strategy_id = list(reader.get_all_sids())
    pnl_results = reader.get_strategy(id_list=strategy_id, fields=['pnl'])

    pnl_list = []
    for key in pnl_results:
        tmp_pnl = pnl_results[key][0]
        tmp_pnl.index = pd.to_datetime(tmp_pnl.index, unit='s')
        tmp_pnl = (tmp_pnl + cash) / cash
        pnl_list.append(tmp_pnl)

    pv = pd.concat(pnl_list, axis=1)
    pv.columns = ['pv']

    report = result_metrics_calculation(pv)

    report.to_csv(join(addpath.result_path,'ew_bond','report.csv'))
    # pv=pv.resample('1D').last().ffill()
    pv.to_csv(join(addpath.result_path,'ew_bond','pv.csv'))
    return pv,report

if __name__ =="__main__":
    backtest_bond()
