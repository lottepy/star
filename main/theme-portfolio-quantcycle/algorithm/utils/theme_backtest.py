import os
import pandas as pd
from datetime import datetime, timedelta
from constants import *
import warnings
from algorithm.utils.backtest_engine_config_generator import create_backtest_engine_files
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader
from os.path import dirname, abspath, join, exists
import statsmodels.api as sm
import matplotlib.pyplot as plt
from algorithm.utils.result_analysis import result_metrics_calculation
from algorithm.utils.cul_turnover import cul_turnover

warnings.filterwarnings("ignore")


def theme_backtest(portfolio_name,task):
    rebalance_freq='QUARTERLY'
    underlying_type=PARAMETER_ALGO[portfolio_name]['underlying_type']
    base_ccy=PARAMETER_ALGO[portfolio_name]['base_ccy']
    cash=PARAMETER_ALGO[portfolio_name]['ini_cash']
    commission=0.003
    start=datetime.strptime(BACKTEST_SE[underlying_type]['start'],'%Y-%m-%d')
    end =datetime.strptime(BACKTEST_SE[underlying_type]['end'],'%Y-%m-%d')

    strategy_config_path,strategy_allocation=create_backtest_engine_files(portfolio_name, start, end, rebalance_freq, underlying_type, base_ccy, cash,
                                 commission,task)

    run_once(strategy_config_path, strategy_allocation)
    # inspect results
    reader = ResultReader(join(addpath.result_path, task,portfolio_name,portfolio_name))
    reader.to_csv(join(addpath.result_path,task,portfolio_name))
    strategy_id = list(reader.get_all_sids())
    pnl_results = reader.get_strategy(id_list=strategy_id, fields=['pnl'])
    pnl_list = []
    for key in pnl_results:
        tmp_pnl = pnl_results[key][0]
        tmp_pnl.index = pd.to_datetime(tmp_pnl.index, unit='s')
        tmp_pnl = (tmp_pnl + cash) / cash
        pnl_list.append(tmp_pnl)

    pv = pd.concat(pnl_list, axis=1)
    pv.columns = [portfolio_name]
    pv=pv/pv.iloc[0,:]
    if underlying_type == "US_STOCK":
        input_path = os.path.join(addpath.data_path, "us_data")
    elif underlying_type == "HK_STOCK":
        input_path = os.path.join(addpath.data_path, "hk_data")
    elif underlying_type == "CN_STOCK":
        input_path = os.path.join(addpath.data_path, "cn_data")

    if portfolio_name in ['US_Tech_B','US_Tech_S','US_5G']:
        reference_path = os.path.join(input_path, "reference", 'market_index_nasdaq.csv')
    else:
        reference_path=os.path.join(input_path,"reference",'market_index.csv')
    reference = pd.read_csv(reference_path,index_col=0,parse_dates=[0])
    reference=reference.resample('1D').last().ffill()
    if pv.index[-1]>reference.index[-1]:
        pv=pv.loc[:reference.index[-1],:]
    else:
        reference = reference.loc[:pv.index[-1], :]
    reference = reference.loc[pv.index, :]
    reference = reference / reference.iloc[0, 0]
    x = (reference - reference.shift()) / reference.shift()
    y=(pv-pv.shift())/pv.shift()
    x=x.dropna()
    y=y.dropna()
    x=sm.add_constant(x)
    model = sm.OLS(y, x).fit()
    print(model.summary())
    data_plt = pd.concat([pv, reference], axis=1)
    data_plt.plot()
    plt.grid()
    # plt.plot(data_plt.values)
    plt.legend(loc='best')
    plt.show()

    data_plt.to_csv(join(addpath.result_path,task,portfolio_name, 'portfolio value.csv'))
    report = result_metrics_calculation(pv)
    report_ref = result_metrics_calculation(reference)
    report_out=pd.concat([report,report_ref],axis=0)
    report_out.to_csv(join(addpath.result_path,task,portfolio_name, 'performance metrics.csv'))
    cul_turnover(rebalance_freq,start, end,portfolio_name,task,pv,cash)
    return data_plt, report
