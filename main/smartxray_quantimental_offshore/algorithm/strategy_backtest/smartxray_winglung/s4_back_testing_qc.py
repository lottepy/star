import pandas as pd
import numpy as np
import datetime
import multiprocessing
from os.path import dirname, abspath, join, exists
from os import makedirs, listdir
from tabulate import tabulate
from algorithm import addpath
from algorithm.utils.backtest_engine_config_generator_backtest import create_backtest_engine_files
from algorithm.utils.result_analysis import result_metrics_calculation
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader
import itertools
import requests
import json


    
def index_backtesting(start,end,equity_pct,sector_preference,region_preference,commission_fee,initial_capital,rebalance_freq,model_freq,
                      save_dm_to_local, use_local_dm):
    
    info = str(round(equity_pct, 2)) + '_' + sector_preference + '_' + region_preference
    
    strategy_name = 'smartxray_wl'
    pool_file_list = listdir(join(addpath.data_path, 'pool', '12M'))
    pool = [pd.read_csv(join(addpath.data_path, 'pool', '12M', ele))['ms_secid'] for ele in pool_file_list]
    etf_name = pd.concat(pool).unique().tolist()
    
    window_size = 1
    
    ratio = equity_pct
    
    start_dt = pd.to_datetime(start)
    end_dt = pd.to_datetime(end)
    
    # Make Trading Calendar for backtesting
    r = requests.get(
        'https://algo-internal.aqumon.com/datamaster/exchange_calendar/?start_date='+'2000-01-01'+'&end_date='+'2022-12-31'+'&ex='+'HK')
    user_info = r.content.decode()
    user_dict = json.loads(user_info)

    calendar = pd.DataFrame(user_dict)
    calendar = calendar.sort_values(by='data')['data']
    calendar = calendar.rename('date')
    
    calendar.to_csv( join(addpath.data_path, 'quantcycle_backtest', 'calendar', 'calendar.csv') )

    # Specify directories
    strategy_path = dirname(abspath(__file__))
    root_path = addpath.root_path
    relative_path = strategy_path[len(root_path) + 1:].replace("\\", ".")
    strategy_config_path = join(strategy_path, "config", strategy_name + "_config.json")
    strategy_module = relative_path + ".algo." + strategy_name + "_core"
    result_path = join(addpath.result_path, strategy_name, info)
    csv_result_path = join(result_path, "csv_result")

    create_backtest_engine_files(etf_name, strategy_name, strategy_config_path, result_path, start_dt, end_dt, initial_capital,
                                    commission_fee, window_size, save_dm_to_local, use_local_dm)

    start_year = float(start_dt.year)
    start_month = float(start_dt.month)
    start_day = float(start_dt.day)
    end_year = float(end_dt.year)
    end_month = float(end_dt.month)
    end_day = float(end_dt.day)

    # create strategy_pool
    pool_setting = {"symbol": {
        "STOCKS": etf_name},
        "strategy_module": strategy_module,
        "strategy_name": strategy_name,
        "params": {
            'ratio': [ratio],
            'start_year': [start_year],
            'start_month': [start_month],
            'start_day': [start_day],
            'end_year': [end_year],
            'end_month': [end_month],
            'end_day': [end_day],
            'sector_preference' : [int(sector_preference[-1])],
            'region_preference' : [int(region_preference[-1])]
        }}

    # run backtesting
    run_once(strategy_config_path, strategy_pool_generator(pool_setting, save=False))

    # inspect results
    reader = ResultReader(join(result_path, strategy_name))
    reader.to_csv(csv_result_path)
    
    sid = list(reader.get_all_sids())
    pnl_results = reader.get_strategy(id_list=sid, fields=['pnl'])[0][0]
    
    pv = (pnl_results + initial_capital)/initial_capital
    pv.index = pd.to_datetime(pv.index, unit='s')
    pv.to_csv(join(result_path, 'pv.csv'))
    
    report = result_metrics_calculation(pv)
    
    financial_indicator = [
    ["Portfolio annualized return", report['Return p.a.'].iloc[0]],
    ["Portfolio cumulative return", report['Total Return'].iloc[0]],
    ["Sharpe ratio", report['Sharpe Ratio'].iloc[0]],
    ["Max Draw Down", report['Max Drawdown'].iloc[0]],
    ["Volatility", report['Volatility'].iloc[0]],
    ]
    
    print(tabulate(financial_indicator, headers=["Indicator", "Value"], tablefmt="orgtbl"))
    
    pv['algorithm_period_return'] = pv['pnl']
    
    return pv


if __name__ == "__main__":
    
    initial_capital = 1000000
    
    sector_preference = 'A1'
    region_preference = 'B1'
    save_dm_to_local = False
    use_local_dm = True
    start = '2016-02-29'
    end = '2020-12-31'
    commission_fee = 0
    rebalance_freq = '6M'
    model_freq = '12M'
    
    pv_list = []
    for equity_pct in [0.07, 0.17, 0.27, 0.45, 0.65]:
        pv = index_backtesting(start,end,equity_pct,sector_preference,region_preference,commission_fee,initial_capital,rebalance_freq,model_freq,
                               save_dm_to_local, use_local_dm)