import os
import sys

sys.path.append(os.path.abspath(os.curdir))

import pandas as pd
import numpy as np
from datetime import datetime
from os.path import dirname, abspath, join, exists
from os import makedirs, listdir
from algorithm import addpath
from algorithm.utils.backtest_engine_config_generator import create_backtest_engine_files
from algorithm.utils.result_analysis import result_metrics_calculation
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from algorithm.utils.convert import parse_localtime
from quantcycle.app.result_exporter.result_reader import ResultReader
import itertools


def ashare_test_main(strategy_name, start_str, end_str, cash, commission_rate, freq, factor_rotation, industry_level):
    result_path = join(addpath.result_path, strategy_name, freq, str(factor_rotation.real), industry_level)
    window_size = 52

    # Specify directories
    strategy_path = dirname(abspath(__file__))
    root_path = addpath.root_path
    relative_path = strategy_path[len(root_path) + 1:].replace("\\", ".")
    strategy_config_path = join(strategy_path, "config")
    strategy_module = relative_path + ".core_algo.algo_core"
    csv_result_path = join(result_path, "csv_result")

    if exists(strategy_config_path):
        pass
    else:
        makedirs(strategy_config_path)

    if exists(result_path):
        pass
    else:
        makedirs(result_path)

    if exists(csv_result_path):
        pass
    else:
        makedirs(csv_result_path)

    # create config json
    start_dt = parse_localtime(start_str, 'WW')
    end_dt = parse_localtime(end_str, 'WW')
    strategy_config_path = join(strategy_path, "config.json")

    freq_n = 3.0
    if freq == "semiyear":
        freq_n = 6.0
    elif freq == "qtr":
        freq_n = 3.0

    if industry_level == "SW1":
        industry_level_n = 1.0
    elif industry_level == "SW2":
        industry_level_n = 2.0
    else:
        industry_level_n = 3.0

    if factor_rotation == True:
        factor_rotation_n = 1.0
        selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks", industry_level, freq)
    else:
        factor_rotation_n = 0.0
        selected_stock_path = os.path.join(addpath.data_path, "Ashare", "selected_stocks")

    full_etf_name = []

    portfolio_list = os.listdir(selected_stock_path)
    for portfolio in portfolio_list:
        selected = pd.read_csv(os.path.join(selected_stock_path, portfolio))
        full_etf_name = full_etf_name + selected['Stkcd'].tolist()
    full_etf_name = list(set(full_etf_name))

    save_dm_to_local = False
    use_local_dm = True
    parallel = False
    create_backtest_engine_files(strategy_name, strategy_config_path, result_path, start_dt, end_dt,
                                 cash, commission_rate, window_size, save_dm_to_local, use_local_dm, parallel)

    # create strategy_pool
    pool_setting = {"symbol": {
        "STOCKS": full_etf_name},
        "strategy_module": strategy_module,
        "strategy_name": strategy_name,
        "params": {
            'freq_n': [freq_n],
            'factor_rotation_n': [factor_rotation_n],
            'industry_level_n': [industry_level_n]
        }}

    # run backtesting
    run_once(strategy_config_path, strategy_pool_generator(pool_setting, save=False))

    reader = ResultReader(join(result_path, strategy_name))
    reader.to_csv(csv_result_path)

    strategy_id = list(reader.get_all_sids())
    pnl_results = reader.get_strategy(id_list=strategy_id, fields=['pnl'])

    pnl_list = []
    for key in pnl_results:
        tmp_pnl = pnl_results[key][0]
        tmp_pnl.index = pd.to_datetime(tmp_pnl.index, unit='s')
        tmp_pnl = (tmp_pnl + cash) / cash
        pnl_list.append(tmp_pnl)

    pv = pd.concat(pnl_list, axis=1)

    report = result_metrics_calculation(pv)

    pv.to_csv(join(result_path, 'portfolio value.csv'))
    report.to_csv(join(result_path, 'performance metrics.csv'))


if __name__ == "__main__":
    ts0 = datetime.now()
    cash = 10000000
    start_str = '2013-04-30'
    end_str = '2020-11-30'
    commission_rate = 0.0015
    # strategy_name = "ew_ashare"
    # strategy_name = "score_wt_ashare"
    strategy_name = "score_wt_timed_ashare"
    # strategy_name = "qtr_sw_capped_timed_ashare"
    # freq = 'FF'
    # freq = 'semiyear'
    freq = 'qtr'
    factor_rotation = True
    industry_level = "SW2"
    ashare_test_main(strategy_name, start_str, end_str, cash, commission_rate, freq, factor_rotation, industry_level)
    te0 = datetime.now()
    print("任务总用时", te0 - ts0)
