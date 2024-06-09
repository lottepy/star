import pandas as pd
import numpy as np
from datetime import datetime
import multiprocessing
from os.path import dirname, abspath, join, exists
from os import makedirs, listdir
from algorithm import addpath
from algorithm.utils.backtest_engine_config_generator_benchmark import create_backtest_engine_files
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader
import itertools


def benchmark_generator(cash, start_dt, end_dt):
    strategy_name = 'benchmark'
    etf_name = ['LG13TRUU Index', 'LGTRTRUU Index', 'MXWD Index']
    window_size = 1
    
    commission_rate = 0
    
    for ratio in [0.07, 0.13, 0.17, 0.23, 0.27, 0.33, 0.45, 0.55, 0.65, 0.75]:
    # for ratio in [0, 0.2, 0.4, 0.6]:
        save_dm_to_local = True if ratio == 0.07 else False
        use_local_dm = False if ratio == 0.07 else True
        # Specify directories
        strategy_path = dirname(abspath(__file__))
        root_path = addpath.root_path
        relative_path = strategy_path[len(root_path) + 1:].replace("\\", ".")
        strategy_config_path = join(strategy_path, "config", strategy_name + "_config.json")
        strategy_module = relative_path + ".algo." + strategy_name + "_core"
        result_path = join(addpath.result_path, strategy_name, str(ratio))
        csv_result_path = join(result_path, "csv_result")

        create_backtest_engine_files(etf_name, strategy_name, strategy_config_path, result_path, start_dt, end_dt, cash,
                                        commission_rate, window_size, save_dm_to_local, use_local_dm)

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
                'ratio': [ratio]
            }}

        # run backtesting
        run_once(strategy_config_path, strategy_pool_generator(pool_setting, save=False))

        # inspect results
        reader = ResultReader(join(result_path, strategy_name))
        reader.to_csv(csv_result_path)
        
        sid = list(reader.get_all_sids())
        pnl_results = reader.get_strategy(id_list=sid, fields=['pnl'])[0][0]
        
        pv = (pnl_results + cash)/cash
        pv.index = pd.to_datetime(pv.index, unit='s')
        pv.to_csv(join(result_path, 'pv.csv'))
    
    


if __name__ == "__main__":
    cash = 10000000000000
    
    start_dt = pd.to_datetime('2016-02-29')
    end_dt = pd.to_datetime('2021-01-08')
    
    benchmark_generator(cash, start_dt, end_dt)
