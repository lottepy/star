import pandas as pd
import numpy as np
import datetime

from os.path import dirname, abspath, join, exists
from os import makedirs, listdir

from algorithm import addpath
from algorithm.utils.backtest_engine_config_generator_fake_strategy import create_backtest_engine_files

from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader


def run_fake_strategy(start_dt, end_dt, real_strategy_name):
    print('*' * 60)
    print('Running fake strategy to fetch data..')
    print('*' * 60)
    
    strategy_name = 'fake_strategy'
    
    pool_file_list = listdir(join(addpath.data_path, 'pool', '12M'))
    pool = [pd.read_csv(join(addpath.data_path, 'pool', '12M', ele))['ms_secid'] for ele in pool_file_list]
    etf_name = pd.concat(pool).unique().tolist()
    
    strategy_path = dirname(abspath(__file__))
    strategy_config_path = join(strategy_path, "config", "fake_strategy_config.json")
    result_path = join(addpath.result_path, 'fake_strategy')
    
    initial_capital = 100
    commission_fee = 0
    window_size = 1
    save_dm_to_local = True
    use_local_dm = True
    
    create_backtest_engine_files(etf_name, strategy_name, strategy_config_path, result_path, start_dt, end_dt, initial_capital,
                                    commission_fee, window_size, save_dm_to_local, use_local_dm, real_strategy_name)
    
    strategy_module = 'algorithm.utils.fake_strategy_core'
    
    # create strategy_pool
    pool_setting = {"symbol": {
        "STOCKS": etf_name},
        "strategy_module": strategy_module,
        "strategy_name": strategy_name,
        "params": {}}

    # run backtesting
    run_once(strategy_config_path, strategy_pool_generator(pool_setting, save=False))
    
    print('*' * 60)
    print('Finish running fake strategy..')
    print('*' * 60)