import os
import sys
sys.path.append(os.path.abspath(os.curdir))

import pandas as pd
from datetime import datetime
from os.path import dirname, abspath, join, exists
from os import makedirs, listdir
from algorithm import addpath
from utils.backtest_engine_config_generator import create_backtest_engine_files
from utils.result_analysis import result_metrics_calculation
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from utils.convert import parse_localtime
from quantcycle.app.result_exporter.result_reader import ResultReader


def xray_backtest(strategy_name, start_str, end_str, cash, commission_rate, sector_preference, invest_length, rat):
    portfolio_path = os.path.join(addpath.data_path, "pool", "GZNSH")

    portfolio_files = os.listdir(portfolio_path)
    symbol_list = []
    for portfolio_file in portfolio_files:
        tmp = pd.read_csv(os.path.join(portfolio_path, portfolio_file))
        tmp_list = tmp['ms_secid'].tolist()
        symbol_list = symbol_list + tmp_list
    symbol_list = list(set(symbol_list))
    symbol_list = [s + ".CN" for s in symbol_list]

    # Specify directories
    strategy_path = dirname(abspath(__file__))
    root_path = addpath.root_path
    relative_path = strategy_path[len(root_path) + 1:].replace("\\", ".")
    strategy_config_folder_path = join(strategy_path, "config")
    strategy_config_path = join(strategy_config_folder_path, "config.json")
    strategy_module = relative_path + ".core_algos." + strategy_name + "_core"

    info = str(rat) + '-A' + str(sector_preference) + '-B' + str(invest_length)

    result_path = join(addpath.result_path, strategy_name, info)
    csv_result_path = join(result_path, "csv_result")

    if exists(strategy_config_folder_path):
        pass
    else:
        makedirs(strategy_config_folder_path)

    if exists(result_path):
        pass
    else:
        makedirs(result_path)

    if exists(csv_result_path):
        pass
    else:
        makedirs(csv_result_path)

    save_dm_to_local = True
    use_local_dm = False
    if exists('dm_to_local.log'):
        # Read dm local log
        with open("dm_to_local.log", "r") as f:
            lines = f.readlines()

        # Read the log line by line
        for line in lines:
            log_backtesting_time = datetime.strptime(line.split(':')[0], "%m/%d/%Y %H-%M-%S")
            log_strategy_name = line.split('*')[1]
            log_backtesting_start_date = line.split('*')[3]
            log_backtesting_end_date = line.split('*')[-4]
            log_stocks = line.split('*')[-2]

            # If we have already backtesting the same strategy with same stocks and same backtesting period, use local dm
            if log_strategy_name == strategy_name and log_backtesting_start_date <= start_str \
                    and log_backtesting_end_date >= end_str and log_stocks == ','.join(symbol_list):
                save_dm_to_local = False
                use_local_dm = True
                break

        # limit the number of line of the log to 1000
        if len(lines) > 1000:
            with open("dm_to_local.log", "w") as f:
                f.write('\n'.join(lines))

                # create config json
    start_dt = parse_localtime(start_str, 'WW')
    end_dt = parse_localtime(end_str, 'WW')
    window_size = 400

    # save_dm_to_local = False
    # use_local_dm = True

    create_backtest_engine_files(symbol_list, strategy_name, strategy_config_path, result_path, start_dt, end_dt, cash,
                                 commission_rate, window_size, save_dm_to_local, use_local_dm)

    # Log backtesting parameters
    with open("dm_to_local.log", "a") as f:
        f.write("{}:*{}* Backtesting from *{}* to *{}*, stocks *{}*\n".format(datetime.now().strftime("%m/%d/%Y %H-%M-%S"),
                                                                          strategy_name, start_str, end_str, ','.join(symbol_list)))

    # encode model parameters
    start_year = float(start_dt.year)
    start_month = float(start_dt.month)
    start_day = float(start_dt.day)
    end_year = float(end_dt.year)
    end_month = float(end_dt.month)
    end_day = float(end_dt.day)

    # create strategy_pool
    pool_setting = {"symbol": {
        "STOCKS": symbol_list},
        "strategy_module": strategy_module,
        "strategy_name": strategy_name,
        "params": {
            'start_year': [start_year],
            'start_month': [start_month],
            'start_day': [start_day],
            'end_year': [end_year],
            'end_month': [end_month],
            'end_day': [end_day],
            'sector_preference': [sector_preference],
            'invest_length': [invest_length],
            'risk_ratio': [rat]
        }}

    # run backtesting
    run_once(strategy_config_path, strategy_pool_generator(pool_setting, save=False))

    # inspect results
    reader = ResultReader(join(result_path, strategy_name))
    reader.to_csv(csv_result_path)

    pnl = pd.read_csv(join(csv_result_path, "0", "pnl.csv"), parse_dates=[1], index_col=1)
    pnl['pv'] = (pnl['pnl'] + cash) / cash
    pnl = pnl[['pv']]
    pnl.columns = [strategy_name]

    report = result_metrics_calculation(pnl)
    pnl.to_csv(join(result_path, 'portfolio value.csv'))
    report.to_csv(join(result_path, 'performance metrics.csv'))


if __name__ == "__main__":
    strategy_name = "smartxray_gznsh"
    cash = 1000000
    start_str = '2014-04-30'
    end_str = '2020-12-31'
    commission_rate = 0.0000

    sector_preference_list = [1, 2, 3, 4, 5]
    invest_length_list = [1, 2, 3, 4, 5]
    risk_ratio_list = [20, 40, 50, 60, 70]

    save_dm_to_local = False
    use_local_dm = True
    for risk_ratio in risk_ratio_list:
        for invest_length in invest_length_list:
            for sector_preference in sector_preference_list:
                xray_backtest(strategy_name, start_str, end_str, cash, commission_rate, sector_preference, invest_length, risk_ratio)

