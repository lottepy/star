import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from algorithm import addpath
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator

def create_backtest_engine_files(strategy_name, strategy_config_path, result_path, start_dt, end_dt, cash,
                                 commission_rate, window_size, save_dm_to_local, use_local_dm, parallel):

    # Create config json
    # parameter

    params = {}
    params['start_year'] = int(start_dt.year)
    params['start_month'] = int(start_dt.month)
    params['start_day'] = int(start_dt.day)
    params['end_year'] = int(end_dt.year)
    params['end_month'] = int(end_dt.month)
    params['end_day'] = int(end_dt.day)
    params['main_data'] = ["STOCKS"]

    params['data'] = {
        "STOCKS": {
            "DataCenter": "LocalCSV",
            "DataCenterArgs":{
                "main_dir": os.path.join(addpath.data_path, "Backtest", "bundles"),
                "fxrate_dir": os.path.join(addpath.data_path, "Backtest", "fxrate"),
                "int_dir": os.path.join(addpath.data_path, "Backtest", "interest"),
                "info": os.path.join(addpath.data_path, "Backtest", "info", "info.csv")
            },
            "Fields": "OHLC",
            "Frequency": "DAILY"
        }
    }

    params['account'] = {
        'cash': cash,
        'commission': commission_rate
    }

    params['algo'] = {
        'window_size': {
            "main": window_size
        },
        'base_ccy': "LOCAL"
    }

    params['result_output'] = {
        "flatten": False,
        "save_dir": result_path,
        "save_name": strategy_name,
        "save_n_workers": 1,
        "status_dir": result_path,
        "status_name": strategy_name
    }

    if parallel:
        params['optimization'] = {
            "numba_parallel": False,
            "python_parallel_n_workers": parallel
        }
    else:
        params['optimization'] = {
            "numba_parallel": False,
        }

    params['dm_pickle'] = {
      "save_dir": "results/cache/",
      "save_name": strategy_name + "_dm_pickle",
      "to_pkl": True if save_dm_to_local else False,
      "from_pkl": True if use_local_dm else False
    }


    jsondata = json.dumps(params, indent=4, separators=(',', ': '))

    f = open(strategy_config_path, 'w')
    f.write(jsondata)
    f.close()


def create_ohlc(symbol, underlying_type):
    if underlying_type == "US_STOCK":
        input_path = os.path.join(addpath.data_path, "us_data", "trading", symbol + ".csv")
    elif underlying_type == "HK_STOCK":
        input_path = os.path.join(addpath.data_path, "hk_data", "trading", symbol + ".csv")
    elif underlying_type == "CN_STOCK":
        input_path = os.path.join(addpath.data_path, "cn_data", "trading", symbol + ".csv")
    data_in = pd.read_csv(input_path, parse_dates=[0], index_col=0)
    output = data_in.loc[:, ['PX_LAST', 'PX_VOLUME']]
    output['open'] = output['PX_LAST']
    output['high'] = output['PX_LAST']
    output['low'] = output['PX_LAST']
    output['close'] = output['PX_LAST']
    output['volume'] = output['PX_VOLUME']
    output = output.loc[:, ['open', 'high', 'low', 'close', 'volume']]
    output.index = output.index.map(lambda x: x.strftime('%Y-%m-%d'))
    output.index.name = 'date'
    output_path = os.path.join(addpath.data_path, "backtest_temps", "daily_data")
    if os.path.exists(output_path):
        pass
    else:
        os.makedirs(output_path)
    output.to_csv(os.path.join(output_path, symbol + ".csv"))

