import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from algorithm import addpath
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator

def create_backtest_engine_files(stocks, strategy_name, strategy_config_path, result_path, start_dt, end_dt, cash,
                                 commission_rate, window_size, save_dm_to_local, use_local_dm):

    # Create config json
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
                "main_dir": os.path.join(addpath.data_path, "backtest_data", "bundles"),
                "fxrate_dir": os.path.join(addpath.data_path, "backtest_data", "fxrate"),
                "int_dir": os.path.join(addpath.data_path, "backtest_data", "interest"),
                "info": os.path.join(addpath.data_path, "backtest_data", "info", "info.csv")
            },
            "Fields": ["OHLC", "volume"],
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


    params['numba'] = {
        "parallel": False
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
