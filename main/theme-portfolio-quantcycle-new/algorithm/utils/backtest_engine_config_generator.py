import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from constants import *
from algorithm.utils.helpers import cal_rebalancing_dates
from algorithm import addpath

def create_backtest_engine_files(portfolio_name, start, end, rebalance_freq, underlying_type, base_ccy, cash, commission,task):
    # Create symbol list
    if task=='backtesting':
        root_path=os.path.join(addpath.data_path, "strategy_temps", portfolio_name)
        portfolio_path = os.path.join(root_path, "portfolios")
        rebalancing_dates = cal_rebalancing_dates(start, end, rebalance_freq)
    else:
        root_path = os.path.join(addpath.data_path, "tracking_weight", portfolio_name)
        portfolio_path = os.path.join(root_path, "portfolios")
        file_names = os.listdir(portfolio_path)
        if '.DS_Store' in file_names:
            file_names.remove('.DS_Store')
        rebalancing_dates = [datetime.strptime(file_name[:-4], '%Y-%m-%d') for file_name in file_names]
        start=min(rebalancing_dates)
        end=datetime.today()
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
        create_ohlc(symbol, underlying_type,portfolio_name,root_path)

    # bt_symbol_list_us=[]
    # for symbol in bt_symbol_list:
    #     bt_symbol_list_us.append(symbol+'.US')

    # create symbol info for backtest
    symbol_feature_df = pd.DataFrame(bt_symbol_list, columns=['symbol'])
    symbol_feature_df['csv_name']=bt_symbol_list
    symbol_feature_df['instrument_type'] = underlying_type
    symbol_feature_df['base_ccy'] = base_ccy
    symbol_feature_df['trading_currency'] = base_ccy
    symbol_feature_df.set_index('symbol',inplace=True)
    if underlying_type=='CN_STOCK':
        symbol_lot=pd.read_csv(os.path.join(addpath.data_path,'cn_data','symbol_list.csv'),index_col=0)
    elif underlying_type=='US_STOCK':
        symbol_lot = pd.read_csv(os.path.join(addpath.data_path, 'us_data', 'symbol_list.csv'), index_col=0)
    else:
        symbol_lot = pd.read_csv(os.path.join(addpath.data_path, 'hk_data', 'symbol_list.csv'), index_col=0)
    symbol_feature_df['lot_size']=symbol_lot.loc[bt_symbol_list,['LOTSIZE']]
    symbol_feature_df['bbg_code'] = np.nan
    symbol_feature_df['back_up_bbg_code'] = np.nan
    # symbol_feature_df['source'] = 'self'
    symbol_feature_df['symboltype'] = underlying_type
    info_path = os.path.join(root_path,'info')
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

    params_dict = {
        "rebalance_freq": float(list(REB_FREQ.keys()).index(rebalance_freq)),
        "start_year": float(start.year),
        "start_month": float(start.month),
        "start_day": float(start.day),
        "end_year": float(end.year),
        "end_month": float(end.month),
        "end_day": float(end.day),
        "strategy_code":float(list(STRATEGY_CODE.keys()).index(portfolio_name)),
        "task_code":float(0) if task=="backtesting" else float(1)
    }

    import itertools
    all_combinations = list(itertools.product(["algorithm.backtest_core_algo.core_algo"],['aqumon_theme'], [json.dumps(params_dict)], [json.dumps(symbol_dict)]))
    strategy_allocation = pd.DataFrame(all_combinations, columns=["strategy_module", "strategy_name", "params", "symbol"])

    output_path2 = os.path.join(root_path)
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
                "main_dir": os.path.join(root_path, "daily_data"),
                "fxrate_dir": os.path.join(root_path, "fxrate"),
                "int_dir": os.path.join(root_path, "interest"),
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
      "save_dir": "results/cache/",
      "save_name": portfolio_name + "_dm_pickle",
      "to_pkl": True,
      "from_pkl": False
    }

    output_path3 = os.path.join(addpath.result_path,task, portfolio_name)
    if os.path.exists(output_path3):
        pass
    else:
        os.makedirs(output_path3)

    params['result_output'] = {
        "flatten": False,
        "save_dir": output_path3,
        "save_name": portfolio_name,
        "save_n_workers": 1,
        "status_dir": output_path3,
        "status_name": portfolio_name
    }

    jsondata = json.dumps(params, indent=4, separators=(',', ': '))

    # with open(output_json_path, 'w') as jsonFile:
    #     json.dump(params, jsonFile)
    f = open(output_json_path, 'w')
    f.write(jsondata)
    f.close()
    return output_json_path,strategy_allocation


def create_ohlc(symbol, underlying_type,portfolio_name,root_path):
    if underlying_type == "US_STOCK":
        input_path = os.path.join(addpath.data_path, "us_data", "trading", symbol + ".csv")
    elif underlying_type == "HK_STOCK":
        input_path = os.path.join(addpath.data_path, "hk_data", "trading", symbol + ".csv")

    elif underlying_type == "CN_STOCK":
        input_path = os.path.join(addpath.data_path, "cn_data", "trading", symbol + ".csv")
    data_in = pd.read_csv(input_path, parse_dates=[0], index_col=0)

    output = data_in.loc[:, ['PX_LAST']]
    output['open'] = output['PX_LAST']
    output['high'] = output['PX_LAST']
    output['low'] = output['PX_LAST']
    output['close'] = output['PX_LAST']
    output['volume'] = None
    output = output.loc[:, ['open', 'high', 'low', 'close', 'volume']]
    output.index = output.index.map(lambda x: x.strftime('%Y-%m-%d'))
    output.index.name = 'date'
    output_path = os.path.join(root_path, "daily_data")
    if os.path.exists(output_path):
        pass
    else:
        os.makedirs(output_path)
    output.to_csv(os.path.join(output_path, symbol + ".csv"))

    interest_data = pd.DataFrame(index=output.index)
    interest_data['interest_rate'] = 0
    interest_path= os.path.join(root_path, "interest")
    if os.path.exists(interest_path):
        pass
    else:
        os.makedirs(interest_path)
    interest_data.to_csv(os.path.join(interest_path, symbol + ".csv"))

    fxrate_data = pd.DataFrame(index=output.index)
    fxrate_data['fx_rate'] = 1
    fxrate_path= os.path.join(root_path, "fxrate")
    if os.path.exists(fxrate_path):
        pass
    else:
        os.makedirs(fxrate_path)
    fxrate_data.to_csv(os.path.join(fxrate_path, symbol + ".csv"))
    # print(fxrate_data.index[-1])




