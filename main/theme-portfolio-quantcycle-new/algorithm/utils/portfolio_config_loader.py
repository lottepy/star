# Load theme portfolio config files

import os
import json
import pandas as pd
from datetime import datetime, timedelta
from algorithm import addpath
from constants import *

def portfolio_config_loader(portfolio_name):

    input_path = os.path.join(addpath.config_path, portfolio_name)
    json_file_path = os.path.join(input_path, "strategy_config.json")

    json_item = json.load(open(json_file_path))
    params = json_item.copy()

    start = datetime(params['start_year'], params['start_month'], params['start_day'])
    end = datetime(params['end_year'], params['end_month'], params['end_day'])

    cash = params['account'].get("cash", CASH)
    commission = params['account'].get("commission", COMMISSION)

    lookback_days = params['algo'].get('save_data_time', LOOKBACK_DAYS)
    rebalance_freq = params['algo'].get('rebalance_freq', REBALANCE_FREQ)
    base_ccy = params['algo'].get("base_ccy", "NA")
    underlying_type = params['algo'].get("underlying_type", "")
    number_assets = params['algo'].get("number_assets", NO_ASSETS)
    weighting_approach = params['algo'].get("weighting_approach", "EQUALLY")
    market_cap_threshold = params['algo'].get("market_cap_threshold", "MARKET_CAP_THRESHOLD")
    upper_bound = params['algo'].get("upper_bound", "UPPER_BOUND")
    lower_bound = params['algo'].get("lower_bound", "LOWERBOUND")

    if underlying_type == "US_STOCK":
        symbol_path = os.path.join(addpath.data_path, "us_data")
    elif underlying_type == "HK_STOCK":
        symbol_path = os.path.join(addpath.data_path, "hk_data")
    elif underlying_type == "CN_STOCK":
        symbol_path = os.path.join(addpath.data_path, "cn_data")
    symbol_list_path = os.path.join(symbol_path, "symbol_list.csv")
    symbol_industry=pd.read_csv(symbol_list_path).set_index('symbol')
    symbol_list = pd.read_csv(symbol_list_path)['symbol'].tolist()

    return symbol_list,symbol_industry, start, end, cash, commission, lookback_days, rebalance_freq, base_ccy, \
           underlying_type, number_assets, weighting_approach, market_cap_threshold, upper_bound, lower_bound
