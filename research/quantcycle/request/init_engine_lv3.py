import datetime
import json
import math
import os
import uuid

import numpy as np
import pandas as pd
import requests
from pandas.tseries.offsets import BDay


req_url = "http://172.29.39.140:1234/api/strategy_subscription"
#req_url = "http://192.168.8.130:1234/api/strategy_subscription"


if __name__ == "__main__":
    
    config_dict = {}
    config_dict["start_year"] = 2019
    config_dict["start_month"] = 9
    config_dict["start_day"] = 10
    config_dict["end_year"] = 2020
    config_dict["end_month"] = 9
    config_dict["end_day"] = 21

    config_json_rsi_level1 = json.load(open(r'strategy/FX/technical_indicator/oscillator/RSI/config/RSI_lv1.json'))
    config_json_rsi_level1.update(config_dict)
    config_json_rsi_level1["data"]["FX"]["SymbolArgs"]["DataSource"] = "BT150"
    config_json_rsi_level1["data"]["FX"]["SymbolArgs"]["BackupDataSource"] = "BT150"
    request_id = uuid.uuid1().hex

    params = { 'config_json': json.dumps(config_json_rsi_level1) , "strategy_df":(pd.read_csv(r"strategy\FX\technical_indicator\oscillator\RSI\strategy_pool\RSI_lv1_strategy_pool.csv")).to_json()}
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, data = params)
    print(res.text) 

    request_id = uuid.uuid1().hex
    config_json_rsi_level2 = json.load(open(r'strategy/FX/technical_indicator/oscillator/RSI/config/RSI_lv2.json'))
    config_json_rsi_level2.update(config_dict)
    params = { 'config_json': json.dumps(config_json_rsi_level2) , "strategy_df":(pd.read_csv(r"strategy\FX\technical_indicator\oscillator\RSI\strategy_pool\RSI_lv2_strategy_pool.csv")).to_json()}
    config_json_rsi_level2["data"]["FX"]["SymbolArgs"]["DataSource"] = "BT150"
    config_json_rsi_level1["data"]["FX"]["SymbolArgs"]["BackupDataSource"] = "BT150"
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, data = params)
    print(res.text) 

    config_json_kd_level1 = json.load(open(r'strategy/FX/technical_indicator/oscillator/KD/config/KD_lv1.json'))
    config_json_kd_level1.update(config_dict)
    request_id = uuid.uuid1().hex
    params = { 'config_json': json.dumps(config_json_kd_level1) , "strategy_df":(pd.read_csv(r"strategy\FX\technical_indicator\oscillator\KD\strategy_pool\KD_lv1_strategy_pool.csv")).to_json()}
    config_json_kd_level1["data"]["FX"]["SymbolArgs"]["DataSource"] = "BT150"
    config_json_kd_level1["data"]["FX"]["SymbolArgs"]["BackupDataSource"] = "BT150"
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, data = params)
    print(res.text) 

    request_id = uuid.uuid1().hex
    config_json_kd_level2 = json.load(open(r'strategy/FX/technical_indicator/oscillator/KD/config/KD_lv2.json'))
    config_json_kd_level2.update(config_dict)
    config_json_kd_level2["data"]["FX"]["SymbolArgs"]["DataSource"] = "BT150"
    config_json_kd_level2["data"]["FX"]["SymbolArgs"]["BackupDataSource"] = "BT150"
    params = { 'config_json': json.dumps(config_json_kd_level2) , "strategy_df":(pd.read_csv(r"strategy\FX\technical_indicator\oscillator\KD\strategy_pool\KD_lv2_strategy_pool.csv")).to_json()}
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, data = params)
    print(res.text) 

    request_id = uuid.uuid1().hex
    config_json_level3 = json.load(open(r'strategy/Combination/combination_method1/config/combination.json'))
    config_json_level3.update(config_dict)
    config_json_level3["data"]["FX"]["SymbolArgs"]["DataSource"] = "BT150"
    config_json_level3["data"]["FX"]["SymbolArgs"]["BackupDataSource"] = "BT150"
    params = { 'config_json': json.dumps(config_json_level3) , "strategy_df":(pd.read_csv(r"strategy\Combination\combination_method1\strategy_pool\combination_strategy_pool.csv")).to_json()}
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, data = params)
    print(res.text) 


