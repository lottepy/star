import datetime
import json
import math
import os
import uuid

import numpy as np
import pandas as pd
import requests
from pandas.tseries.offsets import BDay


req_url = "http://172.29.39.140:1235/api/strategy_subscription"
#req_url = "http://192.168.8.22:1234/api/strategy_subscription"


if __name__ == "__main__":
    
    config_dict = {}
    config_dict["start_year"] = 2018
    config_dict["start_month"] = 12
    config_dict["start_day"] = 1
    config_dict["end_year"] = 2020
    config_dict["end_month"] = 11
    config_dict["end_day"] = 16

    df = pd.read_csv(r"examples\strategy\stocks\strategy_pool\EW_lv1_strategy_pool.csv")
    #df.loc[0]["symbol"] = str({"STOCKS": ["JPST.US", "AGG.US"]})

    STOCK_config = json.load(open(r'examples\strategy\stocks\config\EW_stock_lv1_oms.json'))
    STOCK_config.update(config_dict)
    #STOCK_config["PortfolioTaskEngineOrderRouter"]["ACCOUNT"] = "363"
    #STOCK_config["PortfolioTaskEngineOrderRouter"]["brokerType"] = "IB"
    #STOCK_config["algo"]["base_ccy"] = "USD"

    STOCK_config["engine"]["engine_name"] = "EW_lv1_hk_ib"
    STOCK_config["result_output"]["save_name"] = "EW_lv1_hk_ib"
    STOCK_config["result_output"]["status_name"] = "EW_lv1_hk_ib"


    request_id = uuid.uuid1().hex
    params = { 'config_json': json.dumps(STOCK_config) , "strategy_df":df.to_json()}
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, data = params)
    print(res.text) 