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
#req_url = "http://192.168.8.130:1234/api/strategy_subscription"


if __name__ == "__main__":
    df = pd.read_csv(r"examples\strategy\stocks\strategy_pool\EW_lv1_strategy_pool.csv")       
    df["strategy_module"][0] = "quantcycle_tmp.EW_lv1"
    STOCK_config = json.load(open(r'examples\strategy\stocks\config\EW_stock_lv1_oms.json'))
    request_id = uuid.uuid1().hex
    params = { 'config_json': json.dumps(STOCK_config)   , "strategy_df":df.to_json(),"is_override":True}
    f = open('examples/strategy/stocks/algorithm/EW_lv1.py', 'rb')
    res = requests.post(url=req_url,
                        headers={'x-request-id': request_id},
                        data = params,
                        files = {'EW_lv1.py': f, # file name same as the one in strategy_pool
                                                 # no need to care about the dir path
                                 }) 
    f.close()
    print(res.text) 