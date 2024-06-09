import datetime
import json
import math
import os
import uuid

import numpy as np
import pandas as pd
import requests
from pandas.tseries.offsets import BDay

req_url = "http://172.29.38.123:1234/api/result"
#req_url = "http://192.168.8.130:1234/api/result"

if __name__ == "__main__":
    request_id = uuid.uuid1().hex
    params = { "engine_name":"portfolio","id_list":list(range(100)),"fields":["pnl","position","cost"]}
    res = requests.get(url=req_url,headers={'x-request-id': request_id}, json = params)
    #print(res.text) 


    df_list = [pd.read_json(json.loads(res.text)['data'][str(i)][0]).set_index('index') for i in list(range(100))]
    df = pd.concat(df_list, axis=1, join='outer')
    print(df) 
    dt = "20201127"
    df.to_csv(f"portfolio_pnl_{dt}.csv")

    df_list = [pd.read_json(json.loads(res.text)['data'][str(i)][1]).set_index('index') for i in list(range(100))]
    df = pd.concat(df_list, axis=1, join='outer')
    print(df) 
    df.to_csv(f"portfolio_position_{dt}.csv")

    df_list = [pd.read_json(json.loads(res.text)['data'][str(i)][2]).set_index('index') for i in list(range(100))]
    df = pd.concat(df_list, axis=1, join='outer')
    print(df) 
    df.to_csv(f"portfolio_cost_{dt}.csv")
    #df.to_csv("position.csv")
