import requests
import json
import pandas as pd
import re

url = 'https://market.aqumon.com/v2/market/instrument/info/update?access_token=M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ'
data = pd.read_excel('/Users/lizhirui/Desktop/all_symbol2.xlsx',sheet_name='all',index_col=0)
for i in range(data.shape[0]):
    body={}
    body["iuid"] = data.index[i]
    body["ticker"] = data.index[i][6:]
    body["contents"] = [
        {
        "language": "en",
        "name":data["英文简称"][i],
        # "recommend_reason":data["英文简介"][i],
        # "exchange_link":"111"
        },
        {
        "language": "zh-hans",
        "name":data["简体简称"][i],
        # "recommend_reason":data["简体简介"][i],
        # "exchange_link":"222"
        },
        {
        "language": "zh-hant",
        "name":data["繁体简称"][i],
        # "recommend_reason":data["繁体简介"][i],
        # "exchange_link":"333"
        },
    ]
    print(body)
    res = requests.post(url=url,json=body)
    print(res)
