import pandas as pd
import json
from requests import session
from ast import literal_eval

_session = session()

# publish version 2018-01-09
# https://d1svladlv4b69d.cloudfront.net/src/d3/allocation-mountain-chart/js/data.js
# publish version 2018-08-13
url = 'https://d1svladlv4b69d.cloudfront.net/src/d3/allocation-mountain-chart/js/data.js'

data = _session.get(url)
data = data.content.split(";")
tax_data = pd.read_json(data[0].split('=')[1])
ira_data = pd.read_json(data[1].split('=')[1])
data_dict = {
    'tax':tax_data,
    'ira':ira_data,
}

# ira_js = 'data/btm_tax.json'
# js_data = pd.read_json(ira_js)

for key, js_data in data_dict.items():
    sym_list = list(js_data['label'].values)
    weight_list = list(js_data['data'].values)
    temp_df = pd.DataFrame()
    data_df = pd.DataFrame()
    sym_count = 0
    for sym in sym_list:
        risk_count = 0
        for r in weight_list[sym_count]:
            col_name = 'risk_' + str(risk_count)
            temp_df[col_name] = [r]
            risk_count+=1
        temp_df.index = [sym]
        # print temp_df
        data_df = pd.concat([data_df,temp_df],axis=0)
        sym_count += 1
    file_name = 'btm_weight_' + key + '.csv'
    data_df.to_csv(file_name)

# print js_data