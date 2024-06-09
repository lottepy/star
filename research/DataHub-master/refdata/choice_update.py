# -*- coding:utf-8 -*-

from datetime import timedelta, datetime

from lib.commonalgo.data.choice_proxy_client import choice_client
import pandas as pd

data = pd.read_csv('choice_code.csv')
code_list = list(data['CODE'])
name_list = list(data['板块名称'])
code_dict = dict(list(zip(code_list,name_list)))

data_dict = {}
data_name_dict = {}

# for code in list(data['CODE']):
# 	name = code_dict.get(code)
# 	data = choice_client.cps(code, "s0,CODE;s1,NAME", "[s0]>0", "orderby=ra([s0])")
# 	if len(data):
# 		data_dict[code] = data_name_dict[name] = data['SecuCode'].values
#
# pd.DataFrame.from_dict(data_dict,'index').T.to_csv('data_code.csv')
# pd.DataFrame.from_dict(data_name_dict,'index').T.to_csv('data_name.csv')

data = pd.read_csv('data_code.csv')
stock_list = list(data['B_001004'])
data_dict = {}
for code in list(data.columns)[1:]:
	status = data[code][~data[code].isna()]
	data_dict[code] = pd.Series(index=status,data=1)
data_df = pd.DataFrame(data_dict)
data_df = data_df.fillna(0)
data_df.astype('int').to_csv('A_allstock_status.csv')


