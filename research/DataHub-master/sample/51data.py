# -*- coding: utf-8 -*-
import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from six import string_types
import ast

_session = requests.session()

# http://bbs.jctytech.com/index.php/index/stock/readme.html

# get cn stock bid ask
def get_data(symbols = ['700-HK','5-HK','VTI-US']):
	headers = {
		'Content-Type': 'application/json',
	}
	endpoint = 'http://data.api51.cn/apis/multi_real/'
	if isinstance(symbols,string_types):
		symbol_str = symbols
	else:
		symbol_str = ','.join(symbols)
	param = {
		'code': symbol_str,
		'token': 'ab63f1366c07e557436b289a27456cd9',
	}
	response = _session.get(endpoint, params=param, headers=headers,
	                         # auth=HTTPBasicAuth(user, pwd)
	                         )
	rj = json.loads(response.content.decode("GB2312"))
	tickers = [i for i in list(rj.keys()) if '-' in i]
	data={}
	for symbol in tickers:
		data[symbol] = {list(i.keys())[0]: list(i.values())[0] for i in list(rj[symbol]['Fields'].values())}

	data_df = pd.DataFrame(data)
	return data_df

data = get_data(['rt_hk00700','sz000001'])