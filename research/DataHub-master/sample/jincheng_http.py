# -*- coding: utf-8 -*-
import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from six import string_types
import ast
import datetime
import time

_session = requests.session()

# http://bbs.jctytech.com/index.php/index/stock/readme.html

# get cn stock bid ask
def get_detail(symbols = ['700-HK','5-HK','VTI-US']):
	# get realtime price
	endpoint = 'http://dt.jctytech.com/stock.php'
	if isinstance(symbols,string_types):
		symbol_str = symbols
	else:
		symbol_str = ','.join(symbols)
	param = {
		# 'u':'jcy890',
		'u':'test',
		'symbol': symbol_str,
		'type': 'detail',
	}
	response = _session.get(endpoint, params=param)
	rj = json.loads(response.content.decode("utf-8"))
	tickers = [i for i in list(rj.keys()) if '-' in i]
	data={}
	for symbol in tickers:
		data[symbol] = {list(i.keys())[0]: list(i.values())[0] for i in list(rj[symbol]['Fields'].values())}

	data_df = pd.DataFrame(data)
	return data_df

def list_symbols(market ='IXIX'):
	endpoint = 'http://ds.jctytech.com/stock.php'
	param = {
		'u': 'test',
		'market': market,
		'type': 'stock',
	}
	result = _session.get(endpoint, params=param).json()
	print (result)
	return result

def get_kline(symbol = 'IXIXXIN9', st = None, et = None, line=None, num = None, sort = None):
	endpoint = 'http://ds.jctytech.com/stock.php'
	param = {
		'u': 'test',
		'type': 'kline',
		'symbol': symbol,
		'st': st,
		'et': et,
		'line': line,
		'num': num,
		'sort': sort
	}
	result = _session.get(endpoint, params=param).json()
	print(result)
	return result


data = list_symbols('IXIX')

start = int(pd.to_datetime('2018-04-01').timestamp())
end = int(pd.to_datetime('2018-04-10').timestamp())

data = get_kline(symbol='IXIXHSI', st = start, et=end, line='min,1')
df = pd.DataFrame(data)
pd.DataFrame(data).to_clipboard()
print()
