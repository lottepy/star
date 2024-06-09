# -*- coding: utf-8 -*-
import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from six import string_types
import ast
from base64 import b64encode


_session = requests.session()

# http://help.intrinio.com/data-feeds/how-to-nasdaq-basic-real-time-prices-available-via-websockets-sdk-web-api

# get cn stock bid ask
def get_data_intrinio(symbols = ['AAPL.NB','BABA.NB','VWO.NB']):
	headers = {
		'Content-Type': 'application/json',
	}
	user = 'fd89f36635604f0e0810f0bef2707b99'
	pwd = 'a61509770c62a492ed5be350c7f51381'

	endpoint = 'https://api.intrinio.com/token'
	if isinstance(symbols,string_types):
		symbol_str = symbols
	else:
		symbol_str = ','.join(symbols)
	param = {
		'type':'QUODD',
	}
	response = _session.get(endpoint, params=param, headers=headers,
	                        auth=HTTPBasicAuth(user, pwd)
	                        )
	token = response.text
	data_body = {
		"tickers": symbol_str
	}

	data_url = "https://www5.quodd.com/quoddsnap/c/intrinio/t/{}".format(token)
	response = _session.post(data_url, json=data_body, headers=headers)
	data_df = pd.DataFrame(response.json()).T
	data_df.columns = 'US_10_' + data_df.loc['root_ticker'].values

	return data_df


if __name__ == '__main__':
	data = get_data_intrinio(['VXX.NB','BABA.NB','VWO.NB'])
	print (data)
