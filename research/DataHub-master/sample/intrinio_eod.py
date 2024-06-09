# -*- coding: utf-8 -*-
import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from six import string_types
import ast
from base64 import b64encode


_session = requests.session()

# https://intrinio.com/financial-data/us-stock-prices-api?method=api

def get_eod_price(symbol = 'AAPL', start_date = None, end_date = None,
				  frequency = 'daily',
				  page_number=1,
				  page_size=1000,
				  sort_order ='desc'):
	headers = {
		'Content-Type': 'application/json',
	}
	user = 'fd89f36635604f0e0810f0bef2707b99'
	pwd = 'a61509770c62a492ed5be350c7f51381'

	endpoint = 'https://api.intrinio.com/prices'

	param = {
		'identifier':symbol,
		'start_date':start_date,
		'end_date':end_date,
		'frequency':frequency,
		'page_number':page_number,
		'page_size':page_size,
		'sort_order':sort_order
	}
	response = _session.get(endpoint, params=param, headers=headers,
							auth=HTTPBasicAuth(user, pwd)
							)
	if response:
		return pd.DataFrame(response.json().get('data'))
	else:
		return pd.DataFrame([])

# symbols = ['HD','DIS','MSFT','BA','MMM','NKE','JNJ','MCD','INTC','XOM']
# symbols = ['QQQ','VTI','SCHF']
symbols = ['VTI','VXXB']

for symbol in symbols:

	data = get_eod_price(symbol,'2018-01-01','2019-05-15')
	if len(data):
		fname = 'log/intrinio_' + symbol + '.csv'
		print (data.head())
		data.to_csv(fname)