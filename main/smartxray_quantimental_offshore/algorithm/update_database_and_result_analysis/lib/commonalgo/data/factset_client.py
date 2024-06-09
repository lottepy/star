import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import ast
from six import string_types

session = requests.session()

def get_data(symbols = ['700-HK','5-HK','VTI-US']):
	headers = {
		'Content-Type': 'application/json',
	}
	user = 'MAGNUMWM_HK_788686_SERVICES'
	pwd = 'yf9UAi8fMU3PG3qj'
	endpoint = 'https://datadirect.factset.com/services/DFSnapshot?'
	if isinstance(symbols,string_types):
		symbol_str = symbols
	else:
		symbol_str = ','.join(symbols)
	param = {
		'serv': 'FDS1',
		'req_id':99,
		'ids': symbol_str,
		'format':'json',
	}
	# url = 'https://datadirect.factset.com/services/DFSnapshot?serv=FDS1&req_id=99&format=json&ids=' + symbols
	response = session.post(endpoint,data=param, headers=headers, auth=HTTPBasicAuth(user, pwd))
	rj = json.loads(response.content.decode("utf-8"))
	tickers = [i for i in list(rj.keys()) if '-' in i]
	data={}
	for symbol in tickers:
		data[symbol] = {list(i.keys())[0]: list(i.values())[0] for i in list(rj[symbol]['Fields'].values())}

	data_df = pd.DataFrame(data)
	return data_df


# data = get_data('MHIU18-HKF') # get future data
# data = get_data(['700-HK','MHIU18-HKF']) # get equity data
# print (data)