import requests
import json
from six import string_types
# https://1forge.com/

_session = requests.session()


endpoint = "https://forex.1forge.com/1.0.3/quotes"
apikey = "5RzSNt771Zsm33zL0PAskfkI4ne9ATsv"

def get_forex_quote(pair_list = ['EURUSD', 'USDCNH']):
	if isinstance(pair_list,string_types):
		pair_str = pair_list
	else:
		pair_str = ','.join(pair_list)
	payload = {
		'pairs': pair_str,
		'api_key': apikey
	}

	response = _session.get(endpoint, params = payload).json()
	return response

forex_data = get_forex_quote(['CNHHKD', 'USDCNH','USDHKD'])
print(forex_data)