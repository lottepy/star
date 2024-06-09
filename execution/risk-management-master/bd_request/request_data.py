import pandas as pd
import requests
import json
from pandas.io.json import json_normalize
import requests
import numpy as np

_session = requests.session()


def retrieve_data(params):
	endpoint = 'https://market.aqumon.com/v1/market'
	resp = _session.get(endpoint + '/historical/quotes', params=params)
	return resp.json()

def request_weight():
	url = 'https://market.aqumon.com/v2/algo/algo_model/124069/' \
		  'weight?access_token=M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ'
	r = requests.get(url)
	context = r.json()['data']
	df = json_normalize(context)
	# df = df.set_index('iuid').astype(float)
	df.to_csv('../result/sg_weight.csv')
	print df

def request_backtest():
	url = 'https://market.aqumon.com/v2/algo/algo_model/124070/' \
		  'backtesting?access_token=M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ'
	r = requests.get(url)
	context = r.json()['data']
	df = json_normalize(context)
	# df = df.set_index('iuid').astype(float)
	df.to_csv('../result/shk_bt.csv')
	print df

def request_instrument(iuid, tags, period, start_dt, end_dt, currency):
	params = {
		'iuids': iuid,
		'tags': ','.join(map(str, tags)),
		'period': period,
		'start_date': start_dt,
		'start_date_ts': None,
		'end_date': end_dt,
		'end_date_ts': None,
		'adjust_type': 1,
		'tz_region': None,
		'currency': currency,
		'fill': 'nan',
		'precision': 4,
		'ordering': 'A',
		'format': 'json',
		'prelisting_fill': True,
		'with_ts': False
	}
	resp = retrieve_data(params)
	if not resp:
		return resp

	df_data = {}
	for key, value in resp.items():
		ar = np.array(value)
		key = tuple(key.split('.'))
		df_data[key] = pd.Series(ar[:, 1], index=ar[:, 0])
	df = pd.DataFrame.from_dict(df_data)
	return df


def request_sie_weight():
	url = 'https://test-sie.aqumon.com/api/v2/backtesting?' \
		  'benchmark_iuid=CN_30_000300&frequency=1&amount=1000000' \
		  '&start_date=2018-01-31&end_date=2018-02-28&strategy_id=64' \
		  '&iuids=CN_10_000002,CN_10_000009,CN_10_000005,CN_10_000006' \
		  '&weights=0.25,0.25,0.25,0.25'
	r = requests.get(url)
	context = r.json()['data']
	df = json_normalize(context)
	# df = df.set_index('iuid').astype(float)
	df.to_csv('../result/sie_weight.csv')
	print df


if __name__ == '__main__':
	# request_sie_weight()
	# request_backtest()

	result = request_instrument(
		'HK_10_3081',
		[4, 14],
		'D',
		'2014-12-31',
		'2019-05-01',
		'HKD'
	)
	# print result
	result.to_csv('../result/etf_data.csv')