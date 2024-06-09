import pandas as pd
import json
import os
import csv
import logging as log
import requests
from ast import literal_eval
from lib.commonalgo.setting.iuid_mapping import get_iuid_from_symbol
import time

_session = requests.session()

def covert_weight_iuid(weight_dict={}):
	new_weights = {}
	for ts, weight in weight_dict.items():
		new_weight = {}
		for k, v in weight.items():
			iuid = get_iuid_from_symbol(k)
			new_weight[iuid] = v
		new_weights[ts] = new_weight
	return new_weights

class ZiplineResultConverter(object):
	def __init__(self, result):
		self.result = result

	def convert_bt(self, data = None, freq = 'D', calendar = None, format = 'series'):
		if data is None:
			data = self.result.algorithm_period_return
		data.index = data.index.normalize()
		if calendar == 'all days':
			data = data.asfreq('D', method='pad')
		if freq == "M":
			data = data.asfreq('D', method='pad')
			data = data.asfreq('M', how='end', method='pad')

		acc_return = pd.concat([data,pd.Series(index=[data.index[0] - pd.Timedelta(1, unit=freq)],
		                                      data=[0.0])]).sort_index()

		port_nav = acc_return + 1.
		bt_dict = {k.date().isoformat(): '{:.6f}'.format(v) for k, v in port_nav.iteritems()}
		if format == 'json':
			return json.dumps(bt_dict)
		elif format =='list':
			return [[k, v] for k, v in bt_dict.items()]
		elif format =='dict':
			return bt_dict
		else:
			return pd.Series(bt_dict)

	def convert_weight(self, data = None, format = 'json', convert2iuid = True):
		if not data:
			data = self.result.weight

		weight_dict = {}
		row = {}
		for k, v in data.iteritems():
			if not v == row:
				weight_dict[int(k.value / 1e6)] = pd.Series(v).apply(lambda x: 0 if abs(x)<1e-7 else x).astype('S8').astype('float').astype('str').to_dict()
				row = v
		if convert2iuid:
			weight_dict = covert_weight_iuid(weight_dict)
		if format == 'json':
			return json.dumps(weight_dict)
		elif format =='list':
			return [[k, v] for k, v in weight_dict.items()]
		elif format =='dict':
			return weight_dict
		elif format =='table':
			weight_data = {}
			for k, v in weight_dict.items():
				weight_data[k] = pd.Series(v)
			df = pd.concat(weight_data,axis=1).T
			df.index = pd.to_datetime(df.index, unit='ms')
			return df
		else:
			return pd.Series(weight_dict)

	def covert_logger(self, algo_params = {}, db_sync_setting = {}):

		weight_dict = bt_dict = {}
		if db_sync_setting.weight:
			weight_dict = self.convert_weight(format='dict')
		if db_sync_setting.bt:
			bt_dict = self.convert_bt(format='dict')

		data_dict = {
			'algo_type_id': algo_params.get('algo_type_id'),
			'risk_ratio': algo_params.get('risk_ratio'),
			'region': algo_params.get('region'),
			'sector': algo_params.get('sector'),
			'backtesting': bt_dict,
			'weight': weight_dict
		}

		return data_dict

	def convert_transactions(self, data=None, format='table'):
		if data is None:
			data = self.result[['orders','transactions','positions']]

		transaction_dict = {}
		i = 1
		for dt, row in data.iterrows():
			orders = row['orders']
			transactions = row['transactions']
			positions = row['positions']

			positions_dict = {p.get('sid').symbol: p for p in positions}
			trans_dict = {p.get('sid').symbol: p for p in transactions}
			for n, order in enumerate(orders):

				sid = order.get('sid')
				created_time = order.get('created')
				transaction_time = order.get('dt')
				amount = order.get('amount')
				commission = order.get('commission')
				filled = order.get('filled')

				price = trans_dict.get(sid.symbol).get('price')

				last_sale_price = positions_dict.get(sid.symbol).get('last_sale_price')
				cost_basis = positions_dict.get(sid.symbol).get('cost_basis')
				position_amount = positions_dict.get(sid.symbol).get('amount')

				transaction_dict[i] = {'security': sid.symbol,
									   'price': price,
									   'created_time': created_time,
									   'amount': abs(amount),
									   'side': 'buy' if amount>0 else 'sell',
									   'commission': commission,
									   'filled': abs(filled),
									   'last_sale_price': last_sale_price,
									   'cost_basis': cost_basis,
									   'position_amount': position_amount,
									   }
				i += 1

		if format == 'json':
			return json.dumps(transaction_dict)
		elif format =='list':
			return [[k, v] for k, v in transaction_dict.items()]
		elif format =='dict':
			return transaction_dict
		elif format=='table':
			return pd.DataFrame(transaction_dict).T



class ZiplineResultLogger(object):
	def __init__(self, filename = 'test.csv', initialize = True):
		if initialize:
			try:
				os.remove(filename)
			except OSError:
				pass
		self.filename = filename
		self.headers = [
			'algo_type_id',
			'risk_ratio',
			'region',
			'sector',
			'backtesting',
			'weight',
			'fund_type',
		]
		self.writer = None
		self.init_writer()

	def init_writer(self):
		if not os.path.isfile(self.filename):
			with open(self.filename, 'a') as f:
				writer = csv.writer(f)
				writer.writerow(self.headers)

	def write_result(self, data= {}):
		try:
			rows = []
			row = [
			data.get('algo_type_id'),
			data.get('risk_ratio'),
			data.get('region'),
			data.get('sector'),
			data.get('backtesting'),
			data.get('weight'),
			data.get('fund_type')
			]
			rows.append(row)
			with open(self.filename, 'a') as f:
				writer = csv.writer(f)
				writer.writerows(rows)
		except:
			log.exception('failed to write file')