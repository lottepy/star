# -- coding: utf-8 --
import pandas as pd
import json
import os
import csv
import logging as log
import requests
from ast import literal_eval
import time
from ..setting.network_utils import ALGO_EP

_session = requests.session()
ALGO_EP = f'{ALGO_EP}/v2/algo/'

class MarketAlgoAPI(object):
	def __init__(self, endpoint = ALGO_EP):
		if not endpoint:
			self.endpoint = ALGO_EP
		else:
			self.endpoint = endpoint

	def get_algomodel(self, data_dict):
		endpoint = self.endpoint + 'algo_model'
		params = {
			'algo_type_id': data_dict.get('algo_type_id'),
			'risk_ratio': data_dict.get('risk_ratio'),
			'region': data_dict.get('region'),
			'sector': data_dict.get('sector'),
			# 'version': '1',
			"access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ"
		}
		result = _session.get(url=endpoint, params=params).json()

		if not result.get('status').get("ecode"):
			return result.get('data')
		else:
			return result

	def get_backtesting(self, modelid):

		endpoint = self.endpoint + 'algo_model/{}/backtesting'.format(modelid)
		params = {
			"access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ",
		}

		result = _session.get(url = endpoint, params = params).json()

		if not result.get('status').get("ecode"):
			return result.get('data')
		else:
			return result

	def get_weight(self, modelid):
		endpoint = self.endpoint + 'algo_model/{}/weight'.format(modelid)
		params = {
			"access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ",
		}

		result = _session.get(url=endpoint, params=params).json()

		if not result.get('status').get("ecode"):
			return result.get('data')
		else:
			return result


	def list_algo_type(self, algo_class_id):
		endpoint = self.endpoint + 'algo_type'
		params = {
			"access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ",
			"algo_class_id": algo_class_id,
			# "version": '2'
		}
		result = requests.get(endpoint, params=params).json()

		if not result.get('status').get("ecode"):
			return result.get('data')
		else:
			return result


	def post_algomodelfull(self, data_dict):

		payload = {
			'algo_type_id': data_dict.get('algo_type_id'),
			'risk_ratio':data_dict.get('risk_ratio'),
			'region': data_dict.get('region'),
			'sector': data_dict.get('sector'),
			'backtesting': data_dict.get('backtesting'),
			'weight': data_dict.get('weight'),
			'fund_type': data_dict.get('fund_type'),
			# 'version':'1'
			# "access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ"
		}

		import json
		with open('result_test.json', 'w') as fp:
			json.dump(payload, fp,ensure_ascii=False)

		endpoint = self.endpoint + 'algo_model_full?access_token=M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ'
		# headers = {'Content-Type': 'application/json; charset=utf-8'}
		result = _session.post(url = endpoint, json = payload)
		return result

def post_localcsv(filename = '', endpoint = None):
	data = pd.read_csv(filename)
	algoapi = MarketAlgoAPI(endpoint)
	for idx, row in data.iterrows():
		row_dict = dict(row)
		row_dict['backtesting'] = literal_eval(row.get('backtesting'))
		row_dict['weight'] = literal_eval(row.get('weight'))
		row_dict['fund_type'] = literal_eval(row.get('fund_type'))
		result = algoapi.post_algomodelfull(data_dict= row_dict)
		time.sleep(0.2)
	return