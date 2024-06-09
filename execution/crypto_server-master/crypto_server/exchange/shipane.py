# -*- coding: utf-8 -*-
import requests

from .base import ExchangeBase
import pandas as pd


class Shipane(ExchangeBase):
	def __init__(self):
		self.endpoint = 'http://192.168.9.109:8888/{}'
		self.session = requests.session()
		self.accounts = {}

	def fetch_account_balance(self):
		path = self.endpoint.format('positions')
		payload = self.session.get(path, timeout=10).json()
		accounts = payload['subAccounts']
		holdings = payload['dataTable']
		holdings_df = pd.DataFrame(data=holdings['rows'],columns=holdings['columns'])
		holding_list = [dict(symbol=row['证券代码'], name=row['证券名称'], quantity=row['股票余额']) for idx, row in holdings_df.T.iteritems()]
		result = {
			'free': accounts['人民币']['可用金额'],
			'used': accounts['人民币']['冻结金额'],
			'cash': accounts['人民币']['现金资产'],
			'stocks': accounts['人民币']['股票市值'],
			'total': accounts['人民币']['总 资 产'],
			'holdings': holding_list
		}
		# data_table = payload['dataTable']
		# df = pd.DataFrame(data=data_table['rows'], columns=data_table['columns'])
		return result

	def create_order(self, symbol, order_type, action, amount, price=None):
		path = self.endpoint.format('orders')
		_, _, ticker = symbol.split('_')
		data = {
			'action': action.upper(),
			'symbol': ticker,
			'type': order_type.upper(),
			'priceType': 4,
			'price': price or 0,
			'amount': amount
		}
		result = self.session.post(path, json=data, timeout=10).json()
		return result

	def fetch_trades(self):
		path = self.endpoint.format('orders')
		payload = self.session.get(path, params={'status': 'filled'}, timeout=10).json()
		data_table = payload['dataTable']
		df = pd.DataFrame(data=data_table['rows'], columns=data_table['columns']).set_index('证券代码')
		return payload
