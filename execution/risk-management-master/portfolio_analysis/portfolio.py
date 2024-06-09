# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime
import math


class Portfolio:
	def __init__(self, df, name):
		self.position = df
		self.price = None
		self.pv = None
		self.name = name
		self.report = pd.DataFrame(None, index=[self.name])
		self.benchmark = None

	@classmethod
	def from_csv(cls, path, name):
		return cls(pd.read_csv(path, index_col=0), name)

	def fetch_price(self, method='csv', path=''):
		if method == 'csv':
			self.price = pd.read_csv(path, index_col=0, parse_dates=True)

	def simple_analysis(self, start=None, end=None, risk_metrics=True):
		self._compute_weight_pv()
		if risk_metrics:
			self._compute_risk_metrics(start, end)

	def _compute_weight_pv(self):
		value_df = self.position * self.price
		value_df['pv'] = value_df.sum(axis=1)
		self.weight = value_df.iloc[:, :-1].div(value_df['pv'], axis='index')
		self.pv = value_df['pv'] / value_df['pv'].iloc[0]
		self.pv = self.pv.rename(self.name)
		self.acc_return = self.pv.sub(1.)

	def _compute_risk_metrics(self, start, end):
		start_dt = end_dt = None
		if start == None:
			start_dt = datetime.strptime(self.acc_return.index.values[0], '%Y/%m/%d')
		if end == None:
			end_dt = datetime.strptime(self.acc_return.index.values[-1], '%Y/%m/%d')
		period = (end_dt - start_dt).days

		pv = self.pv.loc[start:end]
		acc_return = self.acc_return.loc[start:end]

		self.report['Total Return'] = acc_return.iloc[-1]
		self.report['Return p.a.'] = np.power(self.report['Total Return'] + 1., 365. / period) - 1

		daily_return = pv.pct_change().dropna()
		self.report['Volatility'] = daily_return.std() * math.sqrt(252)
		if self.report['Return p.a.'].values >= 0:
			self.report['Sharpe Ratio'] = self.report['Return p.a.'] / self.report['Volatility']
		else:
			self.report['Sharpe Ratio'] = self.report['Return p.a.'] * self.report['Volatility'] * 10000
		self.report['Max Drawdown'] = (pv.div(pv.cummax()).sub(1.)).min()
		self.report['Calmar Ratio'] = self.report['Return p.a.'] / self.report['Max Drawdown']