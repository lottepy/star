import numpy as np
import pandas as pd
from lib.commonalgo.execution.trade.trading_controller import TradingController
from lib.commonalgo.execution.trade.trading import trading
from lib.commonalgo.data.oss_client import OSS_Client
from algorithm.smart_hedging.vix_futures import VixFutures
from utils.vix_futures import VixFutures
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pytz import timezone
import logging
import time
import os

oss = OSS_Client()

END_POINT = 'http://172.31.86.12:5565/api/v3/'
VIX_LOOKBACK_WINDOW = 500
ROOT_PATH = os.path.dirname(os.path.abspath(__file__))


class TAA(object):
	def __init__(self, account='DU123456', currency='USD', region='US'):
		self._account = account
		self.trade_controller = TradingController(endpoint=END_POINT)
		self._currency = currency
		self._region = region
		self._sync_holdings()
		self._oss_base = 'algo_trading/'
		self._init_logger()

	def _init_logger(self):
		self.logger = logging.getLogger('TAA')
		self.logger.setLevel(logging.DEBUG)

		self.formatter = logging.Formatter('%(asctime)s — %(name)s — %(levelname)s '
									  '— %(filename)s:%(lineno)d — %(message)s')

		sh = logging.StreamHandler()
		sh.setLevel(logging.DEBUG)
		sh.setFormatter(self.formatter)
		self.logger.addHandler(sh)

	def _sync_holdings(self):
		self.trade_controller.sync_holdings(
			brokerid='1',
			baccount=self._account
		)

	def get_positions(self) -> pd.DataFrame:
		positions = trading.get_current_positions(
			self.trade_controller, paper={
				'broker': '1',
				'account': self._account,
				'subaccount': None
			},
			region=self._region
		)
		return positions[['iuid', 'holdingPosition']].set_index('iuid')

	def get_portfolio_value(self):
		cash_info = trading.get_current_cash_info(
			self.trade_controller, paper={
				'broker': '1',
				'account': self._account,
				'subaccount': None
			},
			target_currency=self._currency
		)
		portfolio_value, cash = cash_info['asset'], cash_info['balance']
		return portfolio_value, cash

	def _record_account_info(self):
		self._sync_holdings()
		self.positions = self.get_positions()
		self.portfolio_value, self.cash = self.get_portfolio_value()
		self.logger.info(f'Current Cash: {self.cash} {self._currency}')
		self.logger.info(f'Portfolio Value: {self.portfolio_value} {self._currency}')
		self.logger.info(f'Account Position Info:\n{self.positions}')

	def run(self):
		pass

