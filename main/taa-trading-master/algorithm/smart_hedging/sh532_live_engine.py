"""
SmartHedging实盘，基于SGM-5.3.0算法

Authors:
	Lance LI

Owner:
	Magnum Research Ltd.
"""

from lib.commonalgo.execution.trading_executor import TradingExecutor
import lib.commonalgo.data.bbg_downloader as bbg_data
import lib.commonalgo.data.factset_client as fst_data
import utils.intrinio_api as intrinio_data
from vix_futures import VixFutures
from utils.crontask import register_task, run
from subprocess import call
import logging
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np

PAPER_1 = {
	'broker': '1',
	'account': 'DU1161139',
}
REGION = 'US'
SGM_WEIGHT_PATH = '../../data/sgm/sgm532_20190320.csv'
# REGION = 'HK'
# SGM_WEIGHT_PATH = '../../data/sample_weight/target_weight_HK.csv'


class SmartHedgingEngine:
	def __init__(self, account, region, weight_csv, entry_pctl=50, exit_pctl=98,
				 hedging_status='EMPTY', hedging_asset='VXXB', safe_asset='SHV'):
		self.account = account
		self.region = region
		self.weight_csv = weight_csv
		self.trading_exe = TradingExecutor(
			paper=self.account,
			target_weight_csv=self.weight_csv,
			region=self.region,
			activate_futu=False,  # HK/US/CN
			activate_factset=False,  # HK/US
			activate_choice=False,  # CN
			activate_msq=False,  # HK/US/CN
			activate_intrinio=True,  # US
			channel_priority=['intrinio'],
			disable_msq=False
		)
		self.entry_pctl = entry_pctl
		self.exit_pctl = exit_pctl
		self.hedging_status = hedging_status
		self.hedging_asset = hedging_asset
		self.safe_asset = safe_asset

	def __str__(self):
		return 'Smart Hedging Live'

	def _pre_trade(self):
		self.logger = logging.getLogger('root')
		self.logger.setLevel(logging.DEBUG)
		ch = logging.StreamHandler()
		ch.setLevel(logging.DEBUG)
		formatter = logging.Formatter('%(asctime)s [%(funcName)s]::%(levelname)s::%(message)s')
		ch.setFormatter(formatter)
		self.logger.addHandler(ch)

		hdlr = logging.FileHandler(
			'../../log/smart_hedging/sh532_live_'
			+ time.strftime("%Y-%m-%d-%H-%M", time.localtime()) + '.log'
		)
		hdlr.setFormatter(formatter)
		self.logger.addHandler(hdlr)
		self.logger.info('Pre-trade process done.')

	@staticmethod
	def update_vix_info():
		vix_df = pd.read_csv(
			'../../data/vix_futures/vix-futures.csv',
			index_col=0,
			parse_dates=True
		)
		last_record_dt = vix_df.index.values[-1].astype('datetime64[D]')
		current_dt = np.datetime64(datetime.now()).astype('datetime64[D]')

		bus_day_count = np.busday_count(last_record_dt, current_dt)
		data_record = []
		for i in range(1, bus_day_count + 1):
			# tmp_dt = last_record_dt + np.timedelta64(1, 'D')
			tmp_dt = np.busday_offset(last_record_dt, i, roll='forward')
			last_record_dt_str = last_record_dt.astype('str')
			tmp_dt_str = tmp_dt.astype('str')
			vxx = VixFutures(tmp_dt_str)
			vxx.get_third_wed()
			vxx.get_contract_ticker()
			vxx.fetch_data_from_bbg(last_record_dt_str, tmp_dt_str)

			ratio = float(vxx.M1_expiration) / vxx.contract_life
			futures_level = ratio * vxx.M1_data + (1. - ratio) * vxx.M2_data
			# data_record.append([futures_level, vxx.M1_data, vxx.M2_data])
			update_df = pd.DataFrame(
				data=np.array([futures_level, vxx.M1_data, vxx.M2_data]),
				columns=[tmp_dt],
				index=vix_df.columns.values
			).T
			vix_df = vix_df.append(update_df)

		vix_df.to_csv('../../data/vix_futures/vix-futures.csv')
		print('VIX info already updated.')

	def _order_sgm(self, clear_position):
		"""
		SGM下单模块
		
		:param clear_position: 
			执行前是否需要平掉所有当前仓位
		:return: 
			SGM执行结果
		"""
		self.trading_exe.cancel_all_active_orders()
		self.trading_exe.sync_positions()
		self.cash = self.trading_exe.get_current_cash()
		self.positions = self.trading_exe.get_current_positions().set_index('iuid', drop=False)
		self.logger.info('Account Cash Info:\n{}'.format(self.cash))
		self.logger.info('Account Position Info:\n{}'.format(self.positions))

		# 如果有需要，先平掉所有仓位
		if clear_position and (not self.positions.empty):
			self.trading_exe.liquidate(current_positions=self.positions)

		# 计算目标仓位
		target_positions = self.trading_exe.calculate_target_positions(
			percentage_of_cash_buffer=0.05
		)
		self.logger.info('Target Position:\n{}'.format(target_positions))

		# 抛单并收集下单结果
		orders_sell, orders_buy = self.trading_exe.create_order_basket(target_positions)
		sell_results = self.trading_exe.execute_sell(orders_sell)
		buy_results = self.trading_exe.execute_buy(orders_buy)

		# 获取下单后账户持仓和现金情况
		self.trading_exe.sync_positions()
		self.cash = self.trading_exe.get_current_cash()
		self.positions = self.trading_exe.get_current_positions().set_index('iuid', drop=False)
		self.logger.info('Account Cash Info:\n{}'.format(self.cash))
		self.logger.info('Account Position Info:\n{}'.format(self.positions))

		if True:
			# TODO: 加入判断SGM组合是否下单成功的条件
			result = 'success'
		else:
			result = 'failure'

		self.logger.info('SGM portfolio order status: {}'.format(result))
		return result

	def _order_hedging(self):
		"""
		对冲组合下单模块
		
		:return: 
			对冲组合执行结果
		"""
		current_date = datetime.now()
		self.vxx_underlying = VixFutures(current_date.date().strftime('%Y-%m-%d'))
		vix_df = pd.read_csv('../../data/vix_futures/vix-futures.csv')
		futures_level = self._monitor(current_date, vix_df)

		temp_file = vix_df.iloc[-505:]
		vxx_entry = np.percentile(temp_file['Simulated Index'].values, self.entry_pctl)
		vxx_exit = np.percentile(temp_file['Simulated Index'].values, self.exit_pctl)

		vix_action = self._generate_signal(futures_level, vxx_entry, vxx_exit)
		hedging_info = pd.Series(
			[futures_level, vxx_entry, vxx_exit, self.hedging_status, vix_action],
			index=['futures_level', 'entry', 'exit', 'status', 'action']
		)
		self.logger.info('Hedging Info:\n{}'.format(hedging_info))

		if vix_action == 'NONE':
		# if False:
			pass
		else:
			# order_dict = self._make_vxx_orders(vix_action)
			sell_order_dict, buy_order_dict = self._make_vxx_orders('BUY')
			self.logger.info('SELL order dict:\n{}'.format(sell_order_dict))
			self.logger.info('BUY order dict:\n{}'.format(buy_order_dict))
			sell_order_info = self.trading_exe.place_order(sell_order_dict)
			buy_order_info = self.trading_exe.place_order(buy_order_dict)

			if True:
				# TODO: 加入判断Hedging部分是否下单成功的条件
				result = 'success'
			else:
				result = 'failure'

		self.logger.info('Hedging portfolio order status: {}'.format(result))
		return result


	def _make_vxx_orders(self, direction):
		assert direction in ['BUY', 'SELL']

		sell_order = {}
		buy_order = {}
		realtime_data = intrinio_data.get_data(['VXXB.NB', 'SHV.NB'])

		if direction == 'BUY':
			sell_ticker = 'US_10_' + self.safe_asset
			buy_ticker = 'US_10_' + self.hedging_asset
			ask_price = realtime_data.loc['ask_price_4d', buy_ticker.split('_')[2]] / 10000.
			bid_price = realtime_data.loc['bid_price_4d', sell_ticker.split('_')[2]] / 10000.
			sell_quantity = int(self._get_single_position(sell_ticker))
			sell_amount = sell_quantity * bid_price
			buy_quantity = sell_amount // ask_price

			sell_order = dict(
				type='LIMIT',
				side='SELL',
				symbol=sell_ticker,
				quantity=sell_quantity,
				price=bid_price
			)
			buy_order = dict(
				type='LIMIT',
				side='BUY',
				symbol=buy_ticker,
				quantity=buy_quantity,
				price=ask_price
			)
		elif direction == 'SELL':
			sell_ticker = 'US_10_' + self.hedging_asset
			buy_ticker = 'US_10_' + self.safe_asset
			ask_price = realtime_data.loc['ask_price_4d', buy_ticker.split('_')[2]] / 10000.
			bid_price = realtime_data.loc['bid_price_4d', sell_ticker.split('_')[2]] / 10000.
			sell_quantity = self._get_single_position(sell_ticker)
			sell_amount = sell_quantity * bid_price
			buy_quantity = sell_amount // ask_price

			sell_order = dict(
				type='LIMIT',
				side='SELL',
				symbol=sell_ticker,
				quantity=sell_quantity,
				price=bid_price
			)
			buy_order = dict(
				type='LIMIT',
				side='BUY',
				symbol=buy_ticker,
				quantity=buy_quantity,
				price=ask_price
			)

		return sell_order, buy_order

	def _get_single_position(self, ticker):
		if ticker in self.positions.index.values:
			return self.positions.loc[ticker, 'holdingPosition']
		else:
			return 0

	def _monitor(self, current_date, df):
		self.vxx_underlying.get_third_wed()
		self.vxx_underlying.get_contract_ticker()
		self.vxx_underlying.fetch_data_from_factset()

		ratio = float(self.vxx_underlying.M1_expiration) / self.vxx_underlying.contract_life
		futures_level = ratio * self.vxx_underlying.M1_data + (1. - ratio) * self.vxx_underlying.M2_data

		return futures_level

	def _generate_signal(self, level, entry_th, exit_th):
		assert self.hedging_status in ['EMPTY', 'HEDGING', 'PARKING']

		if level < entry_th and self.hedging_status != 'HEDGING':
			return 'BUY'
		elif level > exit_th and self.hedging_status != 'PARKING':
			return 'SELL'
		else:
			return 'NONE'

	def run_live_trading(self, clear_position=False):
		self._pre_trade()

		sgm_result = self._order_sgm(clear_position)

		# self.cash = self.trading_exe.get_current_cash()
		# self.positions = self.trading_exe.get_current_positions().set_index('iuid', drop=False)
		# self.logger.info('Account Cash Info:\n{}'.format(self.cash))
		# self.logger.info('Account Position Info:\n{}'.format(self.positions))
		# hedging_result = self._order_hedging()



if __name__ == '__main__':
	sh_engine = SmartHedgingEngine(
		account=PAPER_1,
		region=REGION,
		weight_csv=SGM_WEIGHT_PATH,
		entry_pctl=50,
		exit_pctl=96,
		hedging_status='EMPTY',
		hedging_asset='VXXB',
		safe_asset='SHV'
	)

	sh_engine.run_live_trading(clear_position=False)
	# SmartHedgingEngine.update_vix_info()