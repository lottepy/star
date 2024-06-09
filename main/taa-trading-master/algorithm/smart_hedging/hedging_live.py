import numpy as np
import pandas as pd
from lib.commonalgo.execution.trade.trading_controller import TradingController
from lib.commonalgo.execution.trade.trading import trading
from lib.commonalgo.data.oss_client import OSS_Client
from algorithm.smart_hedging.vix_futures import VixFutures
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pytz import timezone
import logging
import time
import os
from algorithm.taa_base import TAA
from utils.intrinio_api import get_data_intrinio

oss = OSS_Client()

END_POINT = 'http://172.31.86.12:5565/api/v3/'
VIX_LOOKBACK_WINDOW = 500
LOG_PATH = '../../log/smart_hedging/'
VIX_DATA_PATH = '../../data/vix_futures/vix-futures.csv'
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))


class SmartHedging(TAA):
	def __init__(self, account='DU123456', currency='USD', region='US', oss_key='sh',
				 hedging_status='EMPTY', hedging_asset='VXX', safe_asset='SHV',
				 entry_pctl=50, exit_pctl=96, hedging_ratio=0.05):
		super(SmartHedging, self).__init__(account, currency, region)

		self.hedging_status = hedging_status
		self.hedging_asset = hedging_asset
		self.safe_asset = safe_asset
		self.oss_key = self._oss_base + 'scan/' + '_'.join(['ib', oss_key, self._account]) + '/'
		self._hedging_iuid = 'US_10_' + self.hedging_asset
		self._safe_iuid = 'US_10_' + self.safe_asset
		self.entry_pctl = entry_pctl
		self.exit_pctl = exit_pctl
		self.hedging_ratio = hedging_ratio

	def _init_logger(self):
		super(SmartHedging, self)._init_logger()

		fh = logging.FileHandler(
			f'{LOG_PATH}/hedging_{time.strftime("%Y-%m-%d")}.log'
		)
		fh.setLevel(logging.DEBUG)
		fh.setFormatter(self.formatter)
		self.logger.addHandler(fh)

		self.logger.info('Logger Initialized.')

	def _update_hedging_status(self):
		current_iuid_list = self.positions.index.values.tolist()

		if self._hedging_iuid in current_iuid_list:
			self.hedging_status = 'HEDGING'
		elif self._safe_iuid in current_iuid_list:
			self.hedging_status = 'PARKING'
		else:
			self.hedging_status = 'EMPTY'

	def run(self):
		self._record_account_info()
		self._update_hedging_status()
		SmartHedging.update_vix_info()

		current_dt = datetime.now(tz=timezone('US/Eastern'))
		current_date = current_dt.date().strftime('%Y-%m-%d')
		self.ux = VixFutures(current_date)
		vix_df = pd.read_csv(VIX_DATA_PATH)

		futures_level, vxx_entry, vxx_exit = self.monitor(current_date, vix_df)
		vix_action = self._generate_signal(futures_level, vxx_entry, vxx_exit)

		hedging_info = pd.Series(
			[futures_level, vxx_entry, vxx_exit, self.hedging_status, vix_action],
			index=['futures_level', 'entry', 'exit', 'status', 'action']
		)
		self.logger.info(f'Hedging Info:\n{hedging_info.to_string()}')

		if vix_action == 'NONE':
			self.logger.info('No trading signal.')
		else:
			target_position = self.make_orders(vix_action)
			self.logger.info(f'Target Positions:\n{target_position}')
			self.place_orders(target_position)
			self.logger.info(f'Target position file uploaded to OSS.')

	def make_orders(self, direction):
		assert direction in ['BUY', 'SELL']

		iuids = [self._safe_iuid, self._hedging_iuid]
		intrinio_symbols = [s.split('_')[2] + '.NB' for s in iuids]
		realtime_data = get_data_intrinio(intrinio_symbols)
		# realtime_data.columns = iuids

		if direction == 'BUY':
			sell_ticker = self._safe_iuid
			buy_ticker = self._hedging_iuid
		elif direction == 'SELL':
			sell_ticker = self._hedging_iuid
			buy_ticker = self._safe_iuid

		ask_price = realtime_data.loc['ask_price_4d', buy_ticker] / 10000.
		if ask_price < 0.01:
			ask_price = realtime_data.loc['last_price_4d', buy_ticker] / 10000.
		bid_price = realtime_data.loc['bid_price_4d', sell_ticker] / 10000.
		if bid_price < 0.01:
			bid_price = realtime_data.loc['last_price_4d', sell_ticker] / 10000.

		current_position = self.positions.loc[sell_ticker, 'holdingPosition']
		sell_quantity = min(
			self.portfolio_value * self.hedging_ratio // bid_price,
			current_position
		)
		buy_quantity = sell_quantity * bid_price // ask_price

		order_position_info =[
			[sell_ticker, -sell_quantity],
			[buy_ticker, buy_quantity]
		]
		order_position = pd.DataFrame(
			order_position_info,
			columns=['iuid', 'target_position']
		).set_index('iuid')

		old_position = self.positions.rename(columns={'holdingPosition': 'target_position'})
		target_position = order_position.add(old_position, fill_value=0.)
		target_position['symbol'] = np.nan
		target_position['target_weight'] = np.nan
		target_position = target_position[['symbol', 'target_weight', 'target_position']]

		return target_position

	def place_orders(self, positions):
		file_name = f'{time.strftime("%Y%m%d%H%M")}.csv'
		local_path = f'{LOG_PATH}/{file_name}'
		positions.to_csv(local_path)
		oss_path = self.oss_key + file_name
		oss.upload_file(local_path, oss_path)

	def monitor(self, current_date, df):
		self.ux.fetch_data_from_bbg(current_date, current_date)

		ratio = float(self.ux.M1_expiration) / self.ux.contract_life
		futures_level = ratio * self.ux.M1_data + (1. - ratio) * self.ux.M2_data

		temp_file = df.iloc[-VIX_LOOKBACK_WINDOW:]
		vxx_entry = np.percentile(temp_file['Simulated Index'].values, self.entry_pctl)
		vxx_exit = np.percentile(temp_file['Simulated Index'].values, self.exit_pctl)

		return futures_level, vxx_entry, vxx_exit

	def _generate_signal(self, level, entry_th, exit_th) -> str:
		assert self.hedging_status in ['EMPTY', 'HEDGING', 'PARKING']

		if level < entry_th and self.hedging_status != 'HEDGING':
			return 'BUY'
		elif level > exit_th and self.hedging_status != 'PARKING':
			return 'SELL'
		else:
			return 'NONE'

	@classmethod
	def update_vix_info(cls):
		vix_df = pd.read_csv(
			VIX_DATA_PATH,
			index_col=0,
			parse_dates=True
		)
		last_record_dt = vix_df.index.values[-1].astype('datetime64[D]')
		current_dt = np.datetime64(datetime.now()).astype('datetime64[D]')
		bus_day_count = np.busday_count(last_record_dt, current_dt)

		if bus_day_count != 0:
			data_record = []
			for i in range(1, bus_day_count + 1):
				# tmp_dt = last_record_dt + np.timedelta64(1, 'D')
				tmp_dt = np.busday_offset(last_record_dt, i, roll='forward')
				last_record_dt_str = last_record_dt.astype('str')
				tmp_dt_str = tmp_dt.astype('str')
				vxx = VixFutures(tmp_dt_str)
				vxx.fetch_data_from_bbg(last_record_dt_str, tmp_dt_str)

				ratio = float(vxx.M1_expiration) / vxx.contract_life
				futures_level = ratio * vxx.M1_data + (1. - ratio) * vxx.M2_data
				update_df = pd.DataFrame(
					data=np.array([futures_level, vxx.M1_data, vxx.M2_data]),
					columns=[tmp_dt],
					index=vix_df.columns.values
				).T
				vix_df = vix_df.append(update_df)

			vix_df.to_csv(VIX_DATA_PATH)
			print('Successfully updating VIX info.')
		else:
			print('VIX info already updated.')


if __name__ == '__main__':
	SmartHedging.update_vix_info()

	sh = SmartHedging(
		account='U3245801',
		currency='USD',
		region='US',
		oss_key='sh',
		hedging_status='PARKING',
		hedging_asset='VXX',
		safe_asset='JPST',
		entry_pctl=40,
		exit_pctl=96,
		hedging_ratio=0.05
	)

	sh.run()

