from lib.commonalgo.data.bbg_downloader import download_his
import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta
from lib.commonalgo.data.factset_client import get_data


MONTH_SHORT = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
BBG_TICKER_LIST = ['UX' + m + '9 Index' for m in MONTH_SHORT]
FACTSET_TICKER_LIST = ['VX' + m + '19-CBF' for m in MONTH_SHORT]


class VixFutures(object):
	def __init__(self, base_date):
		self.base_date = base_date
		self.before_third_wed = True
		self.base_dt = None
		self.current_month = 1
		self._third_wednesday = None
		self.M1 = ''
		self.M2 = ''
		self.agg_data = None
		self.M1_data = 0.0
		self.M2_data = 0.0
		self.M1_expiration = 0.0
		self.M2_expiration = 0.0
		self.contract_life = 0.0
		self.next_expire_dt = None
		self.last_expire_dt = None
		self._get_third_wed()
		self._get_contract_ticker()

	def _get_third_wed(self):
		self.base_dt = datetime.strptime(self.base_date, '%Y-%m-%d')
		self.current_month = self.base_dt.month
		reference_dt = date(self.base_dt.year, self.base_dt.month, 4)
		offset = reference_dt.weekday() + 2
		if offset == 8:
			offset = 1

		self._third_wednesday = date(self.base_dt.year, self.base_dt.month, 22) + timedelta(-offset)

		if self.base_dt.date() <= self._third_wednesday:
			self.before_third_wed = True
			self.next_expire_dt = self._third_wednesday
			self.M1_expiration = np.busday_count(self.base_dt.date(), self.next_expire_dt)

			last_reference_dt = date(self.base_dt.year, self.base_dt.month - 1, 4)
			last_offset = last_reference_dt.weekday() + 2
			if last_offset == 8:
				last_offset = 1
			self.last_expire_dt = date(last_reference_dt.year, last_reference_dt.month, 22) + timedelta(-last_offset)
			self.contract_life = np.busday_count(self.last_expire_dt, self.next_expire_dt) - 1
		else:
			self.before_third_wed = False
			next_reference_dt = date(self.base_dt.year, self.base_dt.month + 1, 4)
			next_offset = next_reference_dt.weekday() + 2
			if next_offset == 8:
				next_offset = 1
			self.next_expire_dt = date(next_reference_dt.year, next_reference_dt.month, 22) + timedelta(-next_offset)
			self.M1_expiration = np.busday_count(self.base_dt.date(), self.next_expire_dt)

			last_reference_dt = date(self.base_dt.year, self.base_dt.month, 4)
			last_offset = last_reference_dt.weekday() + 2
			if last_offset == 8:
				last_offset = 1
			self.last_expire_dt = date(last_reference_dt.year, last_reference_dt.month, 22) + timedelta(-last_offset)
			self.contract_life = np.busday_count(self.last_expire_dt, self.next_expire_dt) - 1

	def _get_contract_ticker(self):
		if self.before_third_wed:
			self.M1 = {
				'factset': FACTSET_TICKER_LIST[self.current_month - 1],
				# 'bbg': BBG_TICKER_LIST[self.current_month - 1]
				'bbg': 'UX1 Index'
			}
			self.M2 = {
				'factset': FACTSET_TICKER_LIST[self.current_month],
				# 'bbg': BBG_TICKER_LIST[self.current_month]
				'bbg': 'UX2 Index'
			}
		else:
			self.M1 = {
				'factset': FACTSET_TICKER_LIST[self.current_month],
				# 'bbg': BBG_TICKER_LIST[self.current_month]
				'bbg': 'UX1 Index'
			}
			self.M2 = {
				'factset': FACTSET_TICKER_LIST[self.current_month + 1],
				# 'bbg': BBG_TICKER_LIST[self.current_month + 1]
				'bbg': 'UX2 Index'
			}

	def fetch_data_from_bbg(self, start, end):
		self.agg_data = download_his(
			[self.M1['bbg'], self.M2['bbg']],
			['PX_LAST'],
			start,
			end,
			currency='USD',
			period="DAILY"
		)
		self.agg_data.columns = self.agg_data.columns.get_level_values(0)
		self.M1_data = self.agg_data[self.M1['bbg']].iloc[-1]
		self.M2_data = self.agg_data[self.M2['bbg']].iloc[-1]

	def fetch_data_from_factset(self):
		self.agg_data = get_data([self.M1['factset'], self.M2['factset']]).loc['LAST_1']
		self.M1_data = float(self.agg_data[self.M1['factset']])
		self.M2_data = float(self.agg_data[self.M2['factset']])

	@property
	def third_wednesday(self):
		return self._third_wednesday


if __name__ == "__main__":
	vxx = VixFutures("2019-10-28")
	print(vxx.third_wednesday)
	print(vxx.next_expire_dt, vxx.last_expire_dt, vxx.contract_life, vxx.M1_expiration)
	print(vxx.M1, vxx.M2)
	# vxx.fetch_data_from_factset()
	# print(vxx.M1_data, vxx.M2_data)
	vxx.fetch_data_from_bbg('2019-10-01', '2019-10-28')
	print(vxx.agg_data)

