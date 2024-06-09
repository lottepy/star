# from utils.bbg_downloader import download_his
from lib.commonalgo.data.bbg_downloader import download_his
import numpy as np
import pandas as pd
import os
import csv
from datetime import datetime, date, timedelta


MONTH_SHORT = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
TICKER_LIST = ['UX' + m + '8 Index' for m in MONTH_SHORT]

class FutureCSV(object):
	def __init__(self,filename = None):
		if filename:
			self.filename = filename
		else:
			self.filename = '../../../data/vix-futures/vix-futures.csv'
		self.headers = [
			'Date',
			'Simulated Index',
			'M1',
			'M2',
		]
		self.writer = None
		self.init_writer()

	def init_writer(self):
		if not os.path.isfile(self.filename):
			with open(self.filename, 'a') as f:
				writer = csv.writer(f)
				writer.writerow(self.headers)

	def write_futures(self, data):
		with open(self.filename, 'a') as f:
			writer = csv.writer(f)
			writer.writerows(data)


class VixFutures(object):
	def __init__(self, base_date):
		self.base_date = base_date
		self.before_third_wed = True
		self.base_dt = None
		self.current_month = 1
		self.third_wednesday = None
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

	def get_third_wed(self):
		self.base_dt = datetime.strptime(self.base_date, '%Y-%m-%d')
		self.current_month = self.base_dt.month
		reference_dt = date(self.base_dt.year, self.base_dt.month, 4)
		offset = reference_dt.weekday() + 2
		if offset == 8:
			offset = 1

		self.third_wednesday = date(self.base_dt.year, self.base_dt.month, 22) + timedelta(-offset)

		if self.base_dt.date() <= self.third_wednesday:
			self.before_third_wed = True
			self.next_expire_dt = self.third_wednesday
			self.M1_expiration = np.busday_count(self.base_dt.date(), self.next_expire_dt)

			if self.base_dt.month == 1:
				last_reference_dt = date(self.base_dt.year - 1, 12, 4)
			else:
				last_reference_dt = date(self.base_dt.year, self.base_dt.month - 1, 4)
			last_offset = last_reference_dt.weekday() + 2
			if last_offset == 8:
				last_offset = 1
			self.last_expire_dt = date(last_reference_dt.year, last_reference_dt.month, 22) + timedelta(-last_offset)
			self.contract_life = np.busday_count(self.last_expire_dt, self.next_expire_dt) - 1
		else:
			self.before_third_wed = False
			if self.base_dt.month == 12:
				next_reference_dt = date(self.base_dt.year + 1, 1, 4)
			else:
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

	def get_contract_ticker(self):
		if self.before_third_wed:
			self.M1 = TICKER_LIST[self.current_month - 1]
			self.M2 = TICKER_LIST[self.current_month]
		else:
			self.M1 = TICKER_LIST[self.current_month]
			self.M2 = TICKER_LIST[self.current_month + 1]

	def fetch_data(self, start, end):
		self.agg_data = download_his([self.M1, self.M2], ['PX_LAST'], start, end, currency='USD', period="DAILY")
		self.agg_data.columns = self.agg_data.columns.get_level_values(0)
		self.M1_data = self.agg_data[self.M1].iloc[-1]
		self.M2_data = self.agg_data[self.M2].iloc[-1]

	def fetch_alldata(self, start, end):
		self.agg_data = download_his([self.M1, self.M2], ['PX_LAST'], start, end, currency='USD', period="DAILY")
		self.agg_data.columns = self.agg_data.columns.get_level_values(0)
		return self.agg_data

def update_futurecsv(filename = None):
	file_updater = FutureCSV(filename)
	exit_data = pd.read_csv(file_updater.filename,
	                        index_col=0, infer_datetime_format=True, parse_dates=True)
	last_date = exit_data.index[-1].date()
	end = datetime.today().date()
	if end > last_date:
		start = last_date + timedelta(days=1)
		vxx = VixFutures(end.strftime('%Y-%m-%d'))
		vxx.get_third_wed()
		vxx.get_contract_ticker()
		update_data = vxx.fetch_alldata(start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
		valid_daterange = [x.date() for x in list(update_data.index)]
		data = []
		for sim_date in valid_daterange:
			start_date = sim_date - timedelta(days=5)
			VixFutures(sim_date.strftime('%Y-%m-%d'))
			vxx.get_third_wed()
			vxx.get_contract_ticker()
			vxx.fetch_data(start_date.strftime('%Y-%m-%d'), sim_date.strftime('%Y-%m-%d'))
			ratio = float(vxx.M1_expiration) / vxx.contract_life
			futures_level = ratio * vxx.M1_data + (1. - ratio) * vxx.M2_data
			# data[sim_date] = [futures_level, vxx.M1_data, vxx.M2_data]
			row = [
				'{d.year}/{d.month}/{d.day}'.format(d=sim_date),
				"{0:.6f}".format(futures_level),
				"{0:.3f}".format(vxx.M1_data),
				"{0:.3f}".format(vxx.M2_data),
			]
			data.append(row)
		file_updater.write_futures(data)

