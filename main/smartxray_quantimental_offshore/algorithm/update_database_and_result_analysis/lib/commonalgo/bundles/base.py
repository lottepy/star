import os

import pandas as pd

# from zipline.data.bundles import register
from ..zipline_patch.markort import markort_equities
from ..zipline_patch.csvdata import csvaqm_equities
from ..zipline_patch.calendars.aqm_calendar_utils import register_aqm_calendars
from zipline.data.bundles.core import ingest, register

register_aqm_calendars()

class BaseBundle(object):
	def __init__(self, bundle_name,
	             calendar ='AQMUS',
	             start = pd.Timestamp('2000-01-04', tz='UTC')):
		self.name = bundle_name
		self.calendar = calendar
		self.start = start

	def register_bundle(self,fromcsv=False, summaryfolder = None, prefillcsvfolder=None):
		symbols_path = os.path.join(summaryfolder, 'Summary.csv')
		summary = pd.read_csv(symbols_path)
		symbols = tuple(summary['Name'].tolist())
		if fromcsv:
			register(
				self.name,
				csvaqm_equities(
					tframes = ['daily'],
					csvdir = summaryfolder),
				calendar_name=self.calendar,
				start_session=self.start)
		else:
			register(
				self.name,
				markort_equities(symbols, prefillcsvfolder),
				calendar_name=self.calendar,
				start_session=self.start)
		print ('Data bundle <{}> registration complete'.format(self.name))

	def update_bundle(self,fromcsv = False, summaryfolder = None, prefillcsvfolder=None):
		self.register_bundle(fromcsv,summaryfolder,prefillcsvfolder)
		ingest(self.name)
		print ('Data bundle <{}> update complete'.format(self.name))


class FuturesBundle(BaseBundle):
	def register_bundle(self, fromcsv=False, summaryfolder=None, freq='daily'):
		symbols_path = os.path.join(summaryfolder, 'Summary.csv')
		summary = pd.read_csv(symbols_path)
		symbols = tuple(summary['Name'].tolist())
		all_minutes = 1440
		if freq == 'daily':
			all_minutes = 390
		if fromcsv:
			register(
				self.name,
				csvaqm_equities(
					tframes = [freq],
					csvdir = summaryfolder),
				calendar_name=self.calendar,
				start_session=self.start,
				minutes_per_day=all_minutes
			)
		else:
			print ('Futures Data Bundle only accepts csv ingesting.')
		print ('Data bundle <{}> registration complete'.format(self.name))

	def update_bundle(self, fromcsv=False, summaryfolder=None, freq='daily'):
		self.register_bundle(fromcsv, summaryfolder, freq)
		ingest(self.name)
		print ('Data bundle <{}> update complete'.format(self.name))