import os
import sys

from os.path import dirname, abspath
root_path = dirname(dirname(dirname(dirname(abspath(__file__)))))
if root_path not in sys.path:
	sys.path.insert(1, root_path)

from logbook import Logger, StreamHandler

import numpy as np
import pandas as pd
from pandas import DataFrame, read_csv, Index, Timedelta, NaT
import requests
import datetime
import pytz

from zipline.utils.cli import maybe_show_progress
from zipline.data.bundles import core as bundles
from ..setting.iuid_mapping import get_iuid_from_symbol
from ..setting.constants import REGION_CURRENCY_MAP
import algorithm.addpath as addpath

handler = StreamHandler(sys.stdout, format_string=" | {record.message}")
logger = Logger(__name__)
logger.handlers.append(handler)

_session = requests.session()
CONCURRENCY = 8
BATCH_SIZE = 4


def retrieve_data(params):
	endpoint = 'https://market.aqumon.com/v1/market'
	resp = _session.get(endpoint + '/historical/quotes', params=params)
	return resp.json()


def get_data(iuids, tags, period, start_date=None, end_date=None, start_date_ts=None,
             end_date_ts=None, adjust_type=0, tz_region=None, currency=None, prelisting_fill=False,
             fill='nan', precision=4, ordering='A', as_json=False, with_ts=False):
	"""
	:param iuids:
	:param tags:
	:param period:
	:param start_date:
	:param end_date:
	:param adjust_type:
	:param tz_region: defaults to instrument region
	:param currency: defaults to instrument currency
	:param prelisting_fill:
	:param fill:
	:param precision:
	:param ordering:
	:param as_json:
	:param with_ts: return min and max timestamp if true
	:return:
	"""
	template = {
		'iuids': '',
		'tags': ','.join(map(str, tags)),
		'period': period,
		'start_date': start_date,
		'start_date_ts': start_date_ts,
		'end_date': end_date,
		'end_date_ts': end_date_ts,
		'adjust_type': adjust_type,
		'tz_region': tz_region,
		'currency': currency,
		'fill': fill,
		'precision': precision,
		'ordering': ordering,
		'format': 'json',
		'prelisting_fill': prelisting_fill,
		'with_ts': with_ts
	}
	iuid_batch = [iuids[i:i + BATCH_SIZE] for i in range(0, len(iuids), BATCH_SIZE)]
	for batch in iuid_batch:
		params = template.copy()
		params['iuids'] = ','.join(batch)
		resp = retrieve_data(params)

	if not resp:
		return resp
	if as_json:
		return resp
	df_data = {}
	if with_ts:
		timestamps = {}
		for key, value in resp.items():
			ar = np.array(value)
			key = tuple(key.split('.'))
			df_data[key] = pd.Series(ar[:, 2], index=ar[:, 0])
			ts = list(filter(lambda v: v != 'nan', ar[:, 1]))
			timestamps['max'] = max(timestamps.setdefault('max', int(ts[-1])), int(ts[-1]))
			timestamps['min'] = min(timestamps.setdefault('min', int(ts[-1])), int(ts[-1]))
		df = pd.DataFrame.from_dict(df_data)
		return df.astype('float'), timestamps
	for key, value in resp.items():
		ar = np.array(value)
		key = tuple(key.split('.'))
		df_data[key] = pd.Series(ar[:, 1], index=ar[:, 0])
	df = pd.DataFrame.from_dict(df_data)
	return df


def fetch_markort_data(
		symbol=None,
		start=None,
		end=None,
		calendar=None,
		prefillcsvfolder=None):
	whole_price = pd.DataFrame({'date': [np.nan]}).dropna()
	aqm_iuid = get_iuid_from_symbol(symbol)
	region, asset_type, sym_market = aqm_iuid.split('_')
	curr_field = REGION_CURRENCY_MAP.get(region, None)
	if asset_type == '10' and region == 'CN':
		# CN COMMON STOCK
		price_field = 8
		cap_field = 7  # STOCK MARKET CAP
	elif asset_type == '10':
		# US or HK ETF
		price_field = 4
		cap_field = 14  # TRACKING INDEX MARKET CAP
	elif asset_type == '20' and region == 'CN':
		# CN MUTUAL FUND
		price_field = 52
		cap_field = 14
	elif asset_type == '30':
		# INDEX
		price_field = 4
		cap_field = 7  # INDEX MARKET CAP

	utc0 = pd.Timestamp('2000-01-01', tz = 'UTC').to_pydatetime()
	end0 = (pd.Timestamp.today(tz='UTC') + Timedelta(days =1)).to_pydatetime()
	if start:
		start = max(start, utc0)
		start_date = start.strftime('%Y-%m-%d')
	else:
		start_date = utc0.strftime('%Y-%m-%d')
	if end:
		end = min(end, end0)
		end_date = end.strftime('%Y-%m-%d')
	else:
		end_date = end0.strftime('%Y-%m-%d')

	data_df = get_data([aqm_iuid], [price_field, cap_field],
	                   'D', start_date, end_date, adjust_type=1, tz_region=None, currency=curr_field,
	                   fill='nan', precision=4, prelisting_fill=True)
	whole_price['date'] = data_df.index
	whole_price.set_index('date', inplace=True)
	whole_price['close'] = data_df[(aqm_iuid, str(price_field))].values.astype('float')
	whole_price['volume'] = data_df[(aqm_iuid, str(cap_field))].values.astype('float')
	whole_price.index = pd.to_datetime(whole_price.index, utc=False)
	whole_price = whole_price.asfreq(freq='B', method='ffill', normalize=True)

	# zero value
	if symbol == '601811.SH':
		whole_price = whole_price.replace('0.0000', np.NaN)

	# NA value
	whole_price.fillna(method='ffill', inplace=True)
	if symbol == 'ASHR':
		whole_price.fillna(method='bfill', inplace=True)

	whole_price['open'] = whole_price ['low'] = whole_price['high'] = whole_price['close']

	cal_list = pd.Series(index=calendar.adhoc_holidays).tz_localize(None).index.tolist()
	whole_price = whole_price.loc[(set(whole_price.index) - set(cal_list))].sort_index()

	# Allow user to use csv data to prefill
	if prefillcsvfolder is not None:
		loaded_csv_data = pd.read_csv("{}/{}.csv".format(prefillcsvfolder, symbol),
									  index_col='date',
									  usecols=['date', 'close', 'volume', 'open', 'low', 'high'])
		loaded_csv_data.index = pd.to_datetime(loaded_csv_data.index)

		# 如果用来prefill的csv文件为空，则表明用户无意fill改symbol的历史数据
		# Lance：2019年8月6日14:33:08
		if loaded_csv_data.empty:
			pass
		else:
			close_first_valid_index = whole_price.close.first_valid_index()
			volume_first_valid_index = whole_price.volume.first_valid_index()

			tofill_close_csv_data = loaded_csv_data.loc[:close_first_valid_index, :].copy()
			tofill_volume_csv_data = loaded_csv_data.loc[:volume_first_valid_index, :].copy()

			close_multiple = whole_price.loc[close_first_valid_index,'close'] / tofill_close_csv_data.loc[close_first_valid_index,'close']
			tofill_close_csv_data.loc[:, 'close'] = tofill_close_csv_data.loc[:, 'close'] * close_multiple
			vol_multiple = whole_price.loc[volume_first_valid_index, 'volume'] / tofill_volume_csv_data.loc[volume_first_valid_index, 'volume']
			tofill_volume_csv_data.loc[:, 'volume'] = tofill_volume_csv_data.loc[:, 'volume'] * vol_multiple

			combined_csv = whole_price.copy()
			combined_csv.loc[combined_csv.index.isin(tofill_close_csv_data.index), 'close'] = tofill_close_csv_data.loc[:, 'close']
			combined_csv.loc[combined_csv.index.isin(tofill_volume_csv_data.index), 'volume'] = tofill_volume_csv_data.loc[:, 'volume']
			whole_price = combined_csv.copy()

	csv_data = whole_price
	csv_data['dividend'] = 0.0
	csv_data['split'] = 1.0
	fname = symbol + '.csv'
	csv_data.to_csv(addpath.data_path + "\\bundles\\" + fname)

	return whole_price

def markort_equities(symbols = None, prefillcsvfolder=None):

	return MarkortData(symbols, prefillcsvfolder).ingest



class MarkortData:
	"""Create a data bundle ingest function from a set of symbols loaded from AQUMON Markort Service
	Notes
	-----
	The sids for each symbol will be the index into the symbols sequence.
	"""

	# strict this in memory so that we can reiterate over it
	def __init__(self, symbols, prefillcsvfolder=None, start=None, end=None):
		self.symbols = tuple(symbols)
		self.prefillcsvfolder = prefillcsvfolder

	def ingest(self,
	           environ,
	           asset_db_writer,
	           minute_bar_writer,
	           daily_bar_writer,
	           adjustment_writer,
	           calendar,
	           start_session,
	           end_session,
	           cache,
	           show_progress,
	           output_dir):
		markort_bundle(environ,
		               asset_db_writer,
		               minute_bar_writer,
		               daily_bar_writer,
		               adjustment_writer,
		               calendar,
		               start_session,
		               end_session,
		               cache,
		               show_progress,
		               output_dir,
		               symbols=self.symbols,
					   prefillcsvfolder=self.prefillcsvfolder)


@bundles.register("markort")
def markort_bundle(environ,
                   asset_db_writer,
                   minute_bar_writer,
                   daily_bar_writer,
                   adjustment_writer,
                   calendar,
                   start_session,
                   end_session,
                   cache,
                   show_progress,
                   output_dir,
                   symbols=[],
				   prefillcsvfolder=None):
	"""
	Build a zipline data bundle from the aqumon markort data api by IT.
	"""

	divs_splits = {'divs': DataFrame(columns=['sid', 'amount',
	                                          'ex_date', 'record_date',
	                                          'declared_date', 'pay_date']),
	               'splits': DataFrame(columns=['sid', 'ratio',
	                                            'effective_date'])}

	dtype = [('start_date', 'datetime64[ns]'),
	         ('end_date', 'datetime64[ns]'),
	         ('auto_close_date', 'datetime64[ns]'),
	         ('symbol', 'object')]
	metadata = DataFrame(np.empty(len(symbols), dtype=dtype))

	# writer = minute_bar_writer
	writer = daily_bar_writer

	writer.write(_pricing_iter(symbols, metadata,
	                           show_progress,
	                           calendar=calendar, cache=cache,
	                           start=start_session.to_pydatetime(),
	                           end=end_session.to_pydatetime(),
							   prefillcsvfolder=prefillcsvfolder),
	             show_progress=show_progress,
				 )

	# Hardcode the exchange to "MAKROT" for all assets and (elsewhere)
	metadata['exchange'] = "MARKORT"

	asset_db_writer.write(equities=metadata)

	divs_splits['divs']['sid'] = 0.0
	divs_splits['splits']['sid'] = 1.0
	adjustment_writer.write(splits=divs_splits['splits'],
	                        dividends=divs_splits['divs'])


def _pricing_iter(symbols, metadata, show_progress,
                  calendar=None, cache=None, start=None, end=None, prefillcsvfolder=None):
	with maybe_show_progress(symbols, show_progress,
	                         label='Loading custom pricing data: ') as it:
		for sid, symbol in enumerate(it):
			logger.debug('%s: sid %s' % (symbol, sid))

			# try:
			# 	data = cache['markort']
			# except KeyError:
			data = cache['markort'] = fetch_markort_data(symbol, start, end, calendar, prefillcsvfolder=prefillcsvfolder).sort_index()

			start_date = data.index[0]
			end_date = data.index[-1]

			# The auto_close date is the day after the last trade.
			ac_date = end_date + Timedelta(days=1)
			metadata.iloc[sid] = start_date, end_date, ac_date, symbol

			yield sid, data
