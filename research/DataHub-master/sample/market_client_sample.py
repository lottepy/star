# -*- coding: utf-8 -*-
# https://github.com/magnumwm/py-market-client

from market_client.client import init_client
from lib.commonalgo.setting.network_utils import MARKET_EP


from market_client import marketlib

market_client = init_client(end_point=MARKET_EP, regions='HK', categories='10')

# res = market_client.snapshot_data(iuids=['CN_60_IF1905', 'CN_60_RB1905','HK_10_700'])
# res = market_client.snapshot_data(iuids=['CN_10_600001','HK_10_700'])


# def is_biz_day(self, date: str, region: str)-> bool
# 	"""
# 		判断是否交易日：
# 		:param date: 日期. yyyy-mm-dd
# 		:param region: 交易所所在地区
# 		:return: bool
# 	"""
# Example
date = '2018-12-17'
region = 'HK'
res = market_client.is_biz_day(date=date, region=region)	# True or False
# def get_biz_day(self, date: str, region: str, days_offset=1)-> str
# 	"""
# 		获取距离date的下X个交易日日期， x为负数的时候则获取前X个交易日日期
# 		:param date: 日期. yyyy-mm-dd
# 		:param region: 交易所所在地区
# 		:param days_offset: 默认为T+1，T-x，T+x都支持
# 		:return:  T + day_offset 日期，yyyy-mm-dd
# 	"""
# Example
date = '2018-12-17'
region = 'HK'
res = market_client.get_biz_day(date=date, region=region, days_offset=1)
# res = '2018-12-18'

print(market_client.get_biz_day(date=date, region=region, days_offset=2))
# res = '2018-12-19'

print(market_client.get_biz_day(date=date, region=region, days_offset=-2))
# res = '2018-12-13'
# def validate_iuids(self, iuids: list)-> list
# 	"""
# 		IUID验证
# 		:param iuids: iuid列表
# 		:return: valid(bool[]): 按顺序，每个iuid是否合法
# 	"""
# Example
res = market_client.validate_iuids(iuids=['AE_10_ADCB', 'US_10_IEFA', 'FALSE_IUID'])
# res = [True, True, False]
# def query_iuid(self, **kwargs)-> dict
# 	"""
# 		IUID查询
# 		:param region: 选填，地区
# 		:param category: 选填，标的类别
# 		:param ticker: 选填，标的在交易所的ticker id
# 		:return: 返回完整标的数据
# 	"""
# Example
# iuid exist
# res = market_client.query_iuid(region='US', category='10', ticker='IEFA')
res = market_client.query_iuid(region='CN', category='60', ticker='IH1907')
# res = {'US_10_IEFA': {'category': '10', 'sync_tags': 0, 'flag': 17, 'name': 'ISHARES CORE MSCI EAFE ETF', 'ticker': 'IEFA', 'region': 'US', 'exchange': 'US', 'currency': 'USD', 'lot_size': '1.000000', 'management_fee_rate': '0.0000', 'extra_data': {'underlying_index': 'MIMUEAFN'}, 'market_group': 0, 'sector': 0, 'trading_start_time': '14:30:00', 'trading_end_time': '21:30:00', 'multiplier': '1.00', 'inception_day': None, 'expire_day': None, 'local_symbol': '', 'ib_destination': '', 'status': 2}}

# iuid not exist
res = market_client.query_iuid(region='1', category='10', ticker='IEFA')
# res = {'1_10_IEFA': None}
# def transfer_broker_local_symbol(self, iuids: list, broker_id: int)-> list
# 		"""
# 		broker local symbol转换,返回ticker
# 		:param iuids: iuid列表
# 		:param broker_id: 券商代码
# 		:return: 券商交易时使用的symbol
# 		"""
# Example
res = market_client.transfer_broker_local_symbol(['US_10_IEFA'], broker_id=1)
# res = ['IEFA']
# def historical_quotes(self, **kwargs)-> dict
# 		"""
# 		获取iuids历史行情数据
# 		:param iuids:
# 		:param tags:
# 		:param period:
# 		:param start_date:
# 		:param end_date:
# 		:param adjust_type:
# 		:param tz_region: defaults to instrument region
# 		:param currency: defaults to instrument currency
# 		:param prelisting_fill:
# 		:param fill:
# 		:param precision:
# 		:param ordering:
# 		:param as_json:
# 		:param with_ts: return min and max timestamp if true
# 		:return:
# 		"""
# Example
# res = market_client.historical_quotes(iuids=['US_10_VEA','US_10_VWO','US_10_SCHF','US_10_VTI'],tags=[4], period='D', start_date_ts=1543203902953, end_date_ts=1545028974000)
# res = market_client.historical_quotes(iuids=['CN_60_cu1905','CN_60_rb1905'],tags=[4], period='D', start_date_ts=1543203902953, end_date_ts=1545028974000)
# res = market_client.historical_quotes(iuids=['HK_10_1','HK_10_5'],tags=[4], period='D', start_date_ts=1488326400000, end_date_ts=1551312000000,adjust_type=0)
# res = market_client.historical_quotes(iuids=['CN_10_600000','HK_10_5'],tags=[4], period='D', start_date_ts=1488326400000, end_date_ts=1551312000000,adjust_type=0)
res = market_client.historical_quotes(iuids=['HK_10_27'],tags=[4], period='D', start_date='2017-01-01', end_date='2019-07-22',adjust_type=0)
print(res)
# res = {'US_10_IEFA.4': [['2018-12-14', '56.6500'], ['2018-12-13', '57.4000'], ['2018-12-12', '57.5300'], ['2018-12-11', '56.7100'], ['2018-12-10', '56.4800'], ['2018-12-07', '57.0400'], ['2018-12-06', '57.6100'], ['2018-12-04', '58.1400'], ['2018-12-03', '59.7200'], ['2018-11-30', '58.9800'], ['2018-11-29', '59.1700'], ['2018-11-28', '59.4900'], ['2018-11-27', '58.6300'], ['2018-11-26', '58.8300']]}
def historical_events(self, iuids: list, start_time=None, end_time=None):
		"""
		:param iuids: uid列表
		:param start_time: 时间戳（ms）
		:param end_time: 时间戳（ms）
		:return:
		"""
# example
# res = market_client.historical_events(iuids=['HK_10_934'], start_time=1514736000000, end_time=1547049600000)
# res = market_client.historical_events(iuids=['GB_10_ERND'], start_time=1514736000000, end_time=1551928909179)
res = market_client.historical_events(iuids=['HK_10_27'], start_time=1483200000000, end_time=1563206400000)
# res = {'HK_10_3115': []}
# def snapshot_data(self, iuids: list)->dict
# 		"""
# 		返回标的iuid最新实时snapshot数据
# 		:param iuids:
# 		:return:
# 		"""
# Example
res = market_client.snapshot_data(iuids=['AE_10_ADCB', 'US_10_IEFA'])
# res = {'GB_40_USDHKD': {'iuid': 'GB_40_USDHKD', 'currency': '', 'price': '7.830300', 'b1': '7.830200', 'bv1': 'nan', 'a1': '7.830500', 'av1': 'nan', 'price_type': 5, 'provider_id': 0, 'provider_timestamp': 1545989585000, 'timestamp': 1545989722330}}
# def fx(self, from_currency: str, to_currency: str)->Decimal
# 		"""
# 		汇率
# 		:param from_currency:
# 		:param to_currency:
# 		:return:
# 		"""
# Example
res = market_client.fx('CNY', 'USD')
# res = 0.145548
def real_time_data(self, iuid_list, orderbook=False, broker_id=None, account_id=None):
		"""
		订阅实时的数据：
		subscribe the info (i.e.., orderbook, tick, trading.order, trading.execution) of each iuid automatically
		"""
# Example
import time
market_client.real_time_data(iuid_list=['GB_40_USDJPY'])
while True:
	print('tick_data: {0}'.format(market_client.tick_data))
	#   如下可供选择
	#   market_client.orderbook_data
	#   market_client.tick_data
	#   market_client.trading_order_data
	#   market_client.trading_execution_data
	time.sleep(10)

# Example
import time
# market_client.real_time_data(iuid_list=['GB_40_USDJPY','HK_10_700','HK_10_5'])
market_client.real_time_data(iuid_list=['HK_10_700','HK_10_5'])
while True:
	print('tick_data: {0}'.format(market_client.tick_data))
	print('orderbook_data: {0}'.format(market_client.orderbook_data))
	#   如下可供选择
	#   market_client.orderbook_data
	#   market_client.tick_data
	#   market_client.trading_order_data
	#   market_client.trading_execution_data
	time.sleep(2)