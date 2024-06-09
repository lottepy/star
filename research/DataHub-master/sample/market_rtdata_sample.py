# -*- coding: utf-8 -*-
# https://github.com/magnumwm/py-market-client

from market_client.client import init_client
from lib.commonalgo.setting.network_utils import MARKET_EP

mc = init_client(end_point=MARKET_EP, regions='HK', categories='10')
# mc = init_client(end_point='https://market.aqumon.com', regions='HK', categories='10')

# Example
import time
# market_client.real_time_data(iuid_list=['GB_40_USDCNY'])
# mc.real_time_data(iuid_list=['HK_60_MHIQ9','HK_60_MHIN9'],orderbook=True)
# market_client.real_time_data(iuid_list=['CN_60_cu1905','CN_60_rb1905'])
mc.real_time_data(iuid_list=['HK_10_2822', 'HK_10_5', 'HK_10_700'], orderbook=True)
# mc.real_time_data(iuid_list=['CN_10_000001','CN_10_600000','CN_10_000002'],orderbook=True)
# mc.real_time_data(iuid_list=['CN_10_000001_full','CN_10_600000_full','CN_10_000002_full'],orderbook=True)
while True:
	print('tick_data: {0}'.format(mc.tick_data))
	print('orderbook_data: {0}'.format(mc.orderbook_data))
	#   如下可供选择
	#   market_client.orderbook_data
	#   market_client.tick_data
	#   market_client.trading_order_data
	#   market_client.trading_execution_data
	time.sleep(2)