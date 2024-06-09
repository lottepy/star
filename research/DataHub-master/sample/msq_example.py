from market_consumer_client import MarketConsumerClient
import sys
import json
import time
from lib.commonalgo.execution.trading_controller import TradingController
from lib.commonalgo.data.factset_client import get_data

trading = TradingController()

test_account = {
	'broker':'2',
	'acc': '111111'
}

paper1 = {
	'broker':'1',
	'account': 'DU1161136'
}

# paper1 = {
# 	'broker':'1',
# 	'account': 'U9117375'
# }

# paper1 = {
# 	'broker':'1',
# 	'account': 'U9778379'
# }

# paper1 = {
# 	'broker':'4',
# 	'account': '666800007835'
# }

factset_map ={
	'HK_60_MHIX8':'MHIX18-HKF',
	'HK_60_MHIZ8':'MHIZ8-HKF'
}
limit_map ={
	'BUY':'BID_1',
	'SELL':'ASK_1'
}

def trading_callback(trading_data):
	global dicts
	# print(trading_data)
	return
if __name__ == '__main__':

	# global dicts = {}

	msq_client = MarketConsumerClient(
	iuid_list = ['HK_10_2822'],
	broker_id = paper1.get('broker'),
	account_id = paper1.get('account'),
	on_order = trading_callback
	)

	# trading = TradingController()
	# cash_info = trading.get_cash_info(baccount = paper1.get('account'), brokerid = paper1.get('broker'))
	# position_info = trading.get_holdings(baccount =paper1.get('account'),brokerid = paper1.get('broker'))
	# print (result)

	# order = dict(
	# 	type='LIMIT',  # Market
	# 	side='BUY',
	# 	symbol='GB_10_VOD',
	# 	quantity=15,
	# 	price=157.3
	# )
	order = dict(
		type='MARKET',  # Market
		side='BUY',
		symbol='GB_10_MINT',
		quantity=15,
		price=157.3
	)

	# order = dict(
	# 	type='LIMIT',  # Market
	# 	side='SELL',
	# 	symbol='HK_60_MHIZ8',
	# 	quantity=1,
	# 	price= 26575
	# )
	# order = dict(
	# 	type='LIMIT',  # Market
	# 	side='BUY',
	# 	symbol='HC_10_600000',
	# 	quantity=100,
	# 	price=10.62
	# )
	# order = dict(
	# 	type='LIMIT',  # Market
	# 	side='BUY',
	# 	symbol='CN_10_600000',
	# 	quantity=100,
	# 	price=10.62
	# )
	# order = dict(
	# 	type='LIMIT',  # Market
	# 	side='BUY',
	# 	symbol='GB_40_CNHHKD',
	# 	quantity=100,
	# 	price=1.127
	# )
	# price_data = get_data(factset_map[order.get('symbol')]).loc[limit_map[order.get('side')]].values
	# order['price'] = float(price_data[0])
	#
	# orders = [
	# 	dict(
	# 		type='Limit',  # Market
	# 		side='Buy',
	# 		symbol='HK_10_5',
	# 		quantity=800,
	# 		price=65.7
	# 	),
	# 	dict(
	# 		type='Limit',  # Market
	# 		side='Sell',
	# 		symbol='HK_10_700',
	# 		quantity=200,
	# 		price=270
	# 	)
	# ]
	#
	# price_data = get_data(factset_map[order.get('symbol')]).loc[limit_map[order.get('side')]].values
	# order['price'] = float(price_data[0])


	# orderid = 116
	# order = trading.submit_order(baccount=paper1.get('account'), order= order,brokerid = paper1.get('broker'))
	# orderid = order.get('data')
	# orderid = trading.submit_orders(baccount='DU1161136', orders= orders,brokerid = paper1.get('broker'))
	# #
	# result = trading.get_order_status(orderid)
	#
	# result = trading.cancel_order(orderid).get('status')
	print ()



	while True:
		print (msq_client.orderbook_data)
		time.sleep(1)

