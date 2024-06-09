from market_consumer_client import MarketConsumerClient
import sys
import json
import time
from lib.commonalgo.execution.trading_controller import TradingController
from lib.commonalgo.data.choice_proxy_client import choice_client
from lib.commonalgo.data.factset_client import get_data

trading = TradingController()

test_account = {
	'broker':'2',
	'acc': '111111'
}
#
# paper1 = {
# 	'broker':'1',
# 	'account': 'DU1161136'
# }

# paper1 = {
# 	'broker':'1',
# 	'account': 'U9117375'
# }

# paper1 = {
# 	'broker':'1',
# 	'account': 'U9778379'
# }

paper1 = {
	'broker':'9',
	'account': '666800007835'
}

factset_map ={
	'HK_60_MHIX8':'MHIX18-HKF',
	'HK_60_MHIZ8':'MHIZ8-HKF'
}

def from_iuids_to_key(symbols = []):
	return [x.replace('_','.') for x in symbols]

def from_iuids_to_choice(symbols = []):
	results = []
	for symbol in symbols:
		results.append(from_iuid_to_choice(symbol))
	return results

def from_iuid_to_choice(symbol):
	_,_,local_symbol = symbol.split('_')
	if int(local_symbol[0]) == 6:
		sufix = '.SH'
	else:
		sufix = '.SZ'
	return local_symbol+sufix

# limit_map ={
# 	'BUY':'a1',
# 	'SELL':'b1'
# }

limit_map ={
	'BUY':'SELLPRICE1',
	'SELL':'BUYPRICE1'
}

def trading_callback(trading_data):
	global dicts
	print(trading_data)
	return
if __name__ == '__main__':

	# global dicts = {}

	trading = TradingController()
	cash_info = trading.get_cash_info(baccount = paper1.get('account'), brokerid = paper1.get('broker'))
	position_info = trading.get_holdings(baccount =paper1.get('account'),brokerid = paper1.get('broker'))
	# print (result)

	orders = [
		dict(
			type='LIMIT',  # Market
			side='BUY',
			symbol='CN_10_600000',
			quantity=100,
			price=65.7
		),
		dict(
			type='LIMIT',  # Market
			side='BUY',
			symbol='CN_10_000001',
			quantity=100,
			price=270
		)
	]

	symbols = [x.get('symbol') for x in orders]
	choice_symbols = from_iuids_to_choice(symbols)
	choice_symbols_str = ','.join(choice_symbols)

	msq_client = MarketConsumerClient(
	iuid_list = symbols,
	broker_id = paper1.get('broker'),
	account_id = paper1.get('account'),
	on_order = trading_callback
	)

	# get price data from socket cache
	# for order in orders:
	# 	price = None
	# 	choice_symbol = from_iuid_to_key([order.get('symbol')])[0]
	# 	while not price:
	# 		ob = msq_client.orderbook_data.get(choice_symbol)
	# 		if ob:
	# 			price = ob.get(limit_map[order.get('side')])
	# 	order['price'] = float(price)


	# update price data from choice snapshot
	price_data = choice_client.csqsnapshot(choice_symbols_str, "BuyPrice1,SellPrice1")
	for order in orders:
		choice_symbol = from_iuid_to_choice(order.get('symbol'))
		key = limit_map[order.get('side')]
		order['price'] = float(price_data[(choice_symbol,key)][0])


	order = trading.submit_orders(baccount='DU1161136', orders= orders,brokerid = paper1.get('broker'))
	orderids = []
	if not order.get('status').get('ecode'):
		orderids = [x.get('id') for x in order.get('data')]
	# #
	for orderid in orderids:
		result = trading.get_order_status(orderid).get('data')
		status = result.get('status')
		cancel_result = trading.cancel_order(orderid).get('status')

print(1)

