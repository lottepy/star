import json

from django.conf import settings
from django.http import JsonResponse
from common.logger import log
from .exchange.binance import Binance
from .exchange.shipane import Shipane
from .exchange.bitmex import Bitmex

_exchanges = {
	'binance': Binance(settings.API_KEY, settings.API_SECRET),
	'shipane': Shipane(),
	'bitmex': Bitmex(settings.BITMEX_KEY, settings.BITMEX_SEC)
}


def get_exchange(ex_name):
	return _exchanges[ex_name]


def order(request):
	"""
	orders = {
		'tid': tid,
		'exchange': exchange,
		'orders': [{
			'symbol': symbol,
			'type': 'market',
			'exchange': exchange,
			'action': 'sell',
			'amount': amount,
			'price': price,
			'ob_ts': ob_ts
		}],
		'ts': ts,
		'timeout': timeout,
	}
	"""
	payload = json.loads(request.body)
	if payload['token'] != settings.ACCESS_TOKEN:
		return JsonResponse({})
	exchange = get_exchange(payload['exchange'])
	buy_orders = filter(lambda o: o['action'].upper() == 'BUY', payload['orders'])
	sell_orders = filter(lambda o: o['action'].upper() == 'SELL', payload['orders'])
	order_results = []
	response = {
		'orders': order_results
	}

	for order in sell_orders:
		assert (order['type'] == 'market')
		try:
			result = exchange.create_order(
				order['symbol'], order['type'], order['action'], order['amount'], order.get('price')
			)
			order_results.append(result)
		except:
			log.exception('order_exc')

	for order in buy_orders:
		assert (order['type'] == 'market')
		try:
			result = exchange.create_order(
				order['symbol'], order['type'], order['action'], order['amount'], order.get('price')
			)
			order_results.append(result)
		except:
			log.exception('order_exc')
	return JsonResponse(response)


def position(request):
	if request.GET['token'] != settings.ACCESS_TOKEN:
		return JsonResponse({})
	ex_name = request.GET.get('exchange', 'binance')
	exchange = get_exchange(ex_name)
	try:
		result = exchange.fetch_account_balance()
	except:
		log.exception('position_exc')
		result = {}
	return JsonResponse(result)


def trade(request):
	if request.GET['token'] != settings.ACCESS_TOKEN:
		return JsonResponse({})
	ex_name = request.GET.get('exchange', 'binance')
	exchange = get_exchange(ex_name)
	try:
		result = exchange.fetch_trades()
	except:
		log.exception('position_exc')
		result = {}
	return JsonResponse(result)
