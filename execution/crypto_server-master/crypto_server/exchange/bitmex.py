import ccxt

from .base import ExchangeBase

class Bitmex(ExchangeBase):
	def __init__(self, api_key, api_secret):
		self.exchange = ccxt.binance({
			'apiKey': api_key,
			'secret': api_secret,
			'options': {
				'adjustForTimeDifference': True,
				'recvWindow': 15 * 1000,
			},
			'verbose': False,
		})
		self.exchange.load_markets()
		self.accounts = {}

	def create_order(self, *args, **kwargs):
		return self.exchange.create_order(*args, **kwargs)