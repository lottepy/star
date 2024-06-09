import ccxt


class ExchangeBase(object):
	def __init__(self, api_key, api_secret):
		self.exchange = ccxt.binance()
		self.exchange.load_markets()
		self.accounts = {}

	def id_to_symbol(self, symbol_id):
		symbol = self.exchange.find_market(symbol_id)['symbol']
		return symbol

	def symbol_to_id(self, symbol):
		try:
			return self.exchange.market_id(symbol)
		except:
			return None

	def common_currency_code(self, asset):
		return self.exchange.common_currency_code(asset)

	def fetch_account_balance(self):
		response = self.exchange.fetch_balance()
		response.pop('info')
		response.pop('free')
		response.pop('used')
		response.pop('total')
		for asset, balance in response.items():
			available = float(balance['free'])
			reserve = float(balance['used'])
			symbol = self.common_currency_code(asset)
			if available != 0 or reserve != 0:
				self.accounts[symbol] = {
					'free': available,
					'used': reserve
				}
		return self.accounts
