from gevent import monkey
monkey.patch_all()

from lib.commonit.datahub import MarketCache, OrderbookCache
import time

cache = MarketCache(['SG_60_CNX8'])
obcache = OrderbookCache(['SG_60_CNX8'])
while True:
	time.sleep(1)
	print(cache['SG_60_CNX8'])
	print(obcache['SG_60_CNX8'])