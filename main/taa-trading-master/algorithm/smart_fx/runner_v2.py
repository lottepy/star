import sys
import os


from algorithm.fx_base_v2 import ForexLive
from utils.constants import *
from datetime import datetime
from pytz import timezone
import time

ONE_MINUTE = 60

def main_run():
	today = datetime.today().strftime('%Y%m%d')
	today = datetime(2020,4,30).strftime('%Y%m%d')
	symbols = ALL_CCY
	# today: 跑ftp的日期
	# symbols: 所有symbol
	fx = ForexLive(
		today=today,
		symbols=symbols,
	)

	fx.run()


if __name__ == '__main__':
	main_run()

	# while True:
	# 	dt = datetime.now()
	# 	trade_dt = dt.replace(hour=13, minute=55, second=0)
	# 	end_dt = dt.replace(hour=19, minute=30, second=0)
	# 	if True or dt > trade_dt and dt < end_dt:
	# 		try:
	# 			# main_update()
	# 			main_run()
	# 			break
	# 		except Exception as ee:
	# 			print(ee)
	# 	else:
	# 		print(f'waiting for {trade_dt}, at {dt}')
	# 		time.sleep(ONE_MINUTE)
