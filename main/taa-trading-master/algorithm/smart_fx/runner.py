from algorithm.fx_base import ForexLive, EqualWeight
from algorithm.smart_fx.reversion import MeanReversion
from algorithm.smart_fx.momentum import Momentum
from algorithm.smart_fx.macro import Macro
from utils.constants import *
from datetime import datetime
from pytz import timezone
import time

ONE_MINUTE = 60

def main_run():
	start_date = '2016-12-31'
	end_date = datetime.today().strftime('%Y-%m-%d')
	symbols = ALL_CCY_NO_CROSS

	ew = EqualWeight()
	mr = MeanReversion(weight_file=f'{FX_RECORD_PATH}/mr_weight.csv')
	macro = Macro(expected_return_file=f'{FX_RECORD_PATH}/expected_return.csv',
				  r_squared_file=f'{FX_RECORD_PATH}/r_squared.csv',
				  param_file=f'{FX_RECORD_PATH}/all_adjparam_meanLR.json')

	mom = Momentum(mom_file=f'{FX_RECORD_PATH}/mom_history.csv')
	algo_list = [macro, mr, mom]

	fx = ForexLive(
		start=start_date,
		end=end_date,
		symbols=symbols,
		algo_list=algo_list,
		compound_method='VaR'
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
