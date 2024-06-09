from fx_daily.data.constants import *
from fx_daily.data.config.config import ConfigManager
from fx_daily.core.order import *
import numpy as np
import numba as nb
import importlib

# cp = ConfigManager('data/config/config.ini')
#
# CASH = cp.configs['cash']
# WINDOW_SIZE = cp.configs['window_size']
# COMMISSION = cp.configs['commission']
# strategy_module = cp.configs['strategy_module']
# strategy_name = cp.configs['strategy_name']
# Display = cp.configs['display']
# if strategy_name == 'mean_reversion':
# 	symbol = ['NZDJPY', 'GBPJPY', 'GBPCHF', 'EURJPY', 'CADJPY',
# 			  'CADCHF', 'AUDJPY', 'USDNOK', 'USDCHF', 'NZDUSD',
# 			  'AUDUSD', 'USDZAR', 'USDKRW', 'USDTWD', 'USDINR',
# 			  'USDIDR']
# else:
# 	symbol = ['NZDJPY', 'NOKSEK', 'GBPJPY', 'GBPCHF', 'GBPCAD',
# 			  'EURJPY', 'EURGBP', 'EURCHF', 'EURCAD', 'CADJPY',
# 			  'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK',
# 			  'USDNOK', 'USDJPY', 'USDCHF', 'USDCAD', 'NZDUSD',
# 			  'GBPUSD', 'EURUSD', 'AUDUSD', 'USDZAR', 'USDSGD',
# 			  'USDTHB', "USDKRW", "USDTWD", "USDINR", "USDIDR"]
#
# STRATEGY = getattr(
# 	importlib.import_module(strategy_module),
# 	strategy_name
# )


def calculate_rate(rate_window, ccy_position, weekday):
	# 周4： dt=3
	current_rate = rate_window[-2]
	if weekday == 3:
		dt = 3
	else:
		dt = 1
	# 'LAST', 'BID', 'ASK'
	multiple = (1. + dt * current_rate / 100 / 360)
	multiple_last = multiple[:, 0]
	for i in range(ccy_position.shape[1]):
		multiple_i = multiple_last
		ccy_position[:, i] = ccy_position[:, i] * multiple_i
	pv = np.sum(ccy_position, axis=0)
	return pv, ccy_position


def calculate_spot(FxUSD_data, ccy_position):
	multiple = (FxUSD_data[-1] / FxUSD_data[-2])
	for i in range(ccy_position.shape[1]):
		ccy_position[:, i] = ccy_position[:, i] * multiple
	pv = np.sum(ccy_position, axis=0)
	return pv, ccy_position


def run_once(FxArray, FxUSDArray, RateArray,
			 timeArray, ccy_matrix, strategy_param, back_index, cp):
	# FxArray (time * symbol * field) 每个货币对的汇率 日级
	# FxUSDArray (time * symbol) 每个货币 quote in usd 汇率 日级
	# RateArray (time * symbol)  每个货币的利率 日级
	# timeArray (time * time_field) 时间轴 日级
	# ccy_matrix (n_ccy * n_ccp) 货币对到货币转换矩阵

	CASH = cp.configs['cash']
	WINDOW_SIZE = cp.configs['window_size']
	COMMISSION = cp.configs['commission']
	strategy_module = cp.configs['strategy_module']
	strategy_name = cp.configs['strategy_name']
	Display = cp.configs['display']
	if strategy_name == 'mean_reversion':
		symbol = ['NZDJPY', 'GBPJPY', 'GBPCHF', 'EURJPY', 'CADJPY',
				  'CADCHF', 'AUDJPY', 'USDNOK', 'USDCHF', 'NZDUSD',
				  'AUDUSD', 'USDZAR', 'USDKRW', 'USDTWD', 'USDINR',
				  'USDIDR']
	else:
		symbol = ['NZDJPY', 'NOKSEK', 'GBPJPY', 'GBPCHF', 'GBPCAD',
				  'EURJPY', 'EURGBP', 'EURCHF', 'EURCAD', 'CADJPY',
				  'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK',
				  'USDNOK', 'USDJPY', 'USDCHF', 'USDCAD', 'NZDUSD',
				  'GBPUSD', 'EURUSD', 'AUDUSD', 'USDZAR', 'USDSGD',
				  'USDTHB', "USDKRW", "USDTWD", "USDINR", "USDIDR"]

	STRATEGY = getattr(
		importlib.import_module(strategy_module),
		strategy_name
	)

	n_security = ccy_matrix.shape[1]

	cash = CASH
	pv0 = np.ones(n_security) * CASH / n_security
	pv = pv0
	# ccp_position: 货币对的仓位
	# ccy_position: 货币的仓位

	# 分别记录每个货币对的盈利与仓位
	ccp_position = np.zeros(n_security)
	# 每一列分别记录每个货币对的仓位
	ccy_position = np.zeros((ccy_matrix.shape[0], n_security))
	ccy_position[-1] = pv0
	last_order = np.zeros(n_security)
	# 'timestamp', 'year', 'month', 'day', 'hour', 'minute', 'second'

	backtest_list = np.arange(timeArray.shape[0])[back_index[0]:back_index[1] + 1]
	first_index = backtest_list[0]
	# 存储中间变量
	n_field1 = 1
	n_field2 = 1 + pv0.shape[0]
	n_field3 = n_field2 + 1
	n_field4 = n_field3 + ccy_position.shape[0]
	n_field5 = n_field4 + 1

	status = np.empty((len(backtest_list), n_field5), dtype=object)
	mom_record = np.zeros((1, n_security), dtype=np.float64)

	for i in backtest_list:

		time_data = timeArray[i - WINDOW_SIZE + 1: i + 1]
		current_weekday = time_data[-1, 1]
		current_ts = time_data[-1, 0]
		window_data = FxArray[i - WINDOW_SIZE + 1: i + 1]
		FxUSD_data = FxUSDArray[i - WINDOW_SIZE + 1: i + 1]
		Rate_data = RateArray[i - WINDOW_SIZE + 1: i + 1]

		# 第一天不计息
		if i >= WINDOW_SIZE:
			pv, ccy_position = calculate_spot(FxUSD_data, ccy_position)
			pv, ccy_position = calculate_rate(Rate_data, ccy_position, current_weekday)
		ccp_target, mom_record, weight = STRATEGY(window_data, time_data, pv, strategy_param, last_order, mom_record)
		ccy_position, last_order = cross_order_fx(ccp_target, ccy_matrix, pv, last_order, COMMISSION)

		if Display == 1:
			status[i - first_index, 0:n_field1] = current_ts
			status[i - first_index, n_field1:n_field2] = pv
			status[i - first_index, n_field2:n_field3] = pv.sum()
			status[i - first_index, n_field3:n_field4] = ccy_position.sum(axis=1)
			status[i - first_index, n_field4:n_field5] = dict(zip(symbol, weight.tolist()))

	ccp_return = (pv - pv0) / pv0
	simple_return = (np.sum(pv) - np.sum(pv0)) / np.sum(pv0)

	return simple_return, ccp_return, status
