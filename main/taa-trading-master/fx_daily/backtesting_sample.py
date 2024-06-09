from fx_daily.core.engine import ForexEngine
from fx_daily.utils.constants import *

from datetime import datetime
import numpy as np
import itertools

if __name__ == '__main__':

	# 单次回测范例：动量策略
	strategy_params = [100, 250]

	single_sample = ForexEngine(
		config_file='data/config/config_mom.ini',
		start=datetime(2011, 12, 31),
		end=datetime(2019, 10, 31),
		symbols=ALL_CCY_NO_CROSS,
		params=strategy_params,
		mode='single'
	)
	single_sample.run()

	# # 多次回测范例：均值回归策略
	# # mean-reversion策略参数：[alpha, rho, window, N]
	# alpha_list = np.arange(0.1, 1.0, 0.1)
	# rho_list = [0.02, 0.04, 0.06, 0.08, 0.1]
	# window_list = [10, 20, 30]
	# N_list = [3, 4, 5]
	#
	# all_combinations = list(itertools.product(
	# 	alpha_list,
	# 	rho_list,
	# 	window_list,
	# 	N_list
	# ))
	# strategy_params = np.array(all_combinations, dtype=float)
	#
	# multi_sample = ForexEngine(
	# 	config_file='data/config/config_mr.ini',
	# 	start=datetime(2011, 12, 31),
	# 	end=datetime(2019, 10, 31),
	# 	symbols=ALL_CCY_NO_CROSS,
	# 	params=strategy_params,
	# 	mode='multi'
	# )
	# multi_sample.run()

	# # 计算VaR范例
	# strategy_params = []
	#
	# var_test = ForexEngine(
	# 	config_file='data/config/config_var.ini',
	# 	start=datetime(2011, 12, 31),
	# 	end=datetime(2019, 10, 31),
	# 	symbols=ALL_CCY_NO_CROSS,
	# 	params=strategy_params,
	# 	mode='var',
	# 	weight_file='weight_equal_weighted.csv'
	# )
	# var_test.run()

