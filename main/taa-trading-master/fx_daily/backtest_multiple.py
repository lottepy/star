from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import itertools
import os

from fx_daily.utils.constants import *
from fx_daily.core.engine import ForexEngine

if __name__ == '__main__':

	# mean-reversion策略参数：[alpha, rho, window, N]
	# alpha_list = np.arange(0.1, 1.0, 0.1)
	# rho_list = [0.02, 0.04, 0.06, 0.08, 0.1]
	# window_list = [10, 20, 30]
	# N_list = [3, 4, 5]
	# all_combinations = list(itertools.product(
	# 	alpha_list,
	# 	rho_list,
	# 	window_list,
	# 	N_list
	# ))
	# strategy_params = np.array(all_combinations, dtype=float)
	#
	# mr_engine = ForexEngine(
	# 	config_file='data/config/config_mr.ini',
	# 	start=datetime(2011, 12, 31),
	# 	end=datetime(2019, 10, 31),
	# 	symbols=ALL_CCY_NO_CROSS,
	# 	params=strategy_params,
	# 	mode='multi'
	# )
	# mr_engine.run()

	# momentum策略参数：[rank_period, sample_period]
	M_list = [50, 75, 100, 125, 150, 200]
	P_list = [75, 150, 200, 250, 300, 500]
	all_combinations = list(itertools.product(M_list, P_list))
	strategy_params = np.array(all_combinations, dtype=float)

	mom_engine = ForexEngine(
		config_file='data/config/config_mom.ini',
		start=datetime(2011, 10, 31),
		end=datetime(2019, 10, 31),
		symbols=ALL_CCY_NO_CROSS,
		params=strategy_params,
		mode='multi'
	)
	mom_engine.run()
