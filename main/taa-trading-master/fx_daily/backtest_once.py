from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import itertools
import os

from fx_daily.utils.constants import *
from fx_daily.core.engine import ForexEngine

if __name__ == '__main__':

	# # 均值回归策略
	# strategy_params = np.array([0.1, 0.06, 30, 4])
	#
	# single_sample = ForexEngine(
	# 	config_file='data/config/config_mr.ini',
	# 	start=datetime(2011, 12, 31),
	# 	end=datetime(2019, 10, 31),
	# 	symbols=ALL_CCY_NO_CROSS,
	# 	params=strategy_params,
	# 	mode='single'
	# )
	# single_sample.run()

	# 动量策略
	strategy_params = np.array([75, 500])

	single_sample = ForexEngine(
		config_file='data/config/config_mom.ini',
		start=datetime(2011, 10, 31),
		end=datetime(2019, 10, 31),
		symbols=ALL_CCY_NO_CROSS,
		params=strategy_params,
		mode='single'
	)
	single_sample.run()