import numpy as np
from fx_daily.utils.tools import AlgoTool
import pandas as pd


def var(window_data, time_data, pv, strategy_param, last_order, mom):
	"""
	计算Historical VaR：

	:param window_data: ndarray
		(time, symbol, field)
		field: [last, bid, ask]
	:param time_data: ndarray
		time_data(time, field)
		field: [timestamp, weekday, year, month, day]
	:param pv: ndarray
		pv(1 * N_security)
		记录每个货币的pv = init_pv + pnl
	:param strategy_param: ndarray
		[alpha, rho, window, N]

	:return: ndarray
		ccp_target(1 * N_security)
	"""

	# 获取账户和持仓信息
	portfolio_value = np.sum(pv)
	n_security = window_data.shape[1]

	target_weight = strategy_param[0]
	ccp_target = portfolio_value * target_weight

	return ccp_target, None, target_weight