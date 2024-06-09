import numpy as np
from datetime import datetime
from fx_daily.utils.tools import AlgoTool


def momentum(window_data, time_data, pv, strategy_param, last_order, mom_record):
	"""
	动量策略：

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

	# 获取参数列表
	rank_period = int(strategy_param[0])
	sample_period = int(strategy_param[1])
	t = 0.05
	g = 0.0

	# 获取历史数据
	prices = window_data[:, :, 0]

	# 计算不同跨度的动量
	window_list = np.array([20, 30, 40, 50, 60, 70])
	ic_list = []
	for window_i in window_list:
		x = prices[-sample_period - 1: -1] / prices[-sample_period - window_i - 1: -window_i - 1] - 1.
		y = prices[-sample_period:] / prices[-sample_period - 1: -1] - 1.
		ic_list.append(np.corrcoef(x.flatten(), y.flatten())[0][1])

	# 得到最优动量跨度
	ic_list = np.array(ic_list)
	best_window = window_list[ic_list == ic_list.max()][0]

	mom = prices[-1] / prices[-best_window] - 1.
	if mom_record.shape[0] == 1:
		mom_record = mom
	else:
		mom_record = np.vstack((mom_record, mom))
	signal = np.sign(mom) / n_security

	# 根据波动率配权
	returns = prices[-75:] / prices[-76: -1] - 1.
	volatility = returns.std(axis=0)
	stability = 1. / volatility
	stability[stability <= np.median(stability)] = 0
	sigma_weight = stability / stability.sum()

	if mom_record.shape[0] > 40:
		mom_rank = mom_record[-rank_period:].argsort(axis=0).argsort(axis=0)
		current_level = mom_rank[-1] / (mom_rank.shape[0] - 1.)

		signal = np.zeros(n_security)
		signal[(current_level >= 0.5 + t) & (current_level <= 1.0 - g)] = 1.
		signal[(current_level <= 0.5 - t) & (current_level >= g)] = -1.

	today = datetime.strptime('/'.join(time_data[-1][-3:].astype(int).astype(str)), '%Y/%m/%d')
	if today >= datetime(2019, 10, 25):
		import pandas as pd
		mom_record_df = pd.DataFrame(mom_record)
		mom_record_df.to_csv('mom_record.csv')

	signal = signal * sigma_weight
	target_weight = signal * 2
	ccp_target = portfolio_value * target_weight

	return ccp_target, mom_record, target_weight