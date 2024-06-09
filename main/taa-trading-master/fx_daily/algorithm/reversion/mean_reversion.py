import numpy as np
from fx_daily.utils.tools import AlgoTool

def mean_reversion(window_data, time_data, pv, strategy_param, last_order, mom):
	"""
	均值回归策略：计算过去20天的ema，作为最近的反转信号，
	反转最大的4个货币对做多，反转最小的4个货币对做空，更新仓位
	
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
	alpha = strategy_param[0]
	rho = strategy_param[1]
	w = int(strategy_param[2])
	n_underlying = strategy_param[3]

	# 获取历史数据
	prices = window_data[-w:, :, 0]

	# 计算预期收益
	predict_return = np.ones(n_security)
	for i in range(0, w - 1):
		x_t = prices[i + 1] / prices[i]
		predict_return = AlgoTool.compute_ema(alpha, x_t, predict_return)

	difference = predict_return.max() - predict_return.min()
	if last_order.sum() == 0.:
		r = -1.
	else:
		r = rho

	target_weight = np.zeros(n_security)
	if difference > r:
		target_weight[predict_return.argsort().argsort() >= (n_security - n_underlying)] = 0.5 / n_underlying
		target_weight[predict_return.argsort().argsort() <= (n_underlying - 1)] = -0.5 / n_underlying
		ccp_target = portfolio_value * target_weight
	else:
		ccp_target = last_order
		target_weight = ccp_target / portfolio_value

	return ccp_target, None, target_weight