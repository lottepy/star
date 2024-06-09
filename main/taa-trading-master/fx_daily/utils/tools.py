import numpy as np
import pandas as pd
import sys
from os.path import dirname, abspath, join
import cvxopt
from dateutil.relativedelta import relativedelta
#import cvxpy as cp
import math
import itertools

helper_path = dirname(__file__)
sys.path.append(helper_path)
pd.set_option('display.width', 250)
pd.set_option('display.max_rows', None)


class AlgoTool():
	def __init__(self):
		pass

	@staticmethod
	def normalize_portfolio_olmar(b_t, lb, ub):
		n = len(b_t)
		P = np.eye(n)
		q = -b_t

		# Inequality constrain
		G_lb = -np.eye(n)
		G_ub = np.eye(n)
		G = np.vstack([G_lb, G_ub]).astype(float)

		h_lb = np.array([-lb * np.ones(n)]).T
		h_ub = np.array([ub * np.ones(n)]).T
		h = np.vstack([h_lb, h_ub]).astype(float)

		# Equality constrain
		A = np.array([np.ones(n)])
		b = np.array([[1.]])

		cvxopt.solvers.options['show_progress'] = False
		cvxopt.solvers.options['abstol'] = 1e-21
		optimized = cvxopt.solvers.qp(
			cvxopt.matrix(P),
			cvxopt.matrix(q),
			cvxopt.matrix(G),
			cvxopt.matrix(h),
			cvxopt.matrix(A),
			cvxopt.matrix(b)
		)
		# print optimized['status']

		return np.array(optimized['x'].T).flatten()

	@staticmethod
	def update_portfolio_olmar(b_t, x_head, eps):
		x_mean = np.mean(x_head)
		lam = max(0., (eps - b_t.dot(x_head)) / np.linalg.norm(x_head - x_mean) ** 2)

		# Limit lambda to avoid numerical problems.
		lam = min(100000., lam)

		# Update portfolio.
		b = b_t + lam * (x_head - x_mean)

		return b

	@staticmethod
	def compute_ema(alpha, x_t, xt_head):
		return alpha + (1 - alpha) * np.divide(xt_head, x_t)

	@staticmethod
	def update_portfolio_ons(r, p, beta, delta, eta, A, b, lb, ub):
		grad = np.mat(r / np.dot(p, r)).T
		A += grad * grad.T
		b += (1 + 1. / beta) * grad

		# Projection of p induced by norm A
		pp = AlgoTool.normalize_portfolio_ons(delta * A.I * b, A, lb, ub)

		return pp * (1 - eta) + np.ones(len(r)) / float(len(r)) * eta

	@staticmethod
	def normalize_portfolio_ons(x, M, lb, ub):
		"""
		Projection of x to simplex indiced by matrix M. 
		Uses quadratic programming.

		:param x: np.matrix
			x = delta * (A_t-1)' * b_t-1
		:param M: np.matrix
			M = A_t-1
		:param lb:  float
			Lower bound
		:param ub: float
			Upper bound

		:return: np.array
			Normalized optimal weight.
		"""
		n = M.shape[0]
		P = 2 * M
		q = -2 * M * x

		# Inequality constrain
		G_lb = -np.eye(n)
		G_ub = np.eye(n)
		G = np.vstack([G_lb, G_ub]).astype(float)

		h_lb = np.array([-lb * np.ones(n)]).T
		h_ub = np.array([ub * np.ones(n)]).T
		h = np.vstack([h_lb, h_ub]).astype(float)

		# Equality constrain
		A = np.array([np.ones(n)])
		b = np.array([[1.]])

		cvxopt.solvers.options['show_progress'] = False
		cvxopt.solvers.options['abstol'] = 1e-21
		optimized = cvxopt.solvers.qp(
			cvxopt.matrix(P),
			cvxopt.matrix(q),
			cvxopt.matrix(G),
			cvxopt.matrix(h),
			cvxopt.matrix(A),
			cvxopt.matrix(b)
		)
		# print optimized['status']

		return np.array(optimized['x'].T).flatten()

	@staticmethod
	def get_next_trading_day(current_date, freq_num, freq_unit, count):
		if freq_unit == "D":
			return current_date + relativedelta(days=freq_num * count)
		elif freq_unit == "M":
			return current_date + relativedelta(months=freq_num * count)
		elif freq_unit == "W":
			return current_date + relativedelta(weeks=freq_num * count)
		elif freq_unit == "Y":
			return current_date + relativedelta(years=freq_num * count)

	@staticmethod
	def freq_convert(freq):
		if freq == 'Never':
			freq_num = 100
			freq_unit = 'Y'
		else:
			freq_num = int(freq[:-1])
			freq_unit = freq[-1]
		return freq_num, freq_unit

	@staticmethod
	def compute_carry(tn, fx, scale):
		return -1. * pd.np.divide(pd.np.divide(tn, fx), np.power(10, scale))

	@staticmethod
	def generate_currency_matrix(pair_list):
		all_ccy_list = []
		n = 3
		for p in pair_list:
			all_ccy_list = all_ccy_list + [p[i: i + n] for i in range(0, len(p), n)]
		all_ccy_list = list(dict.fromkeys(all_ccy_list))

		n_pair = len(pair_list)
		n_ccy = len(all_ccy_list)
		ccy_matix = np.zeros((n_pair, n_ccy))
		ccy_matix_df = pd.DataFrame(
			ccy_matix,
			columns=all_ccy_list,
			index=pair_list
		)

		for p in pair_list:
			long_ccy = p[:3]
			short_ccy = p[3:]
			ccy_matix_df.loc[p, long_ccy] = 1
			ccy_matix_df.loc[p, short_ccy] = -1

		return ccy_matix_df

	@staticmethod
	def optimize_weight(b_t, x_head, C, lb, ub, eps):
		n = len(b_t)
		P = np.diag(np.append(np.ones(n), [0.]))
		q = -np.append(b_t, [-C])

		# Inequality constrain
		G_lb = -np.eye(n + 1)
		G_ub = np.diag(np.append(np.ones(n), [0.]))
		G_loss = -np.append(x_head, [1.])
		G = np.vstack([G_lb, G_ub, G_loss]).astype(float)

		h_lb = np.array([-np.append(lb * np.ones(n), [0.])]).T
		h_ub = np.array([ub * np.ones(n + 1)]).T
		h_loss = [-eps]
		h = np.vstack([h_lb, h_ub, h_loss]).astype(float)

		# Equality constrain
		A = np.array([np.append(np.ones(n), [0.])])
		b = np.array([[0.]])

		cvxopt.solvers.options['show_progress'] = False
		cvxopt.solvers.options['abstol'] = 1e-21
		optimized = cvxopt.solvers.qp(
			cvxopt.matrix(P),
			cvxopt.matrix(q),
			cvxopt.matrix(G),
			cvxopt.matrix(h),
			cvxopt.matrix(A),
			cvxopt.matrix(b)
		)
		# print(optimized['status'])

		return -np.array(optimized['x'].T).flatten()[:-1]

	@staticmethod
	def optimize_weight_cvxpy(b_t, x_head, lb, ub, eps):
		n = len(b_t)
		x = cp.Variable(n)

		objective = cp.Minimize(cp.sum_squares(x - b_t))
		constaints = [
			-lb <= x,
			x <= ub,
			b_t.T * x >= eps
		]
		prob = cp.Problem(objective, constaints)

		result = prob.solve()
		solution = np.array(x.value)
		return solution

	@staticmethod
	def portfolio_summary_fx(perf_manual, T):
		pv = perf_manual['port_return']
		cr = pv - 1.
		dr = pv.pct_change().dropna()

		annual_return = math.pow(pv.ix[-1], 1 / T) - 1.
		volatility = dr.std() * math.sqrt(252)
		sharpe = annual_return / volatility
		mdd = (pv.div(pv.cummax()) - 1.).min()
		calmar = annual_return / abs(mdd)
		var = dr.mean() - dr.std() * 2.32

		result = pd.Series(
			[annual_return, volatility, sharpe, mdd, calmar, var],
			index=['annual_return', 'volatitliy', 'sharpe', 'mdd', 'calmar', 'var']
		)

		return result

	@staticmethod
	def get_multi_params(params):
		all_combinations = itertools.product(params)
		return np.array(all_combinations, dtype=float)
