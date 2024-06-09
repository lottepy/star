import numpy as np
from math import sqrt
from numpy.linalg import inv, norm
# from pyOpt import Optimization
# from pyOpt.pyPSQP import PSQP
from numpy import dot, sqrt, log, array
import pandas as pd
import sys
from os.path import dirname, abspath, join

helper_path = dirname(__file__)
sys.path.append(helper_path)
pd.set_option('display.width', 250)
pd.set_option('display.max_rows', None)

def numberOfStocks(ratio):
	if ratio > 1.0 or ratio < 0.0:
		return "Wrong!"
	if ratio >= 0.8:
		return 9
	elif ratio >= 0.6:
		return 8
	elif ratio >= 0.4:
		return 7
	else:
		return 5


def numberOfSb(ratio):
	if ratio > 1.0 or ratio < 0.0:
		return "Wrong!"
	elif ratio >= 0.6:
		return 3
	elif ratio >= 0.4:
		return 5
	else:
		return 7


def number_of_bond(ratio):
	if ratio > 1.0 or ratio < 0.0:
		print("Wrong!")
	elif ratio >= 0.6:
		return 5
	elif ratio >= 0.4:
		return 6
	else:
		return 8


def targetValue(ratio):
	# return 0.02 + 0.01 * sqrt(ratio)
	return 0.02 + 0.01 * ratio ** 2


def target_value(ratio):
	if ratio > 1.0 or ratio < 0.0:
		print("Wrong!")
	elif ratio >= 0.8:
		return 0.03
	elif ratio >= 0.6:
		return 0.025
	elif ratio >= 0.4:
		return 0.0225
	else:
		return 0.02


def target_value_c(ratio):
	if ratio >= 0.4:
		return 0.01875 * ratio + 0.015
	else:
		return 0.02


def secArray(numberOfStock, numberOfBond):
	numberOfSec = numberOfStock + numberOfBond + 1
	bondArray = np.zeros(numberOfSec)
	stockArray = np.ones(numberOfSec)
	bondArray[4:numberOfBond + 4] = 1
	stockArray[4:numberOfBond + 4] = 0
	stockList = stockArray.tolist()
	bondList = bondArray.tolist()
	# print type(stockArray)

	return stockList, bondList


def valueArray(numberOfStock, numberOfBond, valueOfStock, valueOfBond):
	numberOfSec = numberOfStock + numberOfBond
	value = np.zeros(numberOfSec)
	value[0:4] = valueOfStock
	value[4:numberOfBond + 4] = valueOfBond
	value[numberOfBond + 4:numberOfSec] = valueOfStock

	return value

def factorCount(dataMat, weight=0.95):
	meanVals = np.mean(dataMat, axis=0)
	meanRemoved = dataMat - meanVals  # remove mean
	covMat = np.cov(meanRemoved, rowvar=0) # each column represents a variable, row is obs
	eigVals, eigVects = np.linalg.eig(np.mat(covMat))
	eigVals.sort()
	eigSum = eigVals.sum()
	temp = 0
	count = 0.0
	for i in range(len(eigVals) - 1, 0, -1):
		if temp <= weight * eigSum:
			temp += eigVals[i]
			count += 1
	# print eigVals.sum()
	return count


# =============================================================================
# RCP using Newton method, and only considering bonds, excluding stocks
# =============================================================================

def riskContributionNewton(nmlzWeight, lamda, sbRatio, covariance, numberOfBond, tol=1e-15):
	x = np.array(nmlzWeight)
	x = x[4:numberOfBond + 4]
	y = np.append(x, lamda)
	condition = 1
	sumOfBond = 0
	riskParityWeight = []
	maxIt = 100000
	itCount = 0
	cov = covariance[4:numberOfBond + 4, 4:numberOfBond + 4]


	while condition >= tol and itCount < maxIt:
		itCount += 1
		y_old = y
		x = np.array(y[:-1])
		lmd = y[-1]
		xInverse = 1. / x
		xSquareInverse = 1. / (x ** 2)
		F_temp = np.dot(cov, x) - lmd * xInverse
		F_temp = np.array([F_temp]).T
		F = np.vstack([F_temp, np.array([[sum(x) - (1 - sbRatio)]])])
		J_11 = cov + lmd * np.diag(xSquareInverse)
		J_11 = np.append(J_11, np.array([-xInverse]).T, axis=1)
		J_21 = np.ones(len(x))
		J_21 = np.append(J_21, 0)
		J = np.vstack([J_11, J_21])
		y = y - np.dot(inv(J), F).T
		y = y.flatten()

		condition = norm(y - y_old)

	print("Iteration counts: {}".format(itCount))
	riskParityWeight = y[:-1]

	print('==========\nriskParityWeight is:\n{}\n===========\n'.format(riskParityWeight))

	return riskParityWeight

# =============================================================================
# RCP using SQP optimization
# =============================================================================

def riskContributionSQP(objfunc, n, numberOfBond):
	y = []

	opt_prob = Optimization('Risk Contribution Parity Problem', objfunc)
	opt_prob.addVarGroup('x', n, 'c', lower=1e-6, upper=1000., value=0.1)
	opt_prob.addObj('f')
	opt_prob.addConGroup('g', n, 'e')
	# opt_prob.addCon('g', 'i')
	opt_prob.addConGroup('g', 2 + numberOfBond, 'i')

	# Instantiate Optimizer (PSQP) & Solve Problem
	psqp = PSQP()
	psqp.setOption('IPRINT', 0)
	psqp(opt_prob, sens_type='FD')
	s = opt_prob.solution(0)

	for i in range(n):
		y.append(s._variables[i].value)

	# print "y is : {}".format(y)
	x = array(y) / sum(y)
	riskParityWeight = x[4:numberOfBond + 4]

	return riskParityWeight

def riskContributionSQP_bochkam(objfunc, n, numberOfBond):
	y = []

	opt_prob = Optimization('Risk Contribution Parity Problem', objfunc)
	opt_prob.addVarGroup('x', n, 'c', lower=1e-6, upper=1000., value=0.1)
	opt_prob.addObj('f')
	opt_prob.addConGroup('g', n+1, 'e')
	# opt_prob.addCon('g', 'i')
	opt_prob.addConGroup('g', numberOfBond, 'i')

	# Instantiate Optimizer (PSQP) & Solve Problem
	psqp = PSQP()
	psqp.setOption('IPRINT', 0)
	psqp(opt_prob, sens_type='FD')
	s = opt_prob.solution(0)

	for i in range(n):
		y.append(s._variables[i].value)

	# print "y is : {}".format(y)
	x = array(y) / sum(y)
	riskParityWeight = x[4:numberOfBond + 4]

	return riskParityWeight
# =============================================================================
# RCP using SQP optimization for the case: sb_ratio = 0.0
# =============================================================================

def allBondRCPSQP(objfunc, numberOfBond):
	y = []

	opt_prob = Optimization('Risk Contribution Parity Problem', objfunc)
	opt_prob.addVarGroup('x', numberOfBond, 'c', lower=1e-6, upper=1000., value=0.1)
	opt_prob.addObj('f')
	opt_prob.addConGroup('g', 2 + numberOfBond, 'i')

	# Instantiate Optimizer (PSQP) & Solve Problem
	psqp = PSQP()
	psqp.setOption('IPRINT', 0)
	psqp(opt_prob, sens_type='FD')
	s = opt_prob.solution(0)

	for i in range(numberOfBond):
		y.append(s._variables[i].value)

	riskParityWeight = array(y) / sum(y)

	return riskParityWeight

def allBondRCPSQP_cinda(objfunc, numberOfBond):
	y = []

	opt_prob = Optimization('Risk Contribution Parity Problem', objfunc)
	opt_prob.addVarGroup('x', numberOfBond, 'c', lower=1e-6, upper=1000., value=0.1)
	opt_prob.addObj('f')
	opt_prob.addConGroup('g', 4 + numberOfBond, 'i')

	# Instantiate Optimizer (PSQP) & Solve Problem
	psqp = PSQP()
	psqp.setOption('IPRINT', 0)
	psqp(opt_prob, sens_type='FD')
	s = opt_prob.solution(0)

	for i in range(numberOfBond):
		y.append(s._variables[i].value)

	riskParityWeight = array(y) / sum(y)

	return riskParityWeight

def adjustValue(ratio):
	if ratio > 1.0 or ratio < 0.0:
		return "Wrong!"
	elif ratio >= 0.8:
		return 0.03
	elif ratio >= 0.5:
		return 0.02
	elif ratio >= 0.4:
		return 0.01
	else:
		return 0.0


def adjust_value_c(ratio):
	if ratio >= 0.4:
		return 0.05 * ratio  - 0.01
	else:
		return 0.


def get_buy_share_number(init_cap, opt_weight, price, board_lot, buffer):
	# input:
	# init_cap: initial capital; opt_weight: optimal weights; price: share prices; borad_lot: board lot; buffer: buffer.
	# output:
	# number of share to buy; drift.
	init_cap *= (1 - buffer)
	entry_price = price * board_lot
	opt_entry_num = opt_weight * init_cap / entry_price
	buy_entry_num = np.floor(opt_entry_num)
	entry_num_error = opt_entry_num - buy_entry_num

	map_index = sorted(range(len(entry_num_error)), key=lambda k: entry_num_error[k], reverse=True)

	cost = sum(buy_entry_num * entry_price)
	balance = init_cap - cost

	for iter in range(len(price)):
		if balance - entry_price[map_index[iter]] > buffer * init_cap:
			balance -= entry_price[map_index[iter]]
			buy_entry_num[map_index[iter]] += 1
		else:
			break

	buy_stock_num = buy_entry_num * board_lot
	buy_weight = buy_stock_num * price / init_cap
	error_weight = np.abs(buy_weight - opt_weight)
	drift = sum(error_weight) / 2
	return (buy_stock_num, drift)


def optimization_mean_cvar(obj_func, n, T, num_e):
	y = []

	opt_prob = Optimization('Mean-Variance Optimization', obj_func)
	opt_prob.addVarGroup('x', 1 + n + T, 'c', lower=0, upper=1., value=0.01)
	opt_prob.addObj('f')
	opt_prob.addConGroup('g', num_e, 'e')
	opt_prob.addConGroup('g', 3 * n + 2 * T, 'i')
	# print opt_prob

	psqp = PSQP()
	psqp.setOption('IPRINT', 0)
	psqp(opt_prob, sens_type='FD')
	# print(opt_prob.solution(0))

	s = opt_prob.solution(0)
	for i in np.arange(1, n + 1, 1):
		y.append(s._variables[i].value)
	var = s._variables[0].value
	cvar = s._objectives[0].value

	return (y, var, cvar)


def GetNent(Mu, w_0, w_b, S):
	s = np.sqrt((w_0 - w_b).dot(S).dot(w_0 - w_b))
	E, L, G = GenPCBasis(S, [])
	v_tilde = G.dot(w_0 - w_b)
	TE_contr = (v_tilde * v_tilde) / s
	R_2 = np.maximum(10 ** (-10), TE_contr / sum(TE_contr))
	Ne = np.exp(-R_2.dot(np.log(R_2)))
	return Ne


def GenPCBasis(S, A):
	if len(A) == 0:
		N = len(S)
		K = 0
		L0, E0 = np.linalg.eig(S)
		idx = L0.argsort()[::-1]
		L = L0[idx]
		E = E0[:, idx]
	else:
		# constraint
		print()

		K, N = A.shape
		E0 = np.array([])
		B = A
		for n in np.arange(N - K):
			if len(E0) != 0:
				B = A.append(E0.T.dot(S))
			e = GenFirstEigVect(S, B)
			E0 = E0.append(e)

		for n in np.arange(N - K + 1, N + 1, 1):
			B = E0.T.dot(S)
			e = GenFirstEigVect(S, B)
			E0 = E0.append(e)
			E = np.concatenate((E0[N - K + 1:], E0[:N - K]), axis=0)
			# swap order
			# E = [E(:, N - K + 1:N) E(:, 1:N - K)]

	L = np.diag(E.T.dot(S).dot(E))
	G = np.diag(np.sqrt(L)).dot(np.linalg.inv(E))
	G = G[K:N, :]

	return (E, L, G)


def GenFirstEigVect(S, A):
	N = len(S)
	Identity = np.diag(np.ones(N))
	P = Identity

	if np.linalg.matrix_rank(A) > 0:
		P = Identity - A.T.dot(np.inv(A.dot(A.T))).dot(A)

	[L0, E0] = np.linalg.eig(P.dot(S).dot(P.T))
	idx = L0.argsort()[::-1]
	e = E0[:, idx[0]]
	return e


def determinePosition(dt, date_array):
	if dt == date_array[0]:
		return date_array[0]
	elif dt >=  date_array[-1]:
		return date_array[-1]
	else:
		for i in range(len(date_array) -1):
			if dt >= date_array[i] and dt < date_array[i + 1]:
				return date_array[i]


def get_duration(ratio):
	return 3. - 6. * (0.5 - ratio)


def print_break(level=1, shape='-'):
	repeat = level * 30
	print(shape * repeat)


def print_init():
	print()
