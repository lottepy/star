import numpy as np
from math import sqrt
from numpy.linalg import inv, norm
# from pyOpt import Optimization
# from pyOpt.pyPSQP import PSQP
from numpy import dot, sqrt, log, array
import pandas as pd
import sys
from os.path import dirname, abspath, join

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