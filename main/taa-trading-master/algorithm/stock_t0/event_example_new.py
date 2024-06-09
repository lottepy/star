#!/usr/bin/env python

import collections
import json
import logging
import sys
import time
import threading
import numpy as np
from datetime import datetime
from lib.commonalgo.execution.strategy.event_strategy import EventExecutor, EventStrategyBase

SAMPLE_INTERVAL = 3
MAXLEN = 200
WINDOW_MIN = 3
TS = 0
LAST_PRICE = 3
BID_1 = 4
BV_1 = 9
ASK_1 = 14
AV_1 = 19
LAST_VOL = 2

#store history data
class HistoryData():

	def __init__(self):
		self.n_field = 24
		self.history_data = np.zeros((1, self.n_field))
		self.last_data = self.history_data[-1]
		self.window_min = 3
		self.start = False

	def update_data(self, data):
		is_abnormal = True
		# last_vol、vol、last
		if data['MsgType'] == 'tick':
			timestamp = data.get('timestamp') // 1000
			if not timestamp:
				return is_abnormal
			tick_list = []
			for field in ['last_vol', 'vol', 'last']:
				item = data.get(field)
				# get不到，说明数据有问题
				if not item:
					return is_abnormal
				tick_list.append(item)
			if timestamp <= self.last_data[0]:
				self.last_data[[1, 2, 3]] = tick_list
			else:
				self.last_data = np.copy(self.last_data)
				self.last_data[[0] + [1, 2, 3]] = [timestamp] + tick_list
				self.history_data = np.vstack([self.history_data, self.last_data])
			is_abnormal = False

		# b1-b5, bv1-bv5, a1-a5, av1-av5
		elif data['MsgType'] == 'orderbook':
			timestamp = data.get('timestamp', int(time.time() * 1000)) // 1000
			if not timestamp:
				return is_abnormal
			orderbook_list = []
			for field in ['b', 'bv', 'a', 'av']:
				for order in range(1, 6):
					item = data.get(f'{field}{order}')
					if not item:
						return is_abnormal
					orderbook_list.append(item)
			if timestamp <= self.last_data[0]:
				self.last_data[list(np.arange(4, 24))] = orderbook_list
			else:
				self.last_data = np.copy(self.last_data)
				self.last_data[[0] + list(np.arange(4, 24))] = [timestamp] + orderbook_list
				self.history_data = np.vstack([self.history_data, self.last_data])
			is_abnormal = False
		else:
			self.logger.info(f'receive metadata:\n{data}')
			return is_abnormal

		self.last_data = self.history_data[-1]
		print(f"last_data:\n{self.last_data}")
		times = self.history_data[:, TS]
		start_time = times[-1] - self.window_min * 60

		# 当倒数第二个点的时间在窗口之外的时候截取
		if len(times) > 1:
			if times[1] < start_time:
				self.history_data = self.history_data[1:]
				self.start = True
		self.time_axis = self.history_data[:, TS]
		return is_abnormal




class BaseStrategy():
	""" 其他strategy继承BaseStrategy，必须重写on_tick方法 """

	def on_tick(self, parameter_list):
		""" 在收到行情时触发 """
		raise NotImplementedError


class OtherStrategy(BaseStrategy):
	def on_tick(self):
		pass


class MAStrategy(BaseStrategy):
	""" 最近N分钟的滑动平均 """

	def __init__(self, recent_queue: collections.deque, minute: int):
		self.recent_queue = recent_queue  # 均匀采样的价格
		self.minute = minute
		self._sample_points = int(self.minute * 60 / SAMPLE_INTERVAL)  # 从最近多少个采样点取计算MA
		self.ma_queue = collections.deque(maxlen=2)  # 不均匀

	def on_tick(self):
		# 每次tick时 都重新计算MA (需要累计超过n个数据才开始计算)
		if len(self.recent_queue) > self._sample_points:
			_ma = sum(list(self.recent_queue)[-self._sample_points:]) / self._sample_points  # 计算的效率不高 仅为example
			self.ma_queue.append(_ma)

		if len(self.ma_queue) >= 2 and len(self.recent_queue) >= 2:
			# 价格上破MA 则买入
			if (self.recent_queue[-1] > self.ma_queue[-1]) and (self.recent_queue[-2] <= self.ma_queue[-2]):
				print(f'BUY signal from {self.__class__.__name__} with parameter {self.minute}')
				return {'side': 'BUY', 'type': 'LIMIT'}
			elif (self.recent_queue[-1] < self.ma_queue[-1]) and (self.recent_queue[-2] >= self.ma_queue[-2]):
				print(f'SELL signal from {self.__class__.__name__} with parameter {self.minute}')
				return {'side': 'SELL', 'type': 'LIMIT'}


class KnifeStrategy(BaseStrategy):
	def __init__(self, history_data, param, executor, iuid):
		self.history_data = history_data
		for key in param:
			setattr(self, key, param[key])
		self.executor = executor
		self.iuid = iuid
		self.cut_loss_ts = 0.
		self.trade_info = [0., 0.]

	def on_tick(self):
		# 获取现金、持仓等信息
		self.data = self.history_data.history_data
		self.time_axis = self.data[:, TS]

		print(f'数据长度：{self.data.shape}')
		lot_size = self.executor.lot_size.get(self.iuid, 100)
		cash = self.executor.purches_power
		current_position = self.executor.current_holdings[self.iuid]

		# 获取当前时间，确定时间窗口
		current_ts = self.time_axis[-1]
		lookback_sec = self.lookback_sec
		start_ts = current_ts - lookback_sec
		start_idx = np.where(self.time_axis >= start_ts)[0][0]

		previous_start_ts = start_ts - lookback_sec
		previous_start_idx = np.where(self.time_axis >= previous_start_ts)[0][0]

		print(f'start_idx = {start_idx}, '
			  f'previous_start_idx = {previous_start_idx}')

		# 获取策略所需价格数据
		ask_price = self.data[:, ASK_1]
		bid_price = self.data[:, BID_1]
		last_price = self.data[-1, LAST_PRICE]
		start_price = self.data[start_idx, LAST_PRICE]

		ask_quantity = self.data[-1, AV_1]
		bid_quantity = self.data[-1, BV_1]

		# 获取策略所需成交量和盘口数据
		ask_quantity = self.data[-1, AV_1: AV_1 + 5].sum(axis=0)
		bid_quantity = self.data[-1, BV_1: BV_1 + 5].sum(axis=0)
		current_volume = self.data[start_idx:, LAST_VOL].sum(axis=1)
		previous_volume = self.data[previous_start_idx: start_idx, LAST_VOL].sum(axis=1)

		returns = last_price / start_price - 1.
		if ask_quantity != 0.:
			imbalance = bid_quantity / ask_quantity
		else:
			imbalance = 0.
		if previous_volume != 0.:
			volume_change = current_volume / previous_volume
		else:
			volume_change = 1e3
		time_from_cut_loss = current_ts - self.cut_loss_ts

		return_cond = self.up_return_threshold
		imbalance_cond = self.imbalance_threshold
		volume_change_cond = self.vol_change_threshold
		safe_trade_cond = 1200

		filter_result = (returns < return_cond) \
						* (imbalance > imbalance_cond) \
						* (volume_change < volume_change_cond) \
						* (time_from_cut_loss > safe_trade_cond) \
						* (ask_price > bid_price)

		target_weight = filter_result.astype(int)
		target_position = target_weight * cash // ask_price // lot_size * lot_size

		if current_position > 0.1:
			# 目前有仓位
			# last_trade_info = [上次委托时间戳，上次委托价，上次委托量]
			last_trade_ts = self.last_trade_ts
			holding_period = (current_ts - last_trade_ts) / 60
			period_return = bid_price / self.trade_info[1] - 1.

			cut_loss_level = -0.01
			cover_condition = (
				holding_period > self.max_hold_min
				or (period_return < cut_loss_level)
			)

			if cover_condition:
				# 满足平仓条件，准备平仓单
				if period_return < cut_loss_level:
					# 触发了止损条件，短时间内无法交易
					self.cut_loss_ts = current_ts
				order_position = self.trade_info[2]
				order = {
					"side": "SELL",
					"type": "LIMIT",
					"quantity": order_position,
					"price": bid_price
				}
				return order
		elif target_position > 0:
			# 目前空仓
			order_position = target_position - current_position
			order = {
				"side": "BUY",
				"type": "LIMIT",
				"quantity": order_position,
				"price": ask_price
			}
			self.trade_info = [current_ts, ask_price, order_position]
			return order


class EventStrategyExample(EventStrategyBase):
	def __init__(self, opts, t0_params):
		super().__init__(opts)
		# self.sell = False
		self.last_trade = 0
		self.trade_interval = 40

		# store history array data
		self.history_data = HistoryData()

		self.recent_queue = collections.deque(maxlen=MAXLEN)  # 用几秒采样一次的方法 采样最近多少个的last price
		self.data_list = []  # 保存服务器返回原封不动的data 用作debug

		# 读取json的配置，添加不同参数的不同strategy到self.strategy_list
		t0_param = t0_params[self.iuid]
		self.strategy_list = []
		for strategy in t0_param:  # strategy例如{"MAStrategy": {"minute": 3}}
			for strategy_name, param in strategy.items():
				if strategy_name == 'MAStrategy':
					self.strategy_list.append(
						MAStrategy(
							recent_queue=self.recent_queue,
							**param
						)
					)
				elif strategy_name == 'OtherStrategy':
					self.strategy_list.append(OtherStrategy(**param))
				elif strategy_name == 'KnifeStrategy':
					self.strategy_list.append(
						KnifeStrategy(
							history_data=self.history_data,
							param=param,
							executor=self.executor,
							iuid=self.iuid
						)
					)

		save_price_thread = threading.Thread(
			target=self.save_price_every,
			name='SavePriceThread',
			daemon=True
		)
		save_price_thread.start()

	def save_price_every(self):
		while True:
			time.sleep(SAMPLE_INTERVAL)
			if 'last' in self.tick_data:
				last_price = self.tick_data['last']
				self.recent_queue.append(last_price)
			# self.logger.debug(f'last price {last_price}')


	def on_tick(self, data):
		# data 的格式  可能有几个时间戳 以timestamp为准 可能有一些data没有timestamp属性
		# {'last_vol': 200, 'last': 342.8, 'vol': 5888827,
		#  'timestamp': 1568792489000, 'Type': 'Update',
		#  'OnMsgTime': 1568792489861, 'PublishTime': 1568792489863,
		#  'iuid': 'HK_10_700', 'MsgType': 'tick'}
		print(f"data\n{data}")
		ts = datetime.now()
		is_abnormal = self.history_data.update_data(data)
		te = datetime.now()
		print(f"time for preparing data: {te-ts}")

		start = self.history_data.start
		if not start:
			print("历史数据窗口不够长，不发单")
			return

		if is_abnormal:
			print("异常数据，不发单")
			return

		if data['MsgType'] == 'orderbook':
			self.orderbook_data.update(data)

		elif data['MsgType'] == 'tick':
			self.tick_data.update(data)
			self.data_list.append(data)

			t0 = time.time()
			# strategy如何知道current_holding, 需要传executor给strategy吗
			for strategy in self.strategy_list:
				order = strategy.on_tick()
				if order:
					break  # 如果其中一个有信号 就不再执行其他strategy
			t1 = time.time()
			print('time cost for calc', t1 - t0)  # 理论上这个计算的时间不能太长 不然会导致阻塞

			if order:
				order['symbol'] = self.iuid
				order['price'] = self.orderbook_data['a1'] if order['side'] == 'BUY' else self.orderbook_data['b1']
				order['quantity'] = self.executor.lot_size.get(self.iuid, 500)
				self.logger.info(f'*******!!!!**********{order}')

				if self.executor.execution_status != "Idle":
					self.logger.debug(f"Execution_status: {self.executor.execution_status}")
					return
				now = time.time()
				if now - self.last_trade < self.trade_interval:
					self.logger.info('do not trade too often')
					return
				self.last_trade = now

				self.executor.submit_order(order)


if __name__ == '__main__':
	# args = ['--iuid', 'HK_10_700', '-cp', 'config_event.ini']

	#args = ['--iuid', 'CN_10_000001', '-cp', 'config.ini']
	args = ['--iuid', 'HC_10_000001', '-cp', 'config_ib.ini']

	logger = logging.getLogger(__name__)
	logger.info("Running test_strategy")
	parser = EventStrategyExample.get_parser()
	opts = parser.parse_args(args)

	t0_params = json.load(open('t0_params.json'))
	strategy = EventStrategyExample(opts, t0_params)

	strategy.run_forever()
