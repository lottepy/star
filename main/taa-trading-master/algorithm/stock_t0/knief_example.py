#!/usr/bin/env python
import sys
import logging
import time
import numpy as np
from datetime import datetime
from lib.commonalgo.execution.strategy.event_strategy import EventStrategyBase, EventExecutor, go
import argparse

TS = 0
LAST_PRICE = 3
BID_1 = 4
BV_1 = 9
ASK_1 = 14
AV_1 = 19


class EventStrategyExample(EventStrategyBase):
	def __init__(self, opts):
		super().__init__(opts)
		self.sell = False
		self.last_trade = 0
		self.trade_interval = 0

		###
		self.data = np.zeros(24)
		self.last_data = self.data
		self.window_min = 3

		self.lookback_sec = 60
		self.up_return_threshold = -0.012
		self.imbalance_threshold = 300
		self.max_hold_min = 3

	def _get_executor(self, ):
		return EventExecutor(self._opts, self.iuid, self.on_tick,
							 on_trade=self.on_trade, )

	def on_tick(self, data):
		# self.tick_data.update(data)
		self.logger.info(f"data: \n{data}")
		# data 有不同的消息类型： tick、orderbook、metadata
		# orderbook和tick可能会有不同步的情况，因此要跟据timestamp判断先后
		# timestamp、last_vol、vol、last
		if data['MsgType'] == 'tick':
			timestamp = data.get('timestamp') // 1000
			tick_list = []
			for field in ['last_vol', 'vol', 'last']:
				item = data.get(field)
				if not item:
					return
				tick_list.append(item)
			if timestamp <= self.last_data[0]:
				self.last_data[[1, 2, 3]] = tick_list
			else:
				self.last_data = np.copy(self.last_data)
				self.last_data[[0] + [1, 2, 3]] = [timestamp] + tick_list
				self.data = np.vstack([self.data, self.last_data])

		elif data['MsgType'] == 'orderbook':
			# b1-b5, bv1-bv5, a1-a5, av1-av5
			orderbook_list = []
			for field in ['b', 'bv', 'a', 'av']:
				for order in range(1, 6):
					item = data.get(f'{field}{order}')
					if not item:
						return
					orderbook_list.append(item)
			timestamp = data.get('timestamp') // 1000
			if timestamp <= self.last_data[0]:
				self.last_data[list(np.arange(4, 24))] = orderbook_list
			else:
				self.last_data = np.copy(self.last_data)
				self.last_data[[0] + list(np.arange(4, 24))] = [timestamp] + orderbook_list
				self.data = np.vstack([self.data, self.last_data])
		else:
			# Meta Data,暂未考虑
			self.logger.info(f'data:\n{data}')
			return

		self.last_data = self.data[-1]
		self.logger.info(f"last_data:\n{self.last_data}")

		# 得到需要的时间长度的array(如何避免数据过短)
		times = self.data[:, TS]
		start_time = times[-1] - self.window_min * 60
		start_idx = np.where(times >= start_time)[0][0]
		self.data = self.data[start_idx:]
		self.time_axis = self.data[:, TS]

		# 订单还没有处理完, 不发单
		if self.executor.execution_status != "Idle":
			self.logger.debug(f"Execution_status: {self.executor.execution_status}")
			return

		now = time.time()
		if now - self.last_trade < self.trade_interval:
			return
		self.last_trade = now

		lot_size = self.executor.lot_size.get(self.iuid, 100)
		self.logger.debug(f"lotsize: {lot_size}")

		current_ts = self.time_axis[-1]

		lookback_sec = self.lookback_sec
		start_ts = current_ts - lookback_sec
		start_idx = np.where(self.time_axis >= start_ts)[0][0]

		cash = self.executor.purches_power
		self.logger.info(f"cash:{self.executor._current_cash}")
		self.logger.info(f"purches_power:{cash}")

		ask_price = self.data[:, ASK_1]
		bid_price = self.data[:, BID_1]
		# 暂时好像没有last_price

		last_price = self.data[-1, LAST_PRICE]
		start_price = self.data[start_idx, LAST_PRICE]
		ask_quantity = self.data[-1, AV_1]
		bid_quantity = self.data[-1, BV_1]

		returns = last_price / start_price - 1.
		imbalance = bid_quantity - ask_quantity
		self.logger.info(f"start_time:{datetime.fromtimestamp(self.time_axis[start_idx])}")
		self.logger.info(f"current_time:{datetime.fromtimestamp(current_ts)}")
		self.logger.info(f"last_price: {last_price}")
		self.logger.info(f"return:{returns}   imbalance{imbalance}")

		up_return_threshold = self.up_return_threshold
		imbalance_threshold = self.imbalance_threshold

		filter_result = (returns < up_return_threshold) * (imbalance > imbalance_threshold)

		self.logger.info(f"return:{returns < up_return_threshold}   imbalance{imbalance > imbalance_threshold}")

		target_weight = filter_result.astype(int)
		target_position = target_weight * cash // last_price // lot_size * lot_size

		self.logger.info(f"target_position:{target_position}")
		current_position = self.executor.current_holdings[self.iuid]
		self.logger.info(f"current_position:{current_position}")

		if current_position > 0:
			last_trade_ts = self.last_trade_ts
			holding_period = (current_ts - last_trade_ts) / 60
			cover_condition = (holding_period > self.max_hold_min)
			if cover_condition:
				order = {"symbol": self.iuid,
						 "side": "SELL",
						 "type": "LIMIT",
						 "price": bid_price[-1],
						 "quantity": current_position,
						 }
				self.executor.submit_order(order)
		elif target_position > 0:
			order = {"symbol": self.iuid,
					 "side": "BUY",
					 "type": "LIMIT",
					 "price": ask_price[-1],
					 "quantity": target_position,
					 }
			self.last_trade_ts = current_ts
			self.executor.submit_order(order)


if __name__ == '__main__':
	logger = logging.getLogger(__name__)
	logger.info("Running test_strategy")
	parser = EventStrategyExample.get_parser()
	opts = parser.parse_args(sys.argv[1:])

	opts = argparse.Namespace()
	# IB 测试
	# opts.config_path = './config_ib.ini'
	# opts.env = 'Default'
	# opts.iuid = 'HC_10_000001'
	# opts.local = True

	# KR 测试
	opts.config_path = './config.ini'
	opts.env = 'KR'
	opts.iuid = 'CN_10_000001'
	opts.local = True

	strategy = EventStrategyExample(opts)

	strategy.run_forever()
