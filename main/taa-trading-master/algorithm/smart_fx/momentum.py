from algorithm.fx_base import ForexStrategyBase
from utils.constants import *
import pandas as pd
import numpy as np
from datetime import datetime


class Momentum(ForexStrategyBase):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

		self.rank_period = 75
		self.sample_period = 500
		self.t = 0.05
		self.g = 0.0
		self.mom_history = pd.read_csv(self.mom_file, index_col=0, parse_dates=True)

	def generate_weight(self, window_data, time_data, traded_ccp):
		# 获取历史数据
		prices = window_data[:, :, 0]
		mom_record = self.mom_history.values
		n_traded_ccp = len(traded_ccp)
		today = datetime.strptime('/'.join(time_data[-1][-3:].astype(int).astype(str)), '%Y/%m/%d')

		# 计算不同跨度的动量
		window_list = np.array([20, 30, 40, 50, 60, 70])
		ic_list = []
		for window_i in window_list:
			x = prices[-self.sample_period - 1: -1] / prices[-self.sample_period - window_i - 1: -window_i - 1] - 1.
			y = prices[-self.sample_period:] / prices[-self.sample_period - 1: -1] - 1.
			ic_list.append(np.corrcoef(x.flatten(), y.flatten())[0][1])

		# 得到最优动量跨度
		ic_list = np.array(ic_list)
		best_window = window_list[ic_list == ic_list.max()][0]
		self.logger.info(f'[{self.strategy_name}] | 重要参数：best_window = {best_window},'
						 f'rank_period = {self.rank_period}, sample_period = {self.sample_period}')

		mom = prices[-1] / prices[-best_window] - 1.
		mom_record = np.vstack((mom_record, mom))
		self.mom_df = pd.DataFrame(mom, columns=[today], index=traded_ccp)
		self._update_mom_record()
		signal = np.sign(mom) / n_traded_ccp

		# 根据波动率配权
		returns = prices[-75:] / prices[-76: -1] - 1.
		volatility = returns.std(axis=0)
		stability = 1. / volatility
		stability[stability <= np.median(stability)] = 0
		sigma_weight = stability / stability.sum()

		if mom_record.shape[0] > 40:
			mom_rank = mom_record[-self.rank_period:].argsort(axis=0).argsort(axis=0)
			current_level = mom_rank[-1] / (mom_rank.shape[0] - 1.)

			signal = np.zeros(n_traded_ccp)
			signal[(current_level >= 0.5 + self.t) & (current_level <= 1.0 - self.g)] = 1.
			signal[(current_level <= 0.5 - self.t) & (current_level >= self.g)] = -1.
		else:
			self.logger.info(f'[{self.strategy_name}] | 动量历史样本不足，现有{mom_record.shape[0]}个样本')

		signal = signal * sigma_weight
		target_weight = signal * 2

		self.target_weight_df = pd.DataFrame(target_weight, columns=[today], index=traded_ccp)
		self.logger.info(f'[{self.strategy_name}] | 目标权重：\n{self.target_weight_df}')
		return target_weight

	def _update_mom_record(self):
		if self.mom_df.T.index.values not in self.mom_history.index.values:
			self.mom_history = self.mom_history.append(self.mom_df.T).fillna(0.)
			self.mom_history.to_csv(self.mom_file)



if __name__ == '__main__':
	mom = Momentum(mom_file=f'{FX_RECORD_PATH}/mom_history.csv')