from algorithm.fx_base import ForexStrategyBase
from utils.constants import *
import pandas as pd
import numpy as np
from datetime import datetime


class MeanReversion(ForexStrategyBase):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

		self.alpha = 0.1
		self.rho = 0.06
		self.w = 30
		self.n_underlying = 4
		self.weight_record = pd.read_csv(self.weight_file, index_col=0, parse_dates=True)

	def generate_weight(self, window_data, time_data, traded_ccp):
		# 获取历史数据
		prices = window_data[-self.w:, :, 0]
		last_weight = self.weight_record.iloc[-1][traded_ccp].values
		n_traded_ccp = len(traded_ccp)
		today = datetime.strptime('/'.join(time_data[-1][-3:].astype(int).astype(str)), '%Y/%m/%d')

		# 计算预期收益
		predict_return = np.ones(n_traded_ccp)
		for i in range(0, self.w - 1):
			x_t = prices[i + 1] / prices[i]
			predict_return = MeanReversion.compute_ema(self.alpha, x_t, predict_return)
		self.logger.info(f'[{self.strategy_name}] | 预期收益（回归强度）：{dict(zip(traded_ccp, predict_return))}')

		difference = predict_return.max() - predict_return.min()
		r = -1 if not np.any(last_weight) else self.rho
		self.logger.info(f'[{self.strategy_name}] | 重要参数：difference = {difference}, threshold = {r},'
						 f'alpha = {self.alpha}, window = {self.w}, n_underlying = {self.n_underlying}')

		target_weight = np.zeros(n_traded_ccp)
		if difference > r:
			target_weight[predict_return.argsort().argsort() >= (n_traded_ccp - self.n_underlying)] = 0.5 / self.n_underlying
			target_weight[predict_return.argsort().argsort() <= (self.n_underlying - 1)] = -0.5 / self.n_underlying
		else:
			target_weight = last_weight

		self.target_weight_df = pd.DataFrame(target_weight, columns=[today], index=traded_ccp)
		self._update_weight_record()
		return target_weight

	def _update_weight_record(self):
		self.logger.info(f'[{self.strategy_name}] | 目标权重：\n{self.target_weight_df}')
		if self.target_weight_df.T.index.values not in self.weight_record.index.values:
			self.weight_record = self.weight_record.append(self.target_weight_df.T).fillna(0.)
			self.weight_record.to_csv(self.weight_file)

	@staticmethod
	def compute_ema(alpha, x_t, xt_head):
		return alpha + (1 - alpha) * np.divide(xt_head, x_t)


if __name__ == '__main__':
	mr = MeanReversion(weight_file=f'{FX_RECORD_PATH}/mr_weight.csv')