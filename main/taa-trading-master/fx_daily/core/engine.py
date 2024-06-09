from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import itertools
import os

from fx_daily.data.data_reader.fx_daily_reader import load_data
from fx_daily.data.config.config import ConfigManager
from fx_daily.core.backtest import run_once
from fx_daily.utils.constants import *


class ForexEngine(object):
	def __init__(self, config_file, start, end, symbols=None, params=None, mode='single', weight_file=None):
		self.config = ConfigManager(config_file)
		self.start = start
		self.end = end
		self.mode = mode

		self._load_config()

		if isinstance(symbols, list) and isinstance(params, np.ndarray):
			self.symbols = symbols
			self.params = params
		else:
			self._get_defaults()

		if self.mode == 'var':
			assert '.csv' in weight_file, '必须传入合法权重文件才能进行VaR计算'
			self.var_weight = pd.read_csv(os.path.join(self.WEIGHT_PATH, weight_file), index_col=0, parse_dates=True)
			self.weight_dates = self.var_weight.index.values
			self.symbols = self.var_weight.columns.values.tolist()

	def _load_config(self):
		self.WINDOW_SIZE = self.config.configs['window_size']
		self.SAVE_PATH = self.config.configs['save_path']
		self.WEIGHT_PATH = self.config.configs['weight_path']
		self.DISPLAY = self.config.configs['display']
		self.STRATEGY_NAME = self.config.configs['strategy_name']
		self.COMMISSION = self.config.configs['commission']
		self.PARAM_NAME = STRATEGY_NAME_MAP[self.STRATEGY_NAME]

	def _get_defaults(self):
		if self.STRATEGY_NAME == 'mean_reversion':
			# mean-reversion策略参数：[alpha, rho, window, N]
			self.symbols = ALL_CCY_NO_CROSS
			self.params = [0.3, 0.04, 10, 5]
		else:
			# momentum策略参数：[M, P]
			self.symbols = ALL_CCY_NO_CROSS
			self.params = [100, 250]

	def _init_var_test(self):
		pass

	def _suggest_first_bt_date(self, time_array):
		first_back_date = datetime.fromtimestamp(time_array[self.WINDOW_SIZE - 1, 0])
		first_nonan_back_date = datetime.fromtimestamp(time_array[2 * self.WINDOW_SIZE - 2, 0])
		print(f'you can backtest after {first_back_date}')
		print(f'if you dont want nan in your lookback window, you should start after {first_nonan_back_date}')
		assert self.start >= first_back_date, f"suggest you backtest after {first_back_date} to avoid nan"

	def _get_bt_index(self, time_array):
		back_start_timestamp = datetime.timestamp(self.start)
		back_end_timestamp = datetime.timestamp(self.end)
		back_start_index = np.where(time_array[:, 0] >= back_start_timestamp)[0][0]
		back_end_index = np.where(time_array[:, 0] <= back_end_timestamp)[0][-1]
		back_index = np.array([back_start_index, back_end_index])
		return back_index

	def _get_ccy_ccp_map(self, ccy_matrix_df):
		ccy_matrix = ccy_matrix_df.values
		ccp = ccy_matrix_df.columns
		ccy = ccy_matrix_df.index
		return ccy_matrix, ccp, ccy

	def _display(self, simple_return, ccp_return, status):
		status_df = pd.DataFrame(status, columns=['time'] + [f'{self.symbols}_pv' for symbol in self.ccp] + ['pv']
												 + [f'{self.symbols}_position' for symbol in self.ccy] + ['weight'])
		status_df['date'] = status_df['time'].apply(lambda x: datetime.fromtimestamp(x).date())
		status_df.set_index('date', inplace=True)
		status_df['pv'].plot()
		plt.show()
		status_df.to_csv(os.path.join(self.SAVE_PATH, 'single_param.csv'), index=True)

	def _post_backtest(self):
		pass

	def run(self):
		assert self.mode in ['single', 'multi', 'var', 'var_once'], f'回测模式不可为{self.mode}，请修改'

		FxArray, FxUSDArray, RateArray, timeArray, ccy_matrix_df = load_data(PARSE_PATH, self.symbols, self.WINDOW_SIZE)
		self._suggest_first_bt_date(timeArray)
		bt_index = self._get_bt_index(timeArray)
		ccy_matrix, self.ccp, self.ccy = self._get_ccy_ccp_map(ccy_matrix_df)

		if self.mode == 'multi':
			# 多次回测
			self.result = []
			for i, param in enumerate(self.params):
				ts = datetime.now()
				simple_return, ccp_return, status = run_once(FxArray, FxUSDArray, RateArray, timeArray, ccy_matrix,
															 param, bt_index, self.config)
				te = datetime.now()
				print(f'回测用时{te - ts}, param={dict(zip(self.PARAM_NAME, param))}, simple return={simple_return}')
				self.result.append(list(param) + [simple_return])

			result_df = pd.DataFrame(self.result, columns=self.PARAM_NAME + ['Return'])
			result_df.to_csv(os.path.join(self.SAVE_PATH, f'param_opt_{self.STRATEGY_NAME}.csv'))
		elif self.mode == 'single':
			# 单次回测
			ts = datetime.now()
			simple_return, ccp_return, status = run_once(FxArray, FxUSDArray, RateArray, timeArray, ccy_matrix,
														 self.params, bt_index, self.config)
			te = datetime.now()

			if self.DISPLAY == 1:
				self._display(simple_return, ccp_return, status)
			print(f'回测用时{te - ts}, simple return={simple_return}')
		elif self.mode == 'var':
			# 计算Historical VaR时间序列
			self.var_summary = []
			for i, d in enumerate(self.weight_dates):
				temp = self.var_weight.loc[d].dropna()
				weight = temp.values
				self.end = pd.Timestamp(d)
				self.start = self.end - timedelta(days=500)
				bt_index = self._get_bt_index(timeArray)
				param = [weight]

				ts = datetime.now()
				simple_return, ccp_return, status = run_once(FxArray, FxUSDArray, RateArray, timeArray, ccy_matrix,
															 param, bt_index, self.config)
				te = datetime.now()

				status_df = pd.DataFrame(status, columns=['time'] + [f'{self.symbols}_pv' for symbol in self.ccp] + ['pv']
														 + [f'{self.symbols}_position' for symbol in self.ccy]
														 + ['weight'])
				var = status_df['pv'].pct_change().dropna().quantile(0.01)
				self.var_summary.append([self.end, var])
				print(f'回测用时{te - ts}, 回测时段：{self.start}~{self.end}, VaR={var}')
			var_df = pd.DataFrame(self.var_summary, columns=['Date', 'Historical VaR'])
			var_df.to_csv(os.path.join(self.SAVE_PATH, 'historical_var.csv'))
		elif self.mode == 'var_once':
			# 计算一次权重的Historical VaR
			self.start = self.end - timedelta(days=500)
			bt_index = self._get_bt_index(timeArray)
			param = [self.params]

			ts = datetime.now()
			simple_return, ccp_return, status = run_once(FxArray, FxUSDArray, RateArray, timeArray, ccy_matrix,
														 param, bt_index, self.config)
			te = datetime.now()

			status_df = pd.DataFrame(status, columns=['time'] + [f'{self.symbols}_pv' for symbol in self.ccp] + ['pv']
													 + [f'{self.symbols}_position' for symbol in self.ccy]
													 + ['weight'])
			var = status_df['pv'].pct_change().dropna().quantile(0.01)
			print(f'回测用时{te - ts}, 回测时段：{self.start}~{self.end}, VaR={var}')
			return var

	@staticmethod
	def make_params(params):
		pass


if __name__ == '__main__':
	strategy_params = np.random.randn(len(ALL_CCY_NO_CROSS))

	fx_bt = ForexEngine(
		config_file='../data/config/config_var.ini',
		start=datetime(2011, 12, 31),
		end=datetime(2019, 12, 4),
		symbols=ALL_CCY_NO_CROSS,
		params=strategy_params,
		mode='var_once'
	)
	var = fx_bt.run()
	print(var)