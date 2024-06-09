from algorithm.fx_base import ForexStrategyBase
from utils.constants import *
import pandas as pd
import numpy as np
from datetime import datetime
import json


class Macro(ForexStrategyBase):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

		self.expected_return_all = pd.read_csv(self.expected_return_file, index_col=0)
		self.r_squared_all = pd.read_csv(self.r_squared_file, index_col=0)
		self.nice_param = json.load(open(self.param_file))

	def generate_weight(self, window_data, time_data, traded_ccp):
		expected_return = self.expected_return_all.loc[traded_ccp]
		r_squared = self.r_squared_all.loc[traded_ccp]
		expected_return.columns = pd.to_datetime(expected_return.columns, format='%Y-%m-%d')
		r_squared.columns = pd.to_datetime(r_squared.columns, format='%Y-%m-%d')

		# remove time
		expected_return.columns = [datetime(d.year, d.month, d.day) for d in expected_return.columns]
		r_squared.columns = [datetime(d.year, d.month, d.day) for d in r_squared.columns]

		nice_param_dict = self.nice_param
		symbols = traded_ccp

		current_ts = time_data[-1, 0]
		current_date = datetime.fromtimestamp(current_ts)

		signals = []
		for symbol in symbols:

			lb_window = int(nice_param_dict[symbol]['window'])
			EP_low = nice_param_dict[symbol]['EP_low']
			EP_high = nice_param_dict[symbol]['EP_high']
			r_sqr_threshold = nice_param_dict[symbol]['r_squared']

			self.logger.info(f'[{self.strategy_name}] | symbol: {symbol} | parameter: {nice_param_dict[symbol]}')

			model_dates = np.sort([d for d in expected_return.columns if d <= current_date])[-lb_window:]
			mean_exp_return = expected_return.loc[symbol, model_dates].mean()
			mean_r_sqr = r_squared.loc[symbol, model_dates].mean()
			self.logger.info(f'[{self.strategy_name}] | last model date: {model_dates[-1]}')

			if mean_exp_return > 0:
				if not np.isnan(mean_r_sqr) and mean_r_sqr > r_sqr_threshold:
					signal = 1
				else:
					signal = 0
			elif mean_exp_return < 0:
				if not np.isnan(mean_r_sqr) and mean_r_sqr > r_sqr_threshold:
					signal = -1
				else:
					signal = 0
			else:
				signal = 0
			signals.append(signal)

		normalized_weights = np.array(signals) / len(signals) * 2
		# print(pd.DataFrame(pd.Series(normalized_weights)).T.to_string(header=False))

		self.target_weight_df = pd.DataFrame(normalized_weights, columns=[current_date], index=traded_ccp)
		self.logger.info(f'[{self.strategy_name}] | 目标权重：\n{self.target_weight_df}')
		return normalized_weights


if __name__ == '__main__':
	macro = Macro(expected_return_file=f'{FX_RECORD_PATH}/expected_return.csv',
				  r_squared_file=f'{FX_RECORD_PATH}/r_squared.csv',
				  param_file=f'{FX_RECORD_PATH}/all_adjparam_meanLR.json')
