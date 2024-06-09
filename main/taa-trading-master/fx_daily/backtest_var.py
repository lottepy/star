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

cp = ConfigManager('data/config/config.ini')

SAVE_PATH = cp.configs['save_path']
DISPLAY = cp.configs['display']
STRATEGY_NAME = cp.configs['strategy_name']


if __name__ == '__main__':

	weight_df = pd.read_csv(
		'D:/Magnum/Research/FX/weight_df_20191120/weight_equal_weighted.csv',
		index_col=0,
		parse_dates=True
	)
	weight_dates = weight_df.index.values

	var_summary = []
	for i, d in enumerate(weight_dates):
		temp = weight_df.loc[d].dropna()
		weight = temp.values
		symbol = temp.index.values.tolist()
		end = pd.Timestamp(d)
		start = end - timedelta(days=500)

		# parse_path = "//192.168.9.170/share/alioss/0_DymonFx/parse_data"
		ENV = os.getenv('DYMON_FX_ENV', 'dev_algo')
		parse_path = {
			'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/parse_data/",
			'dev_jeff': '../0_DymonFx/parse_data/',
			'live': '../0_DymonFx/parse_data/'
		}[ENV]

		FxArray, FxUSDArray, RateArray, timeArray, ccy_matrix_df = load_data(parse_path, symbol, start, end)

		ccy_matrix = ccy_matrix_df.values
		ccp = ccy_matrix_df.columns
		ccy = ccy_matrix_df.index

		# print(f'currency pair has {ccp.tolist()}')
		# print(f'currency has {ccy.tolist()}')

		strategy_params = [weight]
		ts = datetime.now()
		simple_return, ccp_return, status = run_once(FxArray, FxUSDArray, RateArray, timeArray, ccy_matrix, strategy_params)
		te = datetime.now()

		status_df = pd.DataFrame(status, columns=['time'] + [f'{symbol}_pv' for symbol in ccp] + ['pv']
												 + [f'{symbol}_position' for symbol in ccy]
												 + ['weight'])
		var = status_df['pv'].pct_change().dropna().quantile(0.01)
		var_summary.append([end, var])
		print(f'回测用时{te - ts}, 回测时段：{start}~{end}, VaR={var}')

	var_df = pd.DataFrame(var_summary, columns=['Date', 'Historical VaR'])
	var_df.to_csv(os.path.join(SAVE_PATH, 'historical_var.csv'))