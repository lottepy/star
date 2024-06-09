import pandas as pd
import matplotlib.pyplot as plt
import os
import seaborn as sns
sns.set_style('whitegrid')

from fx_daily.utils.constants import *

VAR_CAP = 0.01


def var_adjusted_bt(bt_path):
	multiplier = 0.9
	bt_result = pd.read_csv(bt_path, index_col=0, parse_dates=True)
	bt_result['dr'] = bt_result['pv'].pct_change().fillna(0.)
	bt_result['dr_adjusted'] = bt_result['dr'] * abs(bt_result['var']).rdiv(VAR_CAP * 0.9)
	bt_result['pv_adjusted'] = bt_result['dr_adjusted'].add(1.).cumprod()

	bt_result.to_csv(os.path.join(WORKING_PATH, 'var_adjusted_bt.csv'))
	bt_result[['pv', 'pv_adjusted']].plot()
	plt.show()


if __name__ == '__main__':
	var_adjusted_bt(os.path.join(WORKING_PATH, 'bt_result_v2.csv'))
