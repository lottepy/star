import pandas as pd
import math
from datetime import datetime
def str_list_to_time_iso(time_input_list):
    time_output_list = []
    for time_input in time_input_list:
        if type(time_input) is str:
            time_output = datetime.strptime(time_input, '%Y-%m-%d')
        else:
            time_output = time_input
        time_output_list.append(time_output)

    return time_output_list


def calculate_2Y_vol(pv, benchmark):
    benchmark = benchmark.merge(pv, how='right', left_index=True, right_index=True).ffill()[benchmark.columns]
    pv.loc[:,pv.columns.str.contains('benchmark')] = benchmark.values
    pv.index = str_list_to_time_iso(list(pv.index))
    pv = pv / pv.iloc[0]
    risk_metrics = pd.DataFrame()
    daily_return = pv.pct_change().dropna()
    daily_return = daily_return.to_period('B')
    daily_return = daily_return.loc[~(daily_return == 0).all(axis=1)]

    for index in range(len(pv) - 504):
        daily_return_2Y = daily_return.iloc[index :504 + index]
        risk_metrics[index] = daily_return_2Y.std() * math.sqrt(252)
    return risk_metrics.T


if __name__ == '__main__':
	value = pd.read_csv('../../result/todatabase/backtest_overall.csv', index_col=0)
	benchmark = pd.read_csv('../../result/todatabase/risk_matching_benchmark.csv', index_col=0)
	value_risk_metrics = calculate_2Y_vol(pv=value, benchmark=benchmark)
	value_risk_metrics.to_csv('../../result/todatabase/risk_matching.csv')