import pandas as pd
import numpy as np
from os.path import join
from algorithm.utils.result_analysis import result_metrics_calculation

from datamaster import dm_client
dm_client.start()

from algorithm import addpath


if __name__ == "__main__":
    benchmark_list = ['HSI Index', 'SHSZ300 Index', 'MXWD Index', 'SPX Index', 'LBUSTRUU Index']

    benchmark_df = []
    for benchmark in benchmark_list:
        res = dm_client.historical(symbols=benchmark, start_date='2000-01-01', end_date='2020-12-31', fields='close')
        benchmark_df_tmp = pd.DataFrame.from_dict(res['values'][benchmark])
        benchmark_df_tmp = benchmark_df_tmp.set_index(0)
        benchmark_df_tmp.index = pd.to_datetime(benchmark_df_tmp.index)
        benchmark_df.append(benchmark_df_tmp)

    benchmark_df = pd.concat(benchmark_df, axis=1).ffill()
    benchmark_df.columns = benchmark_list

    sr = pd.read_csv(join(addpath.result_path, 'smart_rotation_21', 'portfolio value.csv'), index_col=[0], parse_dates=[0])
    sr.index = sr.index.tz_localize(None).normalize()

    sr = pd.concat([benchmark_df, sr], axis=1).ffill()
    sr.to_csv(join(addpath.result_path,'benchmark','sr_benchmark.csv'))
    
    sgu = pd.read_csv(join(addpath.result_path, 'simple_ver_sgu_2', 'portfolio value.csv'), index_col=[0], parse_dates=[0])

    sgu = pd.concat([benchmark_df, sgu], axis=1).ffill().dropna()
    sgu = (sgu.pct_change() + 1).cumprod()
    sgu['Benchmark'] = sgu['MXWD Index'] * 0.6 + sgu['LBUSTRUU Index'] * 0.4
    sgu = sgu.loc[:'2020-09-30']
    sgu.to_csv(join(addpath.result_path,'benchmark','sgu_benchmark.csv'))
    
    sgu_report = result_metrics_calculation(sgu)
    sgu_report.to_csv(join(addpath.result_path, 'benchmark', 'sgu_report.csv'))
    