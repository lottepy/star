import pandas as pd
import numpy as np

from algorithm import addpath
from algorithm.utils.result_analysis import result_metrics_calculation

from os import listdir
from os.path import join

pv_file_list = listdir(join(addpath.result_path, 'backtest', '6M-12M'))
pv_file_list = [ele for ele in pv_file_list if 'PV' in ele and 'A1' in ele and 'B1' in ele]

pv_list = [pd.read_csv(join(addpath.result_path, 'backtest', '6M-12M', ele), index_col=[0], parse_dates=[0])['algorithm_period_return'] for ele in pv_file_list]
pv = pd.concat(pv_list, axis=1)
pv.index = pv.index.tz_localize(None).normalize() 
pv.columns = [ele[3:-4] for ele in pv_file_list]
pv = pv.loc[:'2020-12-31']

pv_full = pv.copy()
pv_2017 = pv.loc['2017-12-31':].copy()
pv_2018 = pv.loc['2018-12-31':].copy()
pv_2019 = pv.loc['2019-12-31':].copy()

pv_full.columns = 'full_' + pv_full.columns
pv_2017.columns = '3Y_' + pv_2017.columns
pv_2018.columns = '2Y_' + pv_2018.columns
pv_2019.columns = '1Y_' + pv_2019.columns

pv_list = [pv_full, pv_2017, pv_2018, pv_2019]
pv_list = [(ele.pct_change() + 1).cumprod().dropna() for ele in pv_list]
report_list = [result_metrics_calculation(ele) for ele in pv_list]

report = pd.concat(report_list)
report = report[report_list[0].columns]

writer = pd.ExcelWriter(join(addpath.result_path, 'backtest', 'backtest_summary.xlsx'))

pd.concat(pv_list, axis=1).to_excel(writer, sheet_name='performance')
report.to_excel(writer, sheet_name='metrics')

writer.save()



