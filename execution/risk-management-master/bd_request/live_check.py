from utils.bbg_downloader import download_his
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style('whitegrid')

start = '2016-08-03'
end = '2019-07-05'
sgm_4 = ['HYG US Equity', 'IWS US Equity', 'LQD US Equity', 'MUB US Equity',
		 'SCHF US Equity', 'VNQ US Equity', 'VTI US Equity', 'VWO US Equity',
		 'XLE US Equity']
sgm_522 = ['AGG US Equity', 'ASHR US Equity', 'EMB US Equity', 'FLOT US Equity',
		   'GLD US Equity', 'HYG US Equity', 'INDA US Equity', 'SCHF US Equity',
		   'VBR US Equity', 'VNQ US Equity', 'VTI US Equity', 'VWO US Equity',
		   'XLE US Equity']

current_list = sgm_4
data = download_his(
	current_list,
	['PX_LAST'],
	start,
	end,
	period='DAILY',
	currency='USD'
).ffill()
data.columns = data.columns.get_level_values(0)
data = data[current_list]
