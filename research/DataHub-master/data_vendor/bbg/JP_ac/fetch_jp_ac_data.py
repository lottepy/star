import pandas as pd
from lib.commonalgo.data.bbg_downloader import download_his,download_ref
import re

jp_df = pd.read_csv('JP_ac_mapping.csv')
start_d = '1990-01-01'
end_d = '2019-01-01'


for idx, row in jp_df.iterrows():
	ticker = row['BBG Ticker'] + ' INDEX'
	fname = 'data/' + row['Asset Type'] + '.csv'
	field = 'PX_LAST'
	try:
		result = download_his(ticker, field, start_date=start_d, end_date=end_d)
		result[(ticker, field)].rename('close').to_csv(fname, header=True)
	except:
		print (fname)