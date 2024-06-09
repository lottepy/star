import pandas as pd
from lib.commonalgo.data.bbg_downloader import download_his,download_ref
import re

# local_file = 'msci_single_country_gross_local.txt'
# USD_file = 'msci_single_country_gross_USD.txt'
#
# def file2list(filename):
#     with open(filename,'r') as f:
#         results = f.readlines()
#         return [x.replace('\n','') for x in results]
#
# local_list = file2list(local_file)
# USD_list = file2list(USD_file)
#
# fname = 'MSCI_EOD.csv'
# df = pd.read_csv(fname)
# filter_country = df[(df.Size == 'Standard') &
#                     (df.Style.isna()) &
#                     (df.Variant == 'Gross') &
#                     (df.Currency == 'Local')]
# local_rows = df[df['Bloomberg Index Name'].isin(local_list)]
# USD_rows = df[df['Bloomberg Index Name'].isin(USD_list)]
# local_rows.to_csv('local_mapping.csv')
# USD_rows.to_csv('USD_mapping.csv')

# local_df = pd.read_csv('local_mapping.csv')
# USD_df = pd.read_csv('USD_mapping.csv')
# start_d = '1990-01-01'
# end_d = '2018-10-31'
#
#
# for idx, row in USD_df.iterrows():
# 	ticker = row['BBG Ticker'] + ' INDEX'
# 	fname = 'msci_country/' + row['Bloomberg Index Name'] + '.csv'
# 	field = 'PX_LAST'
# 	try:
# 		result = download_his(ticker, field, start_date=start_d, end_date=end_d)
# 		result[(ticker, field)].rename('close').to_csv(fname, header=True)
# 	except:
# 		print (fname)


# ticker = 'MACN1A INDEX'
# fname = 'msci_country/' + 'MSCI China A onshore USD' + '.csv'
# field = 'PX_LAST'
# result = download_his(ticker, field, start_date=start_d, end_date=end_d)
# result[(ticker, field)].rename('close').to_csv(fname, header=True)


jp_df = pd.read_csv('JP_ac.csv')
start_d = '1990-01-01'
end_d = '2018-10-31'


for idx, row in jp_df.iterrows():
	ticker = row['BBG Ticker'] + ' INDEX'
	fname = 'JP_ac/' + row['Asset Type'] + '.csv'
	field = 'PX_LAST'
	try:
		result = download_his(ticker, field, start_date=start_d, end_date=end_d)
		result[(ticker, field)].rename('close').to_csv(fname, header=True)
	except:
		print (fname)