# # 将日级货币对汇率转换成对美元汇率
# 用时1s
import pandas as pd
from copy import copy
from datetime import datetime
import os

ENV = os.getenv('DYMON_FX_ENV', 'dev_algo')

def process_quote_in_usd():
	start = datetime.now()

	for tz in ['T150', 'BGNL', 'CMPL']:
		# data_path = f"//192.168.9.170/share/alioss/0_DymonFx/parse_data/{tz}_daily_data"
		# target_path = "//192.168.9.170/share/alioss/0_DymonFx/parse_data"
		data_path = {
			'dev_algo': f"//192.168.9.170/share/alioss/0_DymonFx/parse_data/{tz}_daily_data",
			'dev_jeff': f'../0_DymonFx/parse_data/{tz}_daily_data',
			'live': f'../0_DymonFx/parse_data/{tz}_daily_data',
		}[ENV]
		target_path = {
			'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/parse_data/",
			'dev_jeff': '../0_DymonFx/parse_data/',
			'live': '../0_DymonFx/parse_data/'
		}[ENV]

		all_files = os.listdir(data_path)
		all_ccp = [file.split('.')[0] for file in all_files]

		# In[6]:


		# 1.先把所有货币对merge在一起
		if 'res' in locals():
			del res

		for currency_pair_file in all_files:
			currency_pair_path = os.path.join(data_path, currency_pair_file)
			currency_pair_name = currency_pair_file.split('.')[0]
			currency_pair = pd.read_csv(currency_pair_path, index_col=0, parse_dates=True)
			currency_pair = currency_pair[['PRICE_LAST']]
			currency_pair.rename(columns={'PRICE_LAST': f'last_{currency_pair_name}'}, inplace=True)
			if 'res' not in locals():
				res = currency_pair
			else:
				res = res.join(currency_pair, how='outer')
		res.ffill(inplace=True)
		all_ccp_close = res

		# 2.算出货币对美元的汇率
		all_currency_pair = [file.split('.')[0] for file in all_files]
		left_all_currency = [ccp[:3] for ccp in all_currency_pair]
		right_all_currency = [ccp[3:] for ccp in all_currency_pair]
		all_currency = list(set(left_all_currency + right_all_currency))
		all_currency.remove('USD')
		# 用时6s左右
		ccy_usd = pd.DataFrame()

		for ccp in all_currency_pair:
			statu = 0
			if ccp[:3] == 'USD':
				ccy = ccp[3:]
				ccy_usd[ccy] = all_ccp_close[f'last_{ccp}'].apply(lambda x: 1 / x)
				statu = 1
			elif ccp[3:] == 'USD':
				ccy = ccp[:3]
				ccy_usd[ccy] = all_ccp_close[f'last_{ccp}']
				statu = 1
			if statu == 1:
				all_currency.remove(ccy)

		while len(all_currency) > 0:
			for ccp in all_ccp:
				statu = 0
				if ccp[:3] in ccy_usd.columns:
					ccy = ccp[3:]
					if ccy in all_currency:
						A_B = all_ccp_close[f'last_{ccp}']
						A_USD = ccy_usd[ccp[:3]]
						B_USD = A_USD / A_B
						ccy_usd[ccy] = B_USD
						statu = 1
				elif ccp[3:] in ccy_usd.columns:
					ccy = ccp[:3]
					if ccy in all_currency:
						A_B = all_ccp_close[f'last_{ccp}']
						B_USD = ccy_usd[ccp[3:]]
						A_USD = A_B * A_USD
						ccy_usd[ccy] = A_USD
						statu = 1
				if statu == 1:
					# print(ccp)
					# print(ccy)
					all_currency.remove(ccy)
		ccy_usd['USD'] = 1
		ccy_usd.to_csv(os.path.join(target_path, f'FX_AGAINST_USD_PRICE_DAILY_{tz}.csv'))

	end = datetime.now()
	print(end - start)


if __name__ == '__main__':
	process_quote_in_usd()