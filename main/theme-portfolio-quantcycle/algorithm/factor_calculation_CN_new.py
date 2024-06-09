import numpy as np
import pandas as pd
import os
import math
from datetime import datetime, timedelta
from scipy.stats import skew
import calendar
import statsmodels.api as sm
from algorithm import addpath
import multiprocessing
# Factor construction

def ANNOUNCEMENT_DT_processing(array):
    array = str(int(array))
    return array[0:4] + '/' + array[4:6] + '/' + array[6:8]

def cal_factors(symbol, rebalance_freq):
	cn_path = os.path.join(addpath.data_path,"12_31_data", "cn_data")
	cn_fin_path = os.path.join(cn_path, "financial_monthly_data")
	cn_tra_path = os.path.join(cn_path, "trading")

	temp = pd.read_csv(os.path.join(cn_tra_path, symbol + ".csv"), parse_dates=[0], index_col=0)
	temp = temp[['PX_LAST', 'PX_LAST_RAW', 'PX_VOLUME_RAW', 'EQY_SH_OUT_RAW']]
	temp2 = pd.DataFrame(index=pd.date_range(temp.index[0], temp.index[-1], freq='D'))
	temp2['EQY_SH_OUT_RAW'] = temp['EQY_SH_OUT_RAW']
	temp2['PX_LAST_RAW'] = temp['PX_LAST_RAW']
	temp2['PX_LAST']=temp['PX_LAST']
	temp2 = temp2.ffill().bfill()
	result=pd.DataFrame()

	temp = pd.read_csv(os.path.join(cn_fin_path, symbol + ".csv"),parse_dates=[0],index_col=0)
	column_names = {
		'bps_adjust':'EQUITY_PERSHARE',
		'lt_borrow':'LONG_TERM_BORROW',
		'qfa_eps':'EPS',
		'qfa_net_cash_flows_oper_act':'CF_CASH_FROM_OPER',
		'qfa_net_profit_is':'NET_INCOME',
		'qfa_opprofit':'EBIT',
		'qfa_tot_oper_rev':'SALES_REV_TURN',
		'tot_assets':'BS_TOT_ASSET',
		'tot_equity':'TOT_EQUITY',
		'tot_liab':'BS_TOT_LIAB2'

	}

	if temp.shape[0] > 0:
		temp['symbol'] = symbol
		temp = temp.rename(columns=column_names)
		temp['ANNOUNCEMENT_DT'] = None
		# temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].map(lambda x: np.nan if pd.isna(x) else datetime.strptime(x, "%Y-%m-%d"))
		temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].dropna().apply(ANNOUNCEMENT_DT_processing)
		if temp[['ANNOUNCEMENT_DT']].dropna().shape[0] > 0:
			temp['effective_date'] = np.where(pd.isna(temp['ANNOUNCEMENT_DT']), temp.index + timedelta(days=90),
											  temp['ANNOUNCEMENT_DT'])
		else:
			temp['effective_date'] = temp.index + timedelta(days=90)
		temp.index.name = 'date'
		temp['effective_date']=pd.to_datetime(temp['effective_date'], format='%Y/%m/%d', errors='ignore')
		temp = temp.reset_index()
		temp = temp.set_index('effective_date')

		temp['EQY_SH_OUT_RAW'] = temp2['EQY_SH_OUT_RAW']
		temp['PX_LAST_RAW'] = temp2['PX_LAST_RAW']
		temp['PX_LAST']=temp2['PX_LAST']

		temp = temp.reset_index()
		temp = temp.set_index('date')
		temp['datadate'] = temp.index
		temp = temp.resample('1M').last().ffill()

		temp['SALES_REV_TURN_SEMI'] = temp['SALES_REV_TURN'] + temp['SALES_REV_TURN'].shift(3)
		temp['SALES_REV_TURN_TTM'] = temp['SALES_REV_TURN'] + temp['SALES_REV_TURN'].shift(3)+temp['SALES_REV_TURN'].shift(6)+temp['SALES_REV_TURN'].shift(9)
		temp['EBIT_SEMI'] = temp['EBIT'] + temp['EBIT'].shift(3)
		temp['EBIT_TTM'] = temp['EBIT'] + temp['EBIT'].shift(3)+ temp['EBIT'].shift(6)+ temp['EBIT'].shift(9)
		temp['NET_INCOME_SEMI'] = temp['NET_INCOME'] + temp['NET_INCOME'].shift(3)
		temp['NET_INCOME_TTM'] = temp['NET_INCOME'] + temp['NET_INCOME'].shift(3)+ temp['NET_INCOME'].shift(6)+temp['NET_INCOME'].shift(9)
		temp['CF_CASH_FROM_OPER_SEMI'] = temp['CF_CASH_FROM_OPER'] + temp['CF_CASH_FROM_OPER'].shift(3)
		temp['CF_CASH_FROM_OPER_TTM'] = temp['CF_CASH_FROM_OPER'] + temp['CF_CASH_FROM_OPER'].shift(3)+ temp['CF_CASH_FROM_OPER'].shift(6)+temp['CF_CASH_FROM_OPER'].shift(9)

		# Investment Rate
		temp['ASSETGROWTH'] = (temp['BS_TOT_ASSET'] - temp['BS_TOT_ASSET'].shift(12)) / temp['BS_TOT_ASSET'].shift(12)
		temp['BVGROWTH'] = temp['TOT_EQUITY'] / temp['TOT_EQUITY'].shift(12) - 1

		# Profitability
		temp['REVOA'] = temp['SALES_REV_TURN'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['REVOE'] = temp['SALES_REV_TURN'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
		temp['EBITOA'] = temp['EBIT'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['EBITOE'] = temp['EBIT'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
		temp['ROE'] = temp['NET_INCOME'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))
		temp['ROA'] = temp['NET_INCOME'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['OPCFOA']=temp['CF_CASH_FROM_OPER']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(3))
		temp['OPCFOE']=temp['CF_CASH_FROM_OPER']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(3))

		temp['REVOA_SEMI'] = temp['SALES_REV_TURN_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['REVOE_SEMI'] = temp['SALES_REV_TURN_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['EBITOA_SEMI'] = temp['EBIT_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['EBITOE_SEMI'] = temp['EBIT_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['ROE_SEMI'] = temp['NET_INCOME_SEMI'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['ROA_SEMI'] = temp['NET_INCOME_SEMI'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['OPCFOA_SEMI']=temp['CF_CASH_FROM_OPER_SEMI']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['OPCFOE_SEMI']=temp['CF_CASH_FROM_OPER_SEMI']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))


		temp['REVOA_TTM'] = temp['SALES_REV_TURN_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['REVOE_TTM'] = temp['SALES_REV_TURN_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['EBITOA_TTM'] = temp['EBIT_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBITOE_TTM'] = temp['EBIT_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['ROE_TTM'] = temp['NET_INCOME_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['ROA_TTM'] = temp['NET_INCOME_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['OPCFOA_TTM']=temp['CF_CASH_FROM_OPER_TTM']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['OPCFOE_TTM']=temp['CF_CASH_FROM_OPER_TTM']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))

		# Margin
		temp['EBIT_MARGIN'] = temp['EBIT'] / temp['SALES_REV_TURN']
		temp['OPCF_MARGIN'] = temp['CF_CASH_FROM_OPER'] / temp['SALES_REV_TURN']
		temp['NET_MARGIN'] = temp['NET_INCOME'] / temp['SALES_REV_TURN']

		temp['EBIT_MARGIN_SEMI'] = temp['EBIT_SEMI'] / temp['SALES_REV_TURN_SEMI']
		temp['OPCF_MARGIN_SEMI'] = temp['CF_CASH_FROM_OPER_SEMI'] / temp['SALES_REV_TURN_SEMI']
		temp['NET_MARGIN_SEMI'] = temp['NET_INCOME_SEMI'] / temp['SALES_REV_TURN_SEMI']

		temp['EBIT_MARGIN_TTM'] = temp['EBIT_TTM'] / temp['SALES_REV_TURN_TTM']
		temp['OPCF_MARGIN_TTM'] = temp['CF_CASH_FROM_OPER_TTM'] / temp['SALES_REV_TURN_TTM']
		temp['NET_MARGIN_TTM'] = temp['NET_INCOME_TTM'] / temp['SALES_REV_TURN_TTM']

		# Growth
		temp['ROAGROWTH'] = (temp['NET_INCOME']-temp['NET_INCOME'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['REVOAGROWTH'] = (temp['SALES_REV_TURN']-temp['SALES_REV_TURN'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['OPCFOAGROWTH'] = (temp['CF_CASH_FROM_OPER']-temp['CF_CASH_FROM_OPER'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITOAGROWTH'] = (temp['EBIT']-temp['EBIT'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['ROEGROWTH'] = (temp['NET_INCOME']-temp['NET_INCOME'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['REVOEGROWTH'] = (temp['SALES_REV_TURN']-temp['SALES_REV_TURN'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['OPCFOEGROWTH'] = (temp['CF_CASH_FROM_OPER']-temp['CF_CASH_FROM_OPER'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITOEGROWTH'] = (temp['EBIT']-temp['EBIT'].shift(12))/temp['TOT_EQUITY'].shift(12)

		temp['ROAGROWTH_SEMI'] = (temp['NET_INCOME_SEMI']-temp['NET_INCOME_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['REVOAGROWTH_SEMI'] = (temp['SALES_REV_TURN_SEMI']-temp['SALES_REV_TURN_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['OPCFOAGROWTH_SEMI'] = (temp['CF_CASH_FROM_OPER_SEMI']-temp['CF_CASH_FROM_OPER_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITOAGROWTH_SEMI'] = (temp['EBIT_SEMI']-temp['EBIT_SEMI'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['ROEGROWTH_SEMI'] = (temp['NET_INCOME_SEMI']-temp['NET_INCOME_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['REVOEGROWTH_SEMI'] = (temp['SALES_REV_TURN_SEMI']-temp['SALES_REV_TURN_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['OPCFOEGROWTH_SEMI'] = (temp['CF_CASH_FROM_OPER_SEMI']-temp['CF_CASH_FROM_OPER_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITOEGROWTH_SEMI'] = (temp['EBIT_SEMI']-temp['EBIT_SEMI'].shift(12))/temp['TOT_EQUITY'].shift(12)


		temp['ROAGROWTH_TTM'] = (temp['NET_INCOME_TTM']-temp['NET_INCOME_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['REVOAGROWTH_TTM'] = (temp['SALES_REV_TURN_TTM']-temp['SALES_REV_TURN_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['OPCFOAGROWTH_TTM'] = (temp['CF_CASH_FROM_OPER_TTM']-temp['CF_CASH_FROM_OPER_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITOAGROWTH_TTM'] = (temp['EBIT_TTM']-temp['EBIT_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['ROEGROWTH_TTM'] = (temp['NET_INCOME_TTM']-temp['NET_INCOME_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['REVOEGROWTH_TTM'] = (temp['SALES_REV_TURN_TTM']-temp['SALES_REV_TURN_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['OPCFOEGROWTH_TTM'] = (temp['CF_CASH_FROM_OPER_TTM']-temp['CF_CASH_FROM_OPER_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITOEGROWTH_TTM'] = (temp['EBIT_TTM']-temp['EBIT_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)

		# Earnings Variability
		def earnval(array):
			l = int(len(array) / 3)
			x = np.array([array[a * 3] for a in range(l)])
			y = np.nanstd(x)
			return y

		# temp['EARNVAR'] = temp['EARNGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROAVAR'] = temp['ROAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROEVAR'] = temp['ROEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['REVVAR'] = temp['REVOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['REVVAR'] = temp['REVOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOAVAR'] = temp['OPCFOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOEVAR'] = temp['OPCFOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITOAVAR'] = temp['EBITOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITOEVAR'] = temp['EBITOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)

		temp['ROAVAR_SEMI'] = temp['ROAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROEVAR_SEMI'] = temp['ROEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['REVVAR_SEMI'] = temp['REVOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['REVVAR_SEMI'] = temp['REVOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOAVAR_SEMI'] = temp['OPCFOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOEVAR_SEMI'] = temp['OPCFOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITOAVAR_SEMI'] = temp['EBITOAGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITOEVAR_SEMI'] = temp['EBITOEGROWTH_SEMI'].rolling(window=36, min_periods=30).apply(earnval)

		# temp['EARNVAR_TTM'] = temp['EARNGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROAVAR_TTM'] = temp['ROAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROEVAR_TTM'] = temp['ROEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['REVVAR_TTM'] = temp['REVOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['REVVAR_TTM'] = temp['REVOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOAVAR_TTM'] = temp['OPCFOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOEVAR_TTM'] = temp['OPCFOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITOAVAR_TTM'] = temp['EBITOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITOEVAR_TTM'] = temp['EBITOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)

		# Valuation
		temp['BTM'] = temp['TOT_EQUITY'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
		temp['ETP'] = temp['NET_INCOME'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
		temp['ETP_SEMI'] = temp['NET_INCOME_SEMI'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
		temp['STP'] = temp['SALES_REV_TURN'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
		temp['STP_SEMI'] = temp['SALES_REV_TURN_SEMI'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
		temp['ETP_TTM'] = temp['NET_INCOME_TTM'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
		temp['STP_TTM'] = temp['SALES_REV_TURN_TTM'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])
		# temp['CFTP'] = temp['CF_CASH_FROM_OPER'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW'])

		# Leverage
		temp['LEV'] = temp['BS_TOT_LIAB2'] / temp['BS_TOT_ASSET']

		temp = temp.set_index('effective_date')
		temp = temp.resample('1M').last().ffill()
		if result.shape[0] > 0:
			result = pd.merge(left=result, right=temp, left_index=True, right_index=True, how='outer')
		else:
			result = temp

	if result.shape[0] > 0:
		result = result.ffill()

	result.to_csv(os.path.join(us_path, "Factors", symbol + ".csv"))
	print(os.path.join(us_path, "Factors", symbol + ".csv")+'written')
	# return result


if __name__ == "__main__":
	us_path = os.path.join(addpath.data_path, "12_31_data","cn_data")
	iu_path = os.path.join(us_path, "investment_univ")
	iu_files = os.listdir(iu_path)
	iu_files.remove('.DS_Store')
	symbol_list = []
	for iu_file in iu_files:
		print(iu_file)
		tmp = pd.read_csv(os.path.join(iu_path, iu_file))
		tmp_list = tmp['symbol'].tolist()
		symbol_list = symbol_list + tmp_list
	symbol_list = list(set(symbol_list))
	print(len(symbol_list))

	rebalance_freq = 'MONTHLY'

	pool = multiprocessing.Pool()
	for symbol in symbol_list:
		# pool.apply_async(cal_factors, args=(symbol, rebalance_freq,))
		# print('Sub-processes begins!')
		cal_factors(symbol, rebalance_freq)
	pool.close()
	pool.join()
	print('Sub-processes done!')





	factors_path = os.path.join(us_path, "Factors")
	factors_files = os.listdir(factors_path)
	factors = {}
	# factors_files.remove('.DS_Store')

	for factors_file in factors_files:
		print(factors_file)
		symbol=factors_file[:-4]
		temp=pd.read_csv(os.path.join(factors_path, factors_file), parse_dates=[0],index_col=0)
		if len(temp) > 0:
			factors[symbol]=temp

	for iu_file in iu_files:
		date=datetime(int(iu_file[0:4]),int(iu_file[5:7]),int(iu_file[8:10]))
		print(date)
		tmp = pd.read_csv(os.path.join(iu_path, iu_file))
		tmp_list = tmp['symbol'].tolist()
		dataout=[]

		inve_list = []
		for symbol in tmp_list:
			# print(symbol)
			if symbol in list(factors.keys()):
				if date in factors[symbol].index.tolist():
					temp_symbol=factors[symbol].loc[[date],:]
					dataout.append(temp_symbol)
					inve_list.append(symbol)
		inve_list=pd.DataFrame(inve_list)
		# inve_list.to_csv(os.path.join(iu_path, iu_file),index=False)
		dataout=pd.concat(dataout)
		# dataout=dataout.drop(columns=['YYYY','MM'])
		data=dataout
		# data['TL>TA'] = (data['BS_TOT_LIAB2'] > data['BS_TOT_ASSET']) * 1
		# data['PRE_NET_INCOME_TTM']=data['NET_INCOME_TTM']-data['ROAGROWTH_TTM']*data['PRE_BS_TOT_ASSET']
		# data['loss_for_2y'] = ((data['NET_INCOME_TTM'] < 0) & (data['PRE_NET_INCOME_TTM'] < 0)) * 1
		# temp = data['BS_TOT_ASSET'] / cpi
		# temp = temp.drop(index=temp.index[temp <= 0])
		# temp = temp.apply(math.log)
		# data['O_SCORE'] = -1.32 - 0.407 * temp + 6.03 * data['BS_TOT_LIAB2'] / data['BS_TOT_ASSET'] - 1.43 * data[
		# 	'WORKING_CAPITAL'] / data['BS_TOT_ASSET'] + 0.0757 * data['CURRLIAB'] / data['CURRASSET'] - 1.72 * data[
		# 					  'TL>TA'] - 2.37 * data['ROA_TTM'] - 1.83 * data['OPCF'] / data['BS_TOT_LIAB2'] + 0.285 * \
		# 				  data['loss_for_2y'] - 0.521 * (data['NET_INCOME_TTM'] - data['PRE_NET_INCOME_TTM']) / (
		# 							  abs(data['NET_INCOME_TTM']) + abs(data['PRE_NET_INCOME_TTM']))

		# dataout.index=tmp_list
		CSfactors_path = os.path.join(us_path, "CS_Factors")
		data['MARKET_CAP']=data['EQY_SH_OUT_RAW']*data['PX_LAST_RAW']
		data['CLOSE']=data['PX_LAST_RAW']
		data.set_index(['symbol'],inplace=True)
		data.to_csv(os.path.join(CSfactors_path,iu_file))


