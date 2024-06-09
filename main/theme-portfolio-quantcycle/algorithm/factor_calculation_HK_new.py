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
	us_path = os.path.join(addpath.data_path,"12_31_data","hk_data")
	us_fin_path = os.path.join(us_path, "financial")
	us_tra_path = os.path.join(us_path, "trading")

	temp = pd.read_csv(os.path.join(us_tra_path, symbol + ".csv"), parse_dates=[0], index_col=0)
	temp=temp.dropna(subset=['PX_VOLUME_RAW'])
	result=temp.resample('1M').last().ffill()
	temp2 = pd.DataFrame(index=pd.date_range(temp.index[0], temp.index[-1], freq='D'))
	temp2['EQY_SH_OUT_RAW'] = temp['EQY_SH_OUT_RAW']
	temp2['PX_LAST_RAW'] = temp['PX_LAST_RAW']
	temp2['PX_LAST']=temp['PX_LAST']
	temp2 = temp2.ffill().bfill()


	temp = pd.read_csv(os.path.join(us_fin_path, symbol + ".csv"),parse_dates=[0],index_col=0)

	if temp.shape[0] > 0:
		temp['symbol'] = symbol
		# temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].map(lambda x: np.nan if pd.isna(x) else datetime.strptime(x, "%Y-%m-%d"))
		# temp['ANNOUNCEMENT_DT'] = temp['ANNOUNCEMENT_DT'].dropna().apply(ANNOUNCEMENT_DT_processing)
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

		temp['TOT_EQUITY'] = temp['BS_TOT_ASSET']- temp['BS_TOT_LIAB2']

		temp['GROSS_PROFIT'] = temp['SALES_REV_TURN'] - temp['IS_COGS_TO_FE_AND_PP_AND_G']
		temp['EBIT'] = temp['IS_OPER_INC']
		temp['EBITDA'] = temp['EBIT']+temp['CF_DEPR_AMORT']
		temp['EBT'] = temp['EBIT'] - temp['IS_INT_EXPENSE']

		temp['SALES_REV_TURN_TTM'] = temp['SALES_REV_TURN'] + temp['SALES_REV_TURN'].shift(6)
		temp['IS_COGS_TO_FE_AND_PP_AND_G_TTM'] = temp['IS_COGS_TO_FE_AND_PP_AND_G'] + temp['IS_COGS_TO_FE_AND_PP_AND_G'].shift(6)
		temp['GROSS_PROFIT_TTM'] = temp['GROSS_PROFIT'] + temp['GROSS_PROFIT'].shift(6)
		temp['EBITDA_TTM'] = temp['EBITDA'] + temp['EBITDA'].shift(6)
		temp['EBIT_TTM'] = temp['EBIT'] + temp['EBIT'].shift(6)
		temp['EBT_TTM'] = temp['EBT'] + temp['EBT'].shift(6)
		temp['NET_INCOME_TTM'] = temp['NET_INCOME'] + temp['NET_INCOME'].shift(6)

		# Negative Indicating Factors
		# # Total Accruals
		temp['acc_cul_BS'] = temp['BS_CUR_ASSET_REPORT'].fillna(0) - temp['BS_CASH_CASH_EQUIVALENTS_AND_STI'].fillna(0) - \
					  temp['BS_CUR_LIAB'].fillna(0)+temp['BS_ST_BORROW'].fillna(0)
		tot_assets = temp['BS_TOT_ASSET']
		temp['NIexACC'] = temp['NET_INCOME']-(temp['acc_cul_BS'] - temp['acc_cul_BS'].shift(6))-temp['CF_DEPR_AMORT'].fillna(0)
		temp['NIexACConNI']=temp['NIexACC']/temp['NET_INCOME']
		temp['NIexACConNI_TTM']=(temp['NIexACC']+temp['NIexACC'].shift(6))/temp['NET_INCOME_TTM']

		temp['NIexACConASSET']=temp['NIexACC']*2/(temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(6))
		temp['NIexACConASSET_TTM']=(temp['NIexACC']+temp['NIexACC'].shift(6))*2/(temp['BS_TOT_ASSET']+temp['BS_TOT_ASSET'].shift(12))

		# Investment Rate
		# temp['INVESTRATE'] = temp['CF_CAP_EXPEND_PRPTY_ADD'].fillna(0) / tot_assets.shift(12)
		temp['ASSETGROWTH'] = (tot_assets - tot_assets.shift(12)) / tot_assets.shift(12)
		temp['BVGROWTH'] = temp['TOT_EQUITY'] / temp['TOT_EQUITY'].shift(12) - 1

		# solvency ratios
		temp['WORKING_CAPITAL'] = temp['BS_TOT_ASSET'] - temp['BS_CUR_LIAB']
		temp['CURRENT_RATIO'] = temp['BS_TOT_ASSET'] / temp['BS_CUR_LIAB']
		temp['QUICK_RATIO'] = temp['BS_CASH_CASH_EQUIVALENTS_AND_STI'] / temp['BS_CUR_LIAB']
		# temp['CASH_RATIO'] = temp['BS_CASH_NEAR_CASH_ITEM'] / temp['BS_CUR_LIAB']
		temp['ACCT_RCV_TO']=temp['SALES_REV_TURN']*2/(temp['BS_ACCT_NOTE_RCV']+temp['BS_ACCT_NOTE_RCV'].shift(6))
		temp['ACCT_RCV_TO_TTM']=temp['SALES_REV_TURN_TTM']*2/(temp['BS_ACCT_NOTE_RCV']+temp['BS_ACCT_NOTE_RCV'].shift(12))

		temp['ACCT_PAY_TO']=temp['IS_COGS_TO_FE_AND_PP_AND_G']*2/(temp['ARDR_ACCOUNTS_PAYABLE_TRADE']+temp['ARDR_ACCOUNTS_PAYABLE_TRADE'].shift(6))
		temp['ACCT_PAY_TO_TTM']=temp['IS_COGS_TO_FE_AND_PP_AND_G_TTM']*2/(temp['ARDR_ACCOUNTS_PAYABLE_TRADE']+temp['ARDR_ACCOUNTS_PAYABLE_TRADE'].shift(12))

		#OPERATING CASHFLOW
		temp['OPCF']=temp['NET_INCOME']+temp['CF_DEPR_AMORT']-(temp['WORKING_CAPITAL']-temp['WORKING_CAPITAL'].shift(6))
		temp['OPCF_TTM']=temp['NET_INCOME_TTM']+temp['CF_DEPR_AMORT']+temp['CF_DEPR_AMORT'].shift(6)-\
						 (temp['WORKING_CAPITAL']-temp['WORKING_CAPITAL'].shift(12))

		# Profitability
		temp['EBITDAOA'] = temp['EBITDA'] * 2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['EBITDAOE'] = temp['EBITDA'] * 2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['EBITOA'] = temp['EBIT'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['EBITOE'] = temp['EBIT'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['EBTDAOA'] = temp['EBT'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['EBTDAOE'] = temp['EBT'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['ROE'] = temp['NET_INCOME'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['ROA'] = temp['NET_INCOME'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['GPOA'] = temp['GROSS_PROFIT']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['GPOE'] = temp['GROSS_PROFIT']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))



		# temp['CFOA'] = temp['CF_CASH_FROM_OPER'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBITDAOA_TTM'] = temp['EBITDA_TTM'] * 2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBITDAOE_TTM'] = temp['EBITDA_TTM'] * 2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['EBITOA_TTM'] = temp['EBIT_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBITOE_TTM'] = temp['EBIT_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['EBTOA_TTM'] = temp['EBT_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['EBTOE_TTM'] = temp['EBT_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['ROE_TTM'] = temp['NET_INCOME_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))
		temp['ROA_TTM'] = temp['NET_INCOME_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['GPOA_TTM'] = temp['GROSS_PROFIT_TTM'] * 2 / (temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['GPOE_TTM'] = temp['GROSS_PROFIT_TTM'] * 2 / (temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))

		temp['OPCFOA']=temp['OPCF']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(6))
		temp['OPCFOA_TTM']=temp['OPCF_TTM']*2/(temp['BS_TOT_ASSET'] + temp['BS_TOT_ASSET'].shift(12))
		temp['OPCFOE']=temp['OPCF']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(6))
		temp['OPCFOE_TTM']=temp['OPCF_TTM']*2/(temp['TOT_EQUITY'] + temp['TOT_EQUITY'].shift(12))

		# Margin
		temp['GROSS_PROFIT_MARGIN'] = temp['GROSS_PROFIT'] / temp['SALES_REV_TURN']
		temp['OPCF_MARGIN'] = temp['OPCF'] / temp['SALES_REV_TURN']
		temp['NET_MARGIN'] = temp['NET_INCOME'] / temp['SALES_REV_TURN']

		temp['GROSS_PROFIT_MARGIN_TTM'] = temp['GROSS_PROFIT_TTM'] / temp['SALES_REV_TURN_TTM']
		temp['OPCF_MARGIN_TTM'] = temp['OPCF_TTM'] / temp['SALES_REV_TURN_TTM']
		temp['NET_MARGIN_TTM'] = temp['NET_INCOME_TTM'] / temp['SALES_REV_TURN_TTM']

		# Growth
		temp['ROAGROWTH'] = (temp['NET_INCOME']-temp['NET_INCOME'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['REVOAGROWTH'] = (temp['SALES_REV_TURN']-temp['SALES_REV_TURN'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['OPCFOAGROWTH'] = (temp['OPCF']-temp['OPCF'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['GPOAGROWTH'] = (temp['GROSS_PROFIT']-temp['GROSS_PROFIT'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITDAOAGROWTH'] = (temp['EBITDA']-temp['EBITDA'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITOAGROWTH'] = (temp['EBIT']-temp['EBIT'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBTOAGROWTH'] = (temp['EBT']-temp['EBT'].shift(12))/temp['BS_TOT_ASSET'].shift(12)

		temp['ROEGROWTH'] = (temp['NET_INCOME']-temp['NET_INCOME'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['REVOEGROWTH'] = (temp['SALES_REV_TURN']-temp['SALES_REV_TURN'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['OPCFOEGROWTH'] = (temp['OPCF']-temp['OPCF'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['GPOEGROWTH'] = (temp['GROSS_PROFIT']-temp['GROSS_PROFIT'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITDAOEGROWTH'] = (temp['EBITDA']-temp['EBITDA'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITOEGROWTH'] = (temp['EBIT']-temp['EBIT'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBTOEGROWTH'] = (temp['EBT']-temp['EBT'].shift(12))/temp['TOT_EQUITY'].shift(12)

		temp['ROAGROWTH_TTM'] = (temp['NET_INCOME_TTM']-temp['NET_INCOME_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['REVOAGROWTH_TTM'] = (temp['SALES_REV_TURN_TTM']-temp['SALES_REV_TURN_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['OPCFOAGROWTH_TTM'] = (temp['OPCF_TTM']-temp['OPCF_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['GPOAGROWTH_TTM'] = (temp['GROSS_PROFIT_TTM']-temp['GROSS_PROFIT_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITDAOAGROWTH_TTM'] = (temp['EBITDA_TTM']-temp['EBITDA_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBITOAGROWTH_TTM'] = (temp['EBIT_TTM']-temp['EBIT_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)
		temp['EBTOAGROWTH_TTM'] = (temp['EBT_TTM']-temp['EBT_TTM'].shift(12))/temp['BS_TOT_ASSET'].shift(12)

		temp['ROEGROWTH_TTM'] = (temp['NET_INCOME_TTM']-temp['NET_INCOME_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['REVOEGROWTH_TTM'] = (temp['SALES_REV_TURN_TTM']-temp['SALES_REV_TURN_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['OPCFOEGROWTH_TTM'] = (temp['OPCF_TTM']-temp['OPCF_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['GPOEGROWTH_TTM'] = (temp['GROSS_PROFIT_TTM']-temp['GROSS_PROFIT_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITDAOEGROWTH_TTM'] = (temp['EBITDA_TTM']-temp['EBITDA_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBITOEGROWTH_TTM'] = (temp['EBIT_TTM']-temp['EBIT_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)
		temp['EBTOEGROWTH_TTM'] = (temp['EBT_TTM']-temp['EBT_TTM'].shift(12))/temp['TOT_EQUITY'].shift(12)

		# Earnings Variability
		def earnval(array):
			l = int(len(array) / 3)
			x = np.array([array[a * 3] for a in range(l)])
			y = np.nanstd(x)
			return y

		# temp['EARNVAR'] = temp['EARNGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROAVAR'] = temp['ROAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROEVAR'] = temp['ROEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOAVAR'] = temp['OPCFOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOEVAR'] = temp['OPCFOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOAVAR'] = temp['GPOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOEVAR'] = temp['GPOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDAVAR'] = temp['EBITDAOAGROWTH'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDEVAR'] = temp['EBITDAOEGROWTH'].rolling(window=36, min_periods=30).apply(earnval)

		# temp['EARNVAR_TTM'] = temp['EARNGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROAVAR_TTM'] = temp['ROAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['ROEVAR_TTM'] = temp['ROEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOAVAR_TTM'] = temp['OPCFOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['OPCFOEVAR_TTM'] = temp['OPCFOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOAVAR_TTM'] = temp['GPOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['GPOEVAR_TTM'] = temp['GPOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDAVAR_TTM'] = temp['EBITDAOAGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)
		temp['EBITDEVAR_TTM'] = temp['EBITDAOEGROWTH_TTM'].rolling(window=36, min_periods=30).apply(earnval)

		#inventory turnover
		temp['INVENTORY_TO'] = temp['IS_COGS_TO_FE_AND_PP_AND_G'] * 2 / (temp['BS_INVENTORIES'] + temp['BS_INVENTORIES'].shift(6))
		temp['INVENTORY_TO_TTM'] = temp['IS_COGS_TO_FE_AND_PP_AND_G_TTM'] * 2 / (
					temp['BS_INVENTORIES'] + temp['BS_INVENTORIES'].shift(12))

		# Valuation
		temp['BTM'] = temp['TOT_EQUITY'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
		temp['ETP'] = temp['NET_INCOME'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
		temp['STP'] = temp['SALES_REV_TURN'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
		temp['ETP_TTM'] = temp['NET_INCOME_TTM'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
		temp['STP_TTM'] = temp['SALES_REV_TURN_TTM'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000
		# temp['CFTP'] = temp['CF_CASH_FROM_OPER'] / (temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * 1000000

		# Leverage
		temp['LEV'] = temp['BS_TOT_LIAB2'] / temp['BS_TOT_ASSET']
		temp['LONGTERM_DEBTRATIO'] = temp['BS_LONG_TERM_BORROWINGS'] / temp['BS_TOT_ASSET']
		temp['SHORTTERM_DEBTRATIO'] = temp['BS_ST_BORROW'] / temp['BS_TOT_ASSET']
		temp['ST_PORTION_OF_LT']=temp['BS_ST_PORTION_OF_LT_DEBT']/temp['BS_LONG_TERM_BORROWINGS']

		# Interest Coverage
		# temp['IE_Prop'] = np.where(temp['EBIT'] > 0, temp['IS_INT_EXPENSE'] / temp['EBIT'], np.where(pd.isna(temp['IS_INT_EXPENSE']), 0, 1))
		# temp['IE_Prop_SEMI'] = np.where(temp['EBIT_SEMI'] > 0, temp['IS_INT_EXPENSE_SEMI'] / temp['EBIT_SEMI'],
		# 						   np.where(pd.isna(temp['IS_INT_EXPENSE_SEMI']), 0, 1))
		# temp['IE_Prop_TTM'] = np.where(temp['EBIT_TTM'] > 0, temp['IS_INT_EXPENSE_TTM'] / temp['EBIT_TTM'],
		# 						   np.where(pd.isna(temp['IS_INT_EXPENSE_TTM']), 0, 1))

		#bankruptcy risk
		temp['Z_SCORE']=1.2*temp['WORKING_CAPITAL']/temp['BS_TOT_ASSET']+1.4*temp['BS_RETAIN_EARN']/temp['BS_TOT_ASSET']\
						+3.3*temp['EBIT']/temp['BS_TOT_ASSET']+0.6*(temp['EQY_SH_OUT_RAW'] * temp['PX_LAST_RAW']) * \
						1000000/temp['BS_TOT_LIAB2']+1*temp['SALES_REV_TURN']/temp['BS_TOT_ASSET']

		# REITs Factors
		# temp['NET_OPER_INCOME_to_Assets'] = temp['NET_OPER_INCOME'] / temp['BS_TOT_ASSET']
		# temp['AFFO_to_Assets'] = (temp['CF_CASH_FROM_OPER'] - temp['CF_CAP_EXPEND_PRPTY_ADD']) / temp['BS_TOT_ASSET']

		temp = temp.set_index('effective_date')
		temp = temp.resample('1M').last().ffill()
		if result.shape[0] > 0:
			for ele in temp.columns:
				if ele in result.columns:
					temp=temp.drop(columns=[ele])
			result = pd.merge(left=result, right=temp, left_index=True, right_index=True, how='outer')
		else:
			result = temp

	if result.shape[0] > 0:
		result = result.ffill()

	result.to_csv(os.path.join(us_path, "Factors", symbol + ".csv"))
	print(os.path.join(us_path, "Factors", symbol + ".csv")+'written')
	# return result


if __name__ == "__main__":
	us_path = os.path.join(addpath.data_path,"12_31_data", "hk_data")
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
		factors[symbol]=temp

		# factors[symbol].index = pd.to_datetime(factors[symbol].index, format='%Y-%m-%d', errors='ignore')

	for iu_file in iu_files:
		tmp = pd.read_csv(os.path.join(iu_path, iu_file))
		tmp_list = tmp['symbol'].tolist()
		dataout=[]
		date = datetime(int(iu_file[0:4]), int(iu_file[5:7]), int(iu_file[8:10]))
		for symbol in tmp_list:
			if symbol in list(factors.keys()):
				temp_symbol=factors[symbol].loc[[date],:]
				dataout.append(temp_symbol)
		dataout=pd.concat(dataout)
		data=dataout
		data.set_index('symbol',inplace=True)

		CSfactors_path = os.path.join(us_path, "CS_Factors")
		data['MARKET_CAP']=data['EQY_SH_OUT_RAW']*data['PX_LAST_RAW']
		data['CLOSE']=data['PX_LAST_RAW']
		data.to_csv(os.path.join(CSfactors_path,iu_file))


