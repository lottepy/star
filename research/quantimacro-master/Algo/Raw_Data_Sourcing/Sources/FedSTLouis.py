
#Source data from Federal Reserve Bank of St. Louis

import datetime

import matplotlib.pyplot as plt
import pandas as pd
import pandas_datareader.data as web
import statsmodels.api as sm

from Constants import *

if __name__ == "__name__":

	start = datetime.datetime(1985, 1, 1)
	end = datetime.datetime.today()

	variable_list = ['PAYEMS', 'GDPC1', 'CPIAUCSL', 'DGORDER', 'RSAFS', 'HSN1F', 'HOUST', 'UNRATE', 'INDPRO',
					 'PPIFIS', 'NPPTTL', 'GACDISA066MSFRBNY', 'I42IMSM144SCEN', 'TTLCONS', 'GACDFSA066MSFRBPHI', 'IR',
					 'PERMIT', 'TCU', 'PCEPILFE', 'CPILFESL', 'BUSINV', 'ULCNFB', 'JTSJOL',
					 'PCEC96', 'PCEPI', 'IQ', 'AMDMVS', 'AMTMUO', 'AMDMTI', 'A261RX1Q020SBEA', 'DSPIC96',
					 'BOPTEXP', 'BOPTIMP']

	mth_var_list = ['PAYEMS', 'CPIAUCSL', 'DGORDER', 'RSAFS', 'HSN1F', 'HOUST', 'UNRATE', 'INDPRO',
					 'PPIFIS', 'NPPTTL', 'GACDISA066MSFRBNY', 'I42IMSM144SCEN', 'TTLCONS', 'GACDFSA066MSFRBPHI', 'IR',
					 'PERMIT', 'TCU', 'PCEPILFE', 'CPILFESL', 'BUSINV', 'JTSJOL',
					 'PCEC96', 'PCEPI', 'IQ', 'AMDMVS', 'AMTMUO', 'AMDMTI', 'DSPIC96',
					 'BOPTEXP', 'BOPTIMP', 'NAPM', 'NMFCI', 'NAPMPRI', 'NAPMEI']

	qtr_var_list = ['GDPC1','ULCNFB','A261RX1Q020SBEA']


	US_official_dt = web.DataReader(variable_list, 'fred', start, end)
	ISM_dt = pd.read_csv(RAWDATAPATH + "\\US_ISM.csv", parse_dates=['DATE'])
	ISM_dt = ISM_dt.set_index('DATE')
	US_official_dt = pd.merge(US_official_dt, ISM_dt, left_index=True, right_index=True)
	US_official_dt.to_csv(RAWDATAPATH + "\\US_official.csv")

	US_official_dt = pd.read_csv(RAWDATAPATH + "\\US_official.csv", parse_dates=['DATE'])
	US_official_dt.set_index('DATE', inplace=True)
	US_official_dt_processed = pd.DataFrame()
	mth_dt_processed = pd.DataFrame()
	qtr_dt_processed = pd.DataFrame()
	qtr_dt = US_official_dt.loc[:,['GDPC1','ULCNFB','A261RX1Q020SBEA']].resample('Q').first()
	mth_dt = US_official_dt.drop(columns=['GDPC1','ULCNFB','A261RX1Q020SBEA']).resample('1M').first()

	mth_dt_processed['PAYEMS'] = mth_dt['PAYEMS'].diff()
	qtr_dt_processed['GDPC1'] = qtr_dt['GDPC1'] / qtr_dt['GDPC1'].shift() - 1
	mth_dt_processed['NAPM'] = mth_dt['NAPM']
	mth_dt_processed['CPIAUCSL'] = mth_dt['CPIAUCSL'] / mth_dt['CPIAUCSL'].shift() - 1
	mth_dt_processed['DGORDER'] = mth_dt['DGORDER'] / mth_dt['DGORDER'].shift() - 1
	mth_dt_processed['RSAFS'] = mth_dt['RSAFS'] / mth_dt['RSAFS'].shift() - 1
	mth_dt_processed['HSN1F'] = mth_dt['HSN1F'] / mth_dt['HSN1F'].shift() - 1
	mth_dt_processed['HOUST'] = mth_dt['HOUST'] / mth_dt['HOUST'].shift() - 1
	mth_dt_processed['UNRATE'] = mth_dt['UNRATE'].diff()
	mth_dt_processed['INDPRO'] = mth_dt['INDPRO'] / mth_dt['INDPRO'].shift() - 1
	mth_dt_processed['PPIFIS'] = mth_dt['PPIFIS'] / mth_dt['PPIFIS'].shift() - 1
	mth_dt_processed['NPPTTL'] = mth_dt['NPPTTL'].diff()
	mth_dt_processed['GACDISA066MSFRBNY'] = mth_dt['GACDISA066MSFRBNY']
	mth_dt_processed['I42IMSM144SCEN'] = mth_dt['I42IMSM144SCEN'] / mth_dt['I42IMSM144SCEN'].shift() - 1
	mth_dt_processed['TTLCONS'] = mth_dt['TTLCONS'] / mth_dt['TTLCONS'].shift() - 1
	mth_dt_processed['GACDFSA066MSFRBPHI'] = mth_dt['GACDFSA066MSFRBPHI']
	mth_dt_processed['IR'] = mth_dt['IR'] / mth_dt['IR'].shift() - 1
	mth_dt_processed['NMFCI'] = mth_dt['NMFCI']
	mth_dt_processed['NAPMPRI'] = mth_dt['NAPMPRI']
	mth_dt_processed['PERMIT'] = mth_dt['PERMIT'].diff()
	mth_dt_processed['TCU'] = mth_dt['TCU'].diff()
	mth_dt_processed['PCEPILFE'] = mth_dt['PCEPILFE'] / mth_dt['PCEPILFE'].shift() - 1
	mth_dt_processed['CPILFESL'] = mth_dt['CPILFESL'] / mth_dt['CPILFESL'].shift() - 1
	mth_dt_processed['BUSINV'] = mth_dt['BUSINV'] / mth_dt['BUSINV'].shift() - 1
	qtr_dt_processed['ULCNFB'] = qtr_dt['ULCNFB'] / qtr_dt['ULCNFB'].shift() - 1
	mth_dt_processed['JTSJOL'] = mth_dt['JTSJOL'].diff()
	mth_dt_processed['PCEC96'] = mth_dt['PCEC96'] / mth_dt['PCEC96'].shift() - 1
	mth_dt_processed['PCEPI'] = mth_dt['PCEPI'] / mth_dt['PCEPI'].shift() - 1
	mth_dt_processed['NAPMEI'] = mth_dt['NAPMEI']
	mth_dt_processed['IQ'] = mth_dt['IQ'] / mth_dt['IQ'].shift() - 1
	mth_dt_processed['AMDMVS'] = mth_dt['AMDMVS'] / mth_dt['AMDMVS'].shift() - 1
	mth_dt_processed['AMTMUO'] = mth_dt['AMTMUO'] / mth_dt['AMTMUO'].shift() - 1
	mth_dt_processed['AMDMTI'] = mth_dt['AMDMTI'] / mth_dt['AMDMTI'].shift() - 1
	qtr_dt_processed['A261RX1Q020SBEA'] = qtr_dt['A261RX1Q020SBEA'] / qtr_dt['A261RX1Q020SBEA'].shift() - 1
	mth_dt_processed['DSPIC96'] = mth_dt['DSPIC96'] / mth_dt['DSPIC96'].shift() - 1
	mth_dt_processed['BOPTEXP'] = mth_dt['BOPTEXP'] / mth_dt['BOPTEXP'].shift() - 1
	mth_dt_processed['BOPTIMP'] = mth_dt['BOPTIMP'] / mth_dt['BOPTIMP'].shift() - 1

	US_official_dt_processed = pd.merge(left=mth_dt_processed, right=qtr_dt_processed, how='outer', left_index=True, right_index=True)
	US_official_dt_processed.to_csv(RAWDATAPATH + "\\US_official_dt_processed.csv")

	mth_dt_std = pd.DataFrame()
	qtr_dt_std = pd.DataFrame()
	for var in mth_var_list:
		mth_dt_std[var] = (mth_dt_processed[var] - mth_dt_processed[var].mean()) / mth_dt_processed[var].std()
	for var in qtr_var_list:
		qtr_dt_std[var] = (qtr_dt_processed[var] - qtr_dt_processed[var].mean()) / qtr_dt_processed[var].std()

	US_official_dt_std = pd.merge(left=mth_dt_std, right=qtr_dt_std, how='outer', left_index=True, right_index=True)
	US_official_dt_std.to_csv(RAWDATAPATH + "\\US_official_dt_std.csv")

	#endog = US_official_dt_std.drop(columns=['GDPC1','ULCNFB','A261RX1Q020SBEA'])
	#'PAYEMS', 'CPIAUCSL', 'DGORDER', 'RSAFS', 'HSN1F', 'HOUST', 'UNRATE', 'INDPRO',
	#  'GACDISA066MSFRBNY', 'I42IMSM144SCEN', 'TTLCONS', 'GACDFSA066MSFRBPHI', 'IR',
	# 'PERMIT', 'TCU', 'PCEPILFE', 'CPILFESL', 'BUSINV', 'JTSJOL',
	# 'PCEC96', 'PCEPI', 'IQ', 'AMDMVS', 'AMTMUO', 'AMDMTI', 'DSPIC96',
	# 'BOPTEXP', 'BOPTIMP', 'NAPM', 'NMFCI', 'NAPMPRI', 'NAPMEI'

	#Short History 'PPIFIS', 'NPPTTL',

	test_var_list = ['PAYEMS', 'CPIAUCSL', 'DGORDER', 'RSAFS', 'HSN1F', 'HOUST', 'UNRATE', 'INDPRO',
					 'GACDISA066MSFRBNY', 'I42IMSM144SCEN', 'TTLCONS', 'GACDFSA066MSFRBPHI', 'IR',
					 'PCEC96', 'NAPM', 'NMFCI', 'NAPMPRI', 'NAPMEI'
					 ]

	endog = US_official_dt_std.loc[:, test_var_list]
	endog = endog.dropna()

	mod = sm.tsa.DynamicFactor(endog, k_factors=2, factor_order=2, error_order=2)
	initial_res = mod.fit(method='powell', disp=False, maxiter=10000000)
	res = mod.fit(initial_res.params, disp=False, maxiter=10000000)

	# Plot the factor 1
	fig, ax1 = plt.subplots(figsize=(13, 3))
	dates = test_data.index._mpl_repr()
	ax1.plot(dates, test_data['f1'], label='Factor 1')
	ax1.legend()
	plt.show()

	# Plot the factor 2
	fig, ax2 = plt.subplots(figsize=(13, 3))
	dates = test_data.index._mpl_repr()
	ax2.plot(dates, test_data['f2'], label='Factor 2')
	ax2.legend()
	plt.show()

	fig, ax3 = plt.subplots(figsize=(13, 3))
	dates = test_data.index._mpl_repr()
	ax3.plot(dates, test_data['f3'], label='Factor 3')
	ax3.legend()
	plt.show()

	fig, ax4 = plt.subplots(figsize=(13, 3))
	dates = test_data.dropna().index._mpl_repr()
	ax4.plot(dates, test_data.dropna()['GDP_QOQ'], label='GDP QoQ')
	ax4.legend()
	plt.show()

