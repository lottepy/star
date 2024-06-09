"""The module contains two functions
func getGDPReleaseDate. Get the GDP release date between start and end. So that we can train before
    the GDP release date

func dataScreening. Given an observation day, select the variable which has more .4 or less than -0.4
    correlation with GDP.

__main__ is a script to calculate factors for a set of forecast dates. The same script can be found in QuantimacroCore.py.
"""
from statsmodels.tsa.stattools import adfuller

from Algo.DFM import *
from Algo.US_Data_Cleaning import *


def getGDPReleaseDate(start = '2009-01-01', end = '2019-01-31'):
    endog = pd.read_csv(RAWDATAPATH + "\\US\\GDP QoQ Index.csv")
    endog = endog[['Date', 'Period', 'Actual']]
    endog.columns = ['Release Date', 'Period', 'y']
    endog['Release Date'] = pd.to_datetime(endog['Release Date'])
    endog['Date'] = endog['Release Date']
    endog['Period'] = endog['Period'].apply(lambda x: x[-1])
    endog = endog[endog['Period'] == "A"]


    endog.set_index('Date',inplace=True)
    endog = endog[endog.index <= pd.to_datetime(end)]
    endog = endog[endog.index >= pd.to_datetime(start)]
    return endog.index

def dataScreening(mtr_dt,path,date,country=""):
    rst = initial_correlation(path,date,country = country)
    df = pd.DataFrame()
    rst.set_index('variable_name', inplace=True)
    variables = rst.index.values
    for variable in variables:
        flag = ''
        testValue = rst.loc[variable, 'corr_raw']
        if not np.isnan(testValue):
            if testValue > 0.4 or testValue < -0.4:
                s = mtr_dt[variable]
                x = s.dropna().values
                if adfuller(x)[1] < 0.1:
                    flag += 'r'
        testValue = rst.loc[variable, 'corr_fo']
        if not np.isnan(testValue):
            if testValue > 0.4 or testValue < -0.4:
                flag += 'f'
        testValue = rst.loc[variable, 'corr_gr']
        if not np.isnan(testValue):
            if testValue > 0.4 or testValue < -0.4:
                flag += 'g'
        df.loc[date, variable] = flag

    r_list = []
    f_list = []
    g_list = []

    for variable in variables:
        if len(df.loc[date,variable])>0:
            flag = df.loc[date,variable][0]
            if flag == 'r':
                r_list.append(variable)
            elif flag == 'f':
                f_list.append(variable)
            elif flag == 'g':
                g_list.append(variable)
    return r_list,f_list,g_list



if __name__ == '__main__':
    qtr_dt, mtr_dt, wkl_dt = load_data(QTR_PATH, MTR_PATH, WKL_PATH, 'US')

    # forecast_dates = pd.date_range(end='2019-01-01',periods = 2, freq = "Q")
    forecast_dates = getGDPReleaseDate(end = '2019-02-28')
    # forecast_dates = [i+MonthEnd()-pd.offsets.DateOffset(7) for i in forecast_dates]
    forecast_dates = [i - pd.offsets.DateOffset(1) for i in forecast_dates]
    align_no_ = [(i.day-1) // 7 + 1 if i.day <28 else 4 for i in forecast_dates ]
    forecast_dates = [i.strftime('%Y-%m-%d') for i in forecast_dates]

    for ii in range(len(forecast_dates)):
        date = forecast_dates[ii]
        align_no = align_no_[ii]
        # =================================
        r_list,f_list,g_list = dataScreening(MTR_PATH,date,country = 'US')
        # =================================

        try:
            factors = calcFactors(date,align_no,mtr_dt,window=10,k_factors=2,MONTHLY_UNCHG_LIST = r_list,MONTHLY_FO_DIFF_LIST = f_list,MONTHLY_GROWTH_LIST = g_list)
        except:
            factors = pd.DataFrame()
        factors.to_csv(RESULTSPATH + '\\Factors6\\'+ date + '.csv')

