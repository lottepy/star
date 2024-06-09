'''
The script first generate forecast dates. Then calculate factors for those forecast dates. Finally
    fit and forecast the GDP.
'''

from Algo.CalculateAndSaveFactors import *
from Algo.ForecastGDP import *
from Algo.US_Data_Cleaning import *

if __name__ == '__main__':
    # Parameters
    factorPath = RESULTSPATH + '\\Factors7'
    resultPath = RESULTSPATH
    resultName = 'f7_rslt2.csv'
    start = '2009-01-01'
    end = '2019-02-28'

    # Calculate Factors
    qtr_dt, mtr_dt, wkl_dt = load_data(QTR_PATH, MTR_PATH, WKL_PATH, 'US')
    forecast_dates = getGDPReleaseDate(start = start,end=end)
    forecast_dates = [i - pd.offsets.DateOffset(1) for i in forecast_dates]
    align_no_ = [(i.day - 1) // 7 + 1 if i.day < 28 else 4 for i in forecast_dates]
    forecast_dates = [i.strftime('%Y-%m-%d') for i in forecast_dates]

    for ii in range(len(forecast_dates)):
        date = forecast_dates[ii]
        align_no = align_no_[ii]
        # =================================
        r_list, f_list, g_list = dataScreening(mtr_dt,MTR_PATH, date, country='US')
        # =================================

        try:
            factors = calcFactors(date, align_no, mtr_dt, window=10, k_factors=2, MONTHLY_UNCHG_LIST=r_list,
                                  MONTHLY_FO_DIFF_LIST=f_list, MONTHLY_GROWTH_LIST=g_list)
        except:
            factors = pd.DataFrame()
        factors.to_csv(factorPath + '\\' + date + '.csv')

    new_y = forecast(factorPath)
    new_y.to_csv(resultPath+'\\'+resultName)