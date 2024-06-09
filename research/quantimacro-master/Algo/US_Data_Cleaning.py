import datetime
import os

import numpy as np
import pandas as pd
from scipy.stats.stats import pearsonr

from Constants import *


def matchquarter(release_date):
    if release_date.month <= 3 and release_date.month >= 1:
        date = datetime.datetime(release_date.year - 1, 12, 31)
    elif release_date.month <= 6 and release_date.month >= 4:
        date = datetime.datetime(release_date.year, 3, 31)
    elif release_date.month <= 9 and release_date.month >= 7:
        date = datetime.datetime(release_date.year, 6, 30)
    elif release_date.month <= 12 and release_date.month >= 10:
        date = datetime.datetime(release_date.year, 9, 30)
    return date

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


def initial_correlation(path, date, frequency='monthly', country=''):
    GDP_QOQ = pd.read_csv(RAWDATAPATH + "\\US\\GDP QoQ Index.csv", parse_dates=['Date'])
    GDP_QOQ = GDP_QOQ[GDP_QOQ['Period'].str.contains('A')]
    GDP_QOQ['date'] = GDP_QOQ['Date'].map(matchquarter)
    GDP_QOQ = GDP_QOQ.set_index('date')
    # ====================
    GDP_QOQ['Actual'] = GDP_QOQ['Actual'].apply(lambda x: eval(x[:-1]) if x[-1] == "%" else np.nan)
    # ====================
    if frequency == 'monthly':
        mtr_data_files = os.listdir(path)
        data_list = []
        for mtr_data_file in mtr_data_files:
            file_path = path + "\\" + mtr_data_file
            if os.path.isfile(file_path):
                (name, exts) = os.path.splitext(mtr_data_file)
                if exts == '.csv':
                    mtr_temp_in = pd.read_csv(file_path, parse_dates=['date'], index_col=['date'])
                    variable_name = name.replace(RAWDATAPREFIX[country], '')
                    mtr_temp_in = mtr_temp_in.loc[:, ['ECO_RELEASE_DT', 'ACTUAL_RELEASE']]
                    mtr_temp_in.dropna(inplace=True)
                    mtr_temp_out = mtr_temp_in
                    mtr_temp_out['ECO_RELEASE_DT'] = mtr_temp_out['ECO_RELEASE_DT'].map(lambda x: int(x))
                    mtr_temp_out['TSP'] = mtr_temp_out['ECO_RELEASE_DT'].map(
                        lambda x: datetime.datetime(int(x / 10000), int((x % 10000) / 100), int(x % 100)))
                    mtr_temp_out = mtr_temp_out.loc[:, ['ACTUAL_RELEASE', 'TSP']]

                    # ==========================
                    mtr_temp_out = mtr_temp_out[mtr_temp_out['TSP']<=datetime.datetime.strptime(date,'%Y-%m-%d')]
                    # ==========================

                    mtr_temp_out['release_lag'] = mtr_temp_out['TSP'] - mtr_temp_out.index
                    mtr_temp_out['release_lag'] = mtr_temp_out['release_lag'].map(lambda x: x.days)
                    release_lag = mtr_temp_out['release_lag'].mean()

                    mtr_temp_out['FODIFF'] = mtr_temp_out['ACTUAL_RELEASE'].diff()
                    mtr_temp_out['GROWTH'] = mtr_temp_out['ACTUAL_RELEASE'].diff() / mtr_temp_out[
                        'ACTUAL_RELEASE'].shift().map(lambda x: abs(x))
                    mtr_temp_out = mtr_temp_out.resample('1M').last()
                    mtr_temp_out['ACTUAL_RELEASE'] = mtr_temp_out['ACTUAL_RELEASE'].fillna(method='ffill')
                    mtr_temp_out['FODIFF'] = mtr_temp_out['FODIFF'].fillna(method='ffill')
                    mtr_temp_out['GROWTH'] = mtr_temp_out['GROWTH'].fillna(method='ffill')
                    mtr_temp_out = mtr_temp_out.ffill()

                    mtr_temp_out = mtr_temp_out.resample('Q').mean()
                    mtr_temp_out = mtr_temp_out.dropna()
                    mtr_temp_out = pd.merge(mtr_temp_out, GDP_QOQ['Actual'], left_index=True, right_index=True)
                    if len(mtr_temp_out)>0:
                        corr_raw = np.round(pearsonr(mtr_temp_out['ACTUAL_RELEASE'], mtr_temp_out['Actual'])[0], 3)
                        corr_fo = np.round(pearsonr(mtr_temp_out['FODIFF'], mtr_temp_out['Actual'])[0], 3)
                        corr_gr = np.round(pearsonr(mtr_temp_out['GROWTH'], mtr_temp_out['Actual'])[0], 3)
                    else:
                        corr_raw = corr_fo = corr_gr = np.nan
                    rst_tmp = pd.DataFrame([variable_name, release_lag, corr_raw, corr_fo, corr_gr]).transpose()
                    rst_tmp.columns = ['variable_name', 'release_lag', 'corr_raw', 'corr_fo', 'corr_gr']
                    data_list.append(rst_tmp)

        rst = pd.concat(data_list)

    return rst

if __name__ == "__main__":
    forecast_dates = getGDPReleaseDate(end='2019-02-28')
    forecast_dates = [i - pd.offsets.DateOffset(1) for i in forecast_dates]
    forecast_dates = [i.strftime('%Y-%m-%d') for i in forecast_dates]

    # df = pd.DataFrame([MONTHLY_LIST])
    # df.columns = MONTHLY_LIST
    # df = df.drop(0)
    df = pd.DataFrame()

    for date in forecast_dates:
        rst = initial_correlation(MTR_PATH,date,country = 'US')
        rst.set_index('variable_name',inplace=True)
        variables = rst.index.values
        for variable in variables:
            flag = ''
            testValue = rst.loc[variable,'corr_raw']
            if not np.isnan(testValue):
                if testValue>0.4 or testValue < -0.4:
                    flag += 'r'
            testValue = rst.loc[variable,'corr_fo']
            if not np.isnan(testValue):
                if testValue>0.4 or testValue < -0.4:
                    flag += 'f'
            testValue = rst.loc[variable,'corr_gr']
            if not np.isnan(testValue):
                if testValue>0.4 or testValue < -0.4:
                    flag += 'g'
            df.loc[date,variable] = flag

    df.to_csv(RAWDATAPATH+'\\US\\variable_corr.csv')