'''This module contains six functions
func load_data. Load the raw data into the memory
INPUT:
qtr_path. str. the path where quarterly data is stored, default value is QTR_PATH as defined in Constatns.py
mtr_path. str. the path where monthly data is stored, default value is MTR_PATH as defined in Constatns.py
wkl_path. str. the path where weekly data is stored, default value is WKL_PATH as defined in Constatns.py
country. str. denote the country, default value "US"
OUTPUT:
qtr_align_dt, mtr_align_dt, wkl_align_dt. pd.DataFrame. Store all data available. The index is logical month
The columns have the pattern [variable, tsp_variable], which indicates the release date of each record.

func cut_data. Cut the raw data by date and pre-determined cleaning list
This func has not been completed yet. Need two parts more ("Q","W")
INPUT:
date. str. format:%Y-%m-%d. Observation Day.
df. DataFrame. DataFrame from func load_data.
freq. str. default value "M", indicates the frequence of the data.
OUTPUT:
data_list. list. Each object in the list is a DataFrame which has two columns. The first column is
    the variable name and the second column is the release date. The variable must be in the MONTHLY_LIST
    and the release date must be before the observation day.

func align_data. Re-align the data so that the data will not have staged tail.
dlist. list. The list from func cut_data.
align_no. int. default value 4, choosing from [1,2,3,4]. Each number represents one align method.
    1 indicates all the data released after last month's 7th and before this month's 7th will
        will be aligned together. Used when observation day is between 1st and 7th
    2 indicates all the data released after last month's 14th and before this month's 14th will
        will be aligned together. Used when observation day is between 8th and 14th
    3 indicates all the data released after last month's 21th and before this month's 21th will
        will be aligned together. Used when observation day is between 15th and 21th
    4 indicates all the data released this month will be aligned together. Used when observation
        day is after 22th.
freq. str. Default value "M"
OUTPUT:
dlist_. list. Aligned data list. The index of each components is the aligned date.

func process_data. Choose the data used to calculate factors according to three list.
INPUT:
dlist. list. Aligned data list from func align_data
freq. str. Default value "M"
window. int. Minimum length of data. The data with less length will be discarded.
min_valid. int. Maximum number of nan. The data with nan more than window-min_valid will be discarded.
MONTHLY_UNCHG_LIST. list. Contains the variable names that will be used directly
MONTHLY_FO_DIFF_LIST. list. Contains the variable names that will be used after first order difference.
MONTHLY_GROWTH_LIST. list. Contains the variable names that will be used after taking differences and divided by last value.
OUTPUT:
dataout. DataFrame. Data used to calculate the factors.

func insample. The main func to fetch monthly exogenous data

func getEndog. The main func to fetch endogenous data
INPUT:
date. str. obsecation day
type. str. Default value = "US_GDP_QOQ". Indicates the endogenous data type.
fillMethod. str. Default value "no", choosing from ["no","bfill","interpolation"]
OUTPUT:
endog. Dataframe. The index is the logical date for endogenous data.
'''

import datetime
import os

import numpy as np
import pandas as pd
from pandas.tseries.offsets import *

from Constants import *


def load_data(qtr_path, mtr_path, wkl_path, country):
    qtr_data_files = os.listdir(qtr_path)
    qtr_data_files_list = []
    for qtr_data_file in qtr_data_files:
        file_path = qtr_path + "\\" + qtr_data_file
        if os.path.isfile(file_path):
            (name, exts) = os.path.splitext(qtr_data_file)
            if exts == '.csv':
                qtr_temp_in = pd.read_csv(file_path, parse_dates=['date'], index_col=['date'])
                variable_name = name.replace(RAWDATAPREFIX[country],'')
                qtr_temp_in = qtr_temp_in.loc[:, ['ECO_RELEASE_DT', 'ACTUAL_RELEASE']]
                qtr_temp_in.dropna(inplace=True)
                qtr_temp_out = qtr_temp_in
                qtr_temp_out['ECO_RELEASE_DT'] = qtr_temp_out['ECO_RELEASE_DT'].map(lambda x: int(x))
                qtr_temp_out["tsp_" + variable_name] = qtr_temp_out['ECO_RELEASE_DT'].map(
                    lambda x: datetime.datetime(int(x / 10000), int((x % 10000) / 100), int(x % 100)))
                qtr_temp_out[variable_name] = qtr_temp_out['ACTUAL_RELEASE']
                qtr_temp_out = qtr_temp_out.loc[:, [variable_name, "tsp_" + variable_name]]
                qtr_temp_out = qtr_temp_out.resample('Q').last()
                if qtr_temp_out.shape[0] > 0:
                    qtr_data_files_list.append(qtr_temp_out)
    qtr_align_dt = qtr_data_files_list[0]
    for qtr_data_file in qtr_data_files_list[1:]:
        qtr_align_dt = pd.merge(qtr_align_dt, qtr_data_file, how='outer', left_index=True, right_index=True)
    qtr_align_dt = qtr_align_dt.drop_duplicates()

    mtr_data_files = os.listdir(mtr_path)
    mtr_data_files_list = []
    for mtr_data_file in mtr_data_files:
        file_path = mtr_path + "\\" + mtr_data_file
        if os.path.isfile(file_path):
            (name, exts) = os.path.splitext(mtr_data_file)
            if exts == '.csv':
                mtr_temp_in = pd.read_csv(file_path, parse_dates=['date'], index_col=['date'])
                variable_name = name.replace(RAWDATAPREFIX[country],'')
                mtr_temp_in = mtr_temp_in.loc[:, ['ECO_RELEASE_DT', 'ACTUAL_RELEASE']]
                mtr_temp_in.dropna(inplace=True)
                mtr_temp_out = mtr_temp_in
                mtr_temp_out['ECO_RELEASE_DT'] = mtr_temp_out['ECO_RELEASE_DT'].map(lambda x: int(x))
                mtr_temp_out["tsp_" + variable_name] = mtr_temp_out['ECO_RELEASE_DT'].map(
                    lambda x: datetime.datetime(int(x / 10000), int((x % 10000) / 100), int(x % 100)))
                mtr_temp_out[variable_name] = mtr_temp_out['ACTUAL_RELEASE']
                mtr_temp_out = mtr_temp_out.loc[:, [variable_name, "tsp_" + variable_name]]
                mtr_temp_out = mtr_temp_out.resample('1M').last()
                if mtr_temp_out.shape[0] > 0:
                    mtr_data_files_list.append(mtr_temp_out)
    mtr_align_dt = mtr_data_files_list[0]
    for mtr_data_file in mtr_data_files_list[1:]:
        mtr_align_dt = pd.merge(mtr_align_dt, mtr_data_file, how='outer', left_index=True, right_index=True)
    mtr_align_dt = mtr_align_dt.drop_duplicates()

    wkl_data_files = os.listdir(wkl_path)
    wkl_data_files_list = []
    for wkl_data_file in wkl_data_files:
        file_path = wkl_path + "\\" + wkl_data_file
        if os.path.isfile(file_path):
            (name, exts) = os.path.splitext(wkl_data_file)
            if exts == '.csv':
                wkl_temp_in = pd.read_csv(file_path, parse_dates=['date'], index_col=['date'])
                variable_name = name.replace(RAWDATAPREFIX[country],'')
                wkl_temp_in = wkl_temp_in.loc[:, ['ECO_RELEASE_DT', 'ACTUAL_RELEASE']]
                wkl_temp_in.dropna(inplace=True)
                wkl_temp_out = wkl_temp_in
                wkl_temp_out['ECO_RELEASE_DT'] = wkl_temp_out['ECO_RELEASE_DT'].map(lambda x: int(x))
                wkl_temp_out["tsp_" + variable_name] = wkl_temp_out['ECO_RELEASE_DT'].map(
                    lambda x: datetime.datetime(int(x / 10000), int((x % 10000) / 100), int(x % 100)))
                wkl_temp_out[variable_name] = wkl_temp_out['ACTUAL_RELEASE']
                wkl_temp_out = wkl_temp_out.loc[:, [variable_name, "tsp_" + variable_name]]
                wkl_temp_out = wkl_temp_out.resample('W').last()
                if wkl_temp_out.shape[0] > 0:
                    wkl_data_files_list.append(wkl_temp_out)
    wkl_align_dt = wkl_data_files_list[0]
    for wkl_data_file in wkl_data_files_list[1:]:
        wkl_align_dt = pd.merge(wkl_align_dt, wkl_data_file, how='outer', left_index=True, right_index=True)
    wkl_align_dt = wkl_align_dt.drop_duplicates()

    return qtr_align_dt, mtr_align_dt, wkl_align_dt

def cut_data(date, df, freq = 'M'):
    data_list = []
    if freq == 'M':
        for variable in MONTHLY_LIST:
            if variable in df.columns:
                tmp = df[df['tsp_'+variable]<=date]
                tmp = tmp.loc[:,[variable, 'tsp_'+variable]]
                data_list.append(tmp)

    if freq == 'Q':
        for variable in QTRLY_LIST:
            if variable in df.columns:
                tmp = df[df['tsp_'+variable]<=date]
                tmp = tmp.loc[:,[variable, 'tsp_'+variable]]
                data_list.append(tmp)

    if freq == 'W':
        for variable in WEEKLY_LIST:
            if variable in df.columns:
                tmp = df[df['tsp_'+variable]<=date]
                tmp = tmp.loc[:,[variable, 'tsp_'+variable]]
                data_list.append(tmp)

    return data_list

def align_data(dlist, align_no = 4, freq = 'M'):
    variables = [df.columns[0] for df in dlist]
    dlist_ = []

    if freq == 'M':
        for ii in range(len(dlist)):
            df = dlist[ii]
            variable = variables[ii]
            tmp = df.copy(deep = True)
            tmp['RYear'] = tmp['tsp_'+variable].apply(lambda x: x.year)
            tmp['RMonth'] = tmp['tsp_'+variable].apply(lambda x: x.month)
            tmp['RDay'] = tmp['tsp_'+variable].apply(lambda x: x.day)
            if align_no < 4:
                tmp['Align_date'] = tmp['tsp_'+variable].apply(lambda x: datetime.datetime(x.year,x.month,15)-MonthEnd() if x.day<=7*align_no else datetime.datetime(x.year,x.month,15)+MonthEnd())
            else:
                tmp['Align_date'] = tmp['tsp_'+variable].apply(lambda x: datetime.datetime(x.year,x.month,15)-MonthEnd())
            tmp['Align_month'] = tmp['Align_date'].apply(lambda x: x.year*12+x.month)
            tmp['Logic_date'] = tmp.index
            tmp['Logic_month'] = tmp['Logic_date'].apply(lambda x: x.year*12+x.month)
            tmp['diff'] = tmp['Align_month']-tmp['Logic_month']
            mode = tmp['diff'].mode()
            tmp['Align_date'] = tmp['Logic_date'].apply(lambda x: x + mode * MonthEnd())
            tmp.set_index('Align_date',inplace=True)
            tmp  = tmp.loc[:,[variable,'tsp_'+variable]]

            dlist_.append(tmp)

    return dlist_

def process_data(dlist, freq = 'M', window = 14, min_valid = None,MONTHLY_UNCHG_LIST = [],MONTHLY_FO_DIFF_LIST = [],MONTHLY_GROWTH_LIST = []):
    variables = [df.columns[0] for df in dlist]

    print("MONTHLY_UNCHG_DATA_LENGTH: {}".format(len(MONTHLY_UNCHG_LIST)))
    print("MONTHLY_FO_DIFF_DATA_LENGTH: {}".format(len(MONTHLY_FO_DIFF_LIST)))
    print("MONTHLY_GROWTH_DATA_LENGTH: {}".format(len(MONTHLY_GROWTH_LIST)))


    if min_valid == None:
        min_valid = window - 1

    if freq =="M":
        window = window * 12
        min_valid = min_valid *12
    elif freq =="Q":
        window = window * 4
        min_valid = min_valid * 4
    else:
        window = window * 52
        min_valid = min_valid * 52

    if len(dlist) >= 1:
        datain = dlist[0]
        if len(dlist) >= 2:
            for i in dlist[1:]:
                datain = pd.merge(datain, i, how='outer', left_index=True, right_index=True)


    if freq == "M":
        fo_diff_list_now = []
        for variable in MONTHLY_FO_DIFF_LIST:
            if variable in datain.columns:
                tmp = datain[variable]
                tmp = tmp.iloc[-(window + 1):]
                if tmp.dropna().shape[0] >= min_valid:
                    fo_diff_list_now.append(variable)

        growth_list_now = []
        for variable in MONTHLY_GROWTH_LIST:
            if variable in datain.columns:
                tmp = datain[variable]
                tmp = tmp.iloc[-(window + 1):]
                if tmp.dropna().shape[0] >= min_valid:
                    growth_list_now.append(variable)

        unchg_list_now = []
        for variable in MONTHLY_UNCHG_LIST:
            if variable in datain.columns:
                tmp = datain[variable]
                tmp = tmp.iloc[-(window + 1):]
                if tmp.dropna().shape[0] >= min_valid:
                    unchg_list_now.append(variable)

        variable_list = fo_diff_list_now + growth_list_now + unchg_list_now
        work_in_process = datain.loc[:, variable_list]
        work_in_process = work_in_process.resample('1M').last().interpolate()

        for variable in fo_diff_list_now:
            work_in_process[variable] = work_in_process[variable].diff()
            # work_in_process[variable] = (work_in_process[variable] - work_in_process[variable].mean(skipna=True)) / \
            #                             work_in_process[variable].std(skipna=True)

        for variable in growth_list_now:
            work_in_process[variable] = work_in_process[variable].diff() / abs(work_in_process[variable])
            work_in_process[variable].replace([np.inf, -np.inf], np.nan, inplace=True)

        for variable in unchg_list_now:
            work_in_process[variable] = (work_in_process[variable] - work_in_process[variable].mean(skipna=True)) / \
                                        work_in_process[variable].std(skipna=True)

        dataout = work_in_process.iloc[-(window + 1):]
        dataout.fillna(method = 'ffill',inplace = True)
        dataout = dataout.dropna()

    return dataout

def insample(date,align_no,mtr_dt,window = 14, min_valid = None,MONTHLY_UNCHG_LIST = [],MONTHLY_FO_DIFF_LIST = [],MONTHLY_GROWTH_LIST = []):
    dlist = cut_data(date,mtr_dt,freq = 'M')
    dlist = align_data(dlist,align_no = align_no, freq = 'M')
    df = pd.DataFrame()
    while(len(df.columns) == 0  and window > 0):
        df = process_data(dlist, freq = 'M',window = window,min_valid=min_valid,MONTHLY_UNCHG_LIST=MONTHLY_UNCHG_LIST,MONTHLY_FO_DIFF_LIST=MONTHLY_FO_DIFF_LIST,MONTHLY_GROWTH_LIST=MONTHLY_GROWTH_LIST)
        window -= 1
    return df

def getEndog(date,type = "US_GDP_QOQ",fillMethod = 'no'):
    if type == "US_GDP_QOQ":
        endog = pd.read_csv(RAWDATAPATH + "\\US\\GDP QoQ Index.csv")
        endog = endog[['Date', 'Period', 'Actual']]
        endog.columns = ['Release Date', 'Period', 'y']
        endog['Release Date'] = pd.to_datetime(endog['Release Date'])
        endog['Date'] = endog['Release Date']
        endog['Date'] = endog['Date'].apply(lambda x: x - QuarterEnd())
        endog['Period'] = endog['Period'].apply(lambda x: x[-1])
        endog = endog[endog['Period'] == "A"]
        endog['y'] = endog['y'].apply(lambda x: eval(x[:-1]) if x[-1] == "%" else np.nan)
        endog.dropna(axis=0, inplace=True)
        endog.drop(['Period'], axis=1, inplace=True)
        endog.set_index(['Date'], inplace=True)
        endog = endog[endog['Release Date'] <= datetime.datetime.strptime(date, '%Y-%m-%d')]
        endog.drop(['Release Date'], axis=1, inplace=True)

        begindate = endog.index[0]
        enddate = endog.index[-1]
        tmp_df = pd.DataFrame(index=pd.date_range(start=begindate, end=enddate, freq='M'))
        endog = pd.concat([endog, tmp_df], axis=1)

        if fillMethod == 'bfill':
            endog.fillna(method='bfill', inplace=True)
        elif fillMethod == 'interpolation':
            endog['y'] = GDP_QOQ['y'].interpolate(limit_direction='backward')
        else:
            pass

    return endog

if __name__ == "__main__":
    qtr_dt, mtr_dt, wkl_dt = load_data(QTR_PATH, MTR_PATH, WKL_PATH, 'US')
    date = '2018-12-31'
    align_no = 4
    df = insample(date,align_no,mtr_dt)