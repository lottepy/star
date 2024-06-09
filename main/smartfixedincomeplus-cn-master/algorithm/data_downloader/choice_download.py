# downloan trading data from datamater to update local data
import pandas as pd
import numpy as np
from choice_client import c
from algorithm import addpath
from datetime import datetime
from itertools import product as product
import os
import multiprocessing

indicators = ["TIncomeStatement_89",
              "TIncomeStatementQ_9",
              "TIncomeStatementQ_10",
              "TIncomeStatementQ_14",
              "TIncomeStatementQ_48",
              "TIncomeStatementQ_60",
              "TBalanceStatement_9",
              "TBalanceStatement_25",
              "TBalanceStatement_31",
              "TBalanceStatement_74",
              "TBalanceStatement_88",
              "TBalanceStatement_93",
              "TBalanceStatement_94",
              "TBalanceStatement_128",
              "TBalanceStatement_141",
              "TBalanceStatement_147",
              "TBalanceStatement_216",
              "TBalanceStatement_228",
              "TCashFlowStatementQ_39",
              "EVA",
              "ESTPE",
              "ESTPEG",
              "ESTINSTNUM",
              "ESTMEDIANROA",
              "ESTMEDIANROE",
              "REPORTDATEDIVIDEND",
              "PETTM",
              "PSTTM",
              "PEDYNAMIC",
              "PCFCFOTTM",
              "RATINGTARGETPRICE",
              "WRATINGINSTNUM",
              "WRATINGNUMOFBUY",
              "WRATINGNUMOFOUTPERFORM",
              "WRATINGNUMOFHOLD",
              "WRATINGNUMOFUNDERPERFORM",
              "WRATINGNUMOFSELL"]
indicators_2 = ["PUNLOCKDATE",
              "PPERIODUNLOCKAMT",
              "PTOTALUNLOCKAMT"]
bond_list = ["010107.SH"]

symbol_list_path = os.path.join(addpath.config_path, "ashare_symbol_list.csv")
stocks = pd.read_csv(symbol_list_path)['Stkcd'].tolist()
# stocks_data = open("stocks.txt", "r")
# stocks = stocks_data.read().split(',')
# stocks_data.close()

step = int(len(stocks)/10) + 1
downloaded = []

def half_year_after(end_date):
    year = int(end_date[0:4])
    month = int(end_date[4:6])
    day = end_date[6:8]
    if month > 6:
        year += 1
        month -= 6
    else:
        month += 6
    if month < 10:
        month = "0" + str(month)
    return str(year)+str(month)+str(day)

def download_all_fundamentals():
    year_list = [str(x) for x in range(2012, 2021)]
    # quarter_dict = {"0331":"0430", "0630":"0831", "0930":"1031", "1231":"0430"}
    quarter_dict = {"1231": "0430"}
    parameters = list(product(year_list, quarter_dict.keys()))
    for param in parameters:
        if param in downloaded:
            continue
        report_date = param[0] + param[1]
        end_date = (str(int(param[0]) + 1) if param[1] == "1231" else param[0]) + quarter_dict[param[1]]
        trade_date = end_date
        predict_year = str(int(param[0]) + 1) if param[1] == "1231" else param[0]
        print(report_date + ": start download")
        p = "ReportDate=" + report_date + ",type=1,EndDate=" + end_date + ",TradeDate=" + trade_date + ",cycle=180, PredictYear=" + predict_year
        p2 = "StartDate=" + end_date + ",EndDate=" + half_year_after(end_date)
        for i in range(0, len(stocks), step):
            stocks_chunk = stocks[i: i+step]
            # while True:
            #     try:
            data = c.css(stocks_chunk, indicators, p)
            print("p1 complete")
            data2 = c.css(stocks_chunk, indicators_2, p2)
            print("p2 complete")
            data = pd.concat([data, data2], axis=1)
            data.index = [end_date]
            data.to_csv(os.path.join(addpath.root_path, "data", "Ashare", "CN_stocks_raw", end_date) +"_"+ str(int(i/step)) + ".csv")
                #     break
                # except:
                #     from choice_client import c

def split_data():
    data_path = (os.path.join(addpath.root_path, "data", "Ashare", "CN_stocks_raw"))
    files = os.listdir(data_path)
    pool = multiprocessing.Pool()
    for file in files:
        # split_helper(data_path, file)
        pool.apply_async(split_helper, args=(data_path, file,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")


def split_helper(data_path, file):
    data = pd.read_csv(os.path.join(data_path, file), index_col=0)
    stocks_in_file = np.unique(['.'.join(x.split('.')[0:2]) for x in data.columns])
    for s in stocks_in_file:
        col_list = list(filter(None, [x if '.'.join(x.split('.')[0:2]) == s else None for x in data.columns]))
        s_d = data.loc[:, col_list]
        os.makedirs(os.path.join(addpath.data_path, "Ashare", "CN_stocks", s), exist_ok=True)
        s_d.to_csv(os.path.join(addpath.data_path, "Ashare", "CN_stocks", s, str(data.index[1]) + ".csv"))


def merge_data():
    reverse_dict = {"0501": "0331", "0901": "0630", "1101": "0930", "0430": "1231"}
    pool = multiprocessing.Pool()
    for s in stocks:
        # merge_helper(reverse_dict, s)
        pool.apply_async(merge_helper, args=(reverse_dict, s,))
        print('Sub-processes begins!')
    pool.close()  # 关闭进程池，表示不能再往进程池中添加进程，需要在join之前调用
    pool.join()  # 等待进程池中的所有进程执行完毕
    print("Sub-process(es) done.")

def merge_helper(reverse_dict, s):
    enddt_dict = {"0501": "0501", "0901": "0831", "1101": "1031", "0430": "0430"}
    s_path = os.path.join(addpath.data_path, "Ashare", "CN_stocks", s)
    files = os.listdir(s_path)
    data_list = []
    for file in files:
        data = pd.read_csv(os.path.join(s_path, file))
        data.columns = data.iloc[0]
        end_date = file[4:8]
        report_date = reverse_dict[end_date]
        if end_date == '0430':
            year_r = int(file[0:4]) - 1
        else:
            year_r = int(file[0:4])
        year = int(file[0:4])
        data['report_date'] = datetime(int(year_r), int(int(report_date)/100), int(int(report_date) - int(int(report_date)/100) * 100))
        data['end_date'] = datetime(int(year), int(int(enddt_dict[end_date])/100), int(int(enddt_dict[end_date]) - int(int(enddt_dict[end_date])/100) * 100))
        data = data.drop(index=[0])
        data = data.set_index('end_date')
        data_list.append(data)
    output_data = pd.concat(data_list, axis=0)
    output_data = output_data.dropna(subset=['TINCOMESTATEMENTQ_9'])
    output_data.to_csv(os.path.join(addpath.data_path, "Ashare", "financial_data", s + ".csv"))


def download_all_bonds():
    for bond in bond_list:
        data = c.csd(bond, "OPEN,CLOSE,HIGH,LOW,VOLUME,PCTCHANGE,RATEOFSTDBND", "2009-12-31", "2020-12-03",
                     "type=1,period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
        data.to_csv(os.path.join(addpath.root_path, "data", "CN_bonds", bond) + ".csv")


if __name__ == "__main__":
    # download_all_bonds()
    # download_all_fundamentals()
    # split_data()
    merge_data()
