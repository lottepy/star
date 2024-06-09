from choice_proxy_client import choice_client as c
from code_list import code_list_A_F as code_list
from datetime import datetime, timedelta
import os
from pandas import DataFrame
from time import sleep
from threading import Timer

DATA_BASE_PATH = "../data_final/"
DATA_PATHS = [DATA_BASE_PATH, DATA_BASE_PATH+"cmc/", DATA_BASE_PATH+"csd/", DATA_BASE_PATH+"css/", DATA_BASE_PATH+"csqsnapshot/"]
for path in DATA_PATHS:
    if not os.path.exists(path):
        os.mkdir(path)

def get_css():
    indicators = "OPEN, CLOSE"
    for code in code_list.split(','):
        if not os.path.exists(DATA_BASE_PATH + "css/" + code + ".csv"):
            with open(DATA_BASE_PATH + "css/" + code + ".csv", 'a') as f:
                f.write(f"DATE, {indicators}\n")
    data = DataFrame()
    count = 0
    while data.empty:
        count += 1
        if count > 10:
            print("css fails 10 times")
            break
        data = c.css(code_list, indicators, "AdjustFlag=1")
    for code in code_list.split(','):
        with open(DATA_BASE_PATH + "css/" + code + ".csv", 'a') as f:
            data[code].to_csv(f, header=False, line_terminator='\n')


def get_csd():
    indicators = "OPEN, CLOSE"
    for code in code_list.split(','):
        if not os.path.exists(DATA_BASE_PATH + "csd/" + code + ".csv"):
            with open(DATA_BASE_PATH + "csd/" + code + ".csv", 'a') as f:
                f.write(f"DATE, {indicators}\n")
    data = DataFrame()
    count = 0
    while data.empty:
        count += 1
        if count > 10:
            print("csd fails 10 times")
            break
        data = c.csd(code_list, indicators, (datetime.today() + timedelta(-2)).strftime("%Y-%m-%d"), (datetime.today() + timedelta(-1)).strftime("%Y-%m-%d"), "AdjustFlag=1")
    for code in code_list.split(','):
        with open(DATA_BASE_PATH+"csd/"+code+".csv", 'a') as f:
            data[code].to_csv(f, header=False, line_terminator='\n')

def get_cmc():
    indicators = "OPEN, CLOSE"
    for code in code_list.split(','):
        if not os.path.exists(DATA_BASE_PATH + "cmc/" + code + ".csv"):
            with open(DATA_BASE_PATH + "cmc/" + code + ".csv", 'a') as f:
                f.write(f"DATE, {indicators}\n")
        data = DataFrame()
        count = 0
        while data.empty:
            count += 1
            if count > 10:
                print(f"cmc {code} fails 10 times")
                break
            data = c.cmc(code, indicators, (datetime.today() + timedelta(-2)).strftime("%Y-%m-%d"), (datetime.today() + timedelta(-1)).strftime("%Y-%m-%d"))
        with open(DATA_BASE_PATH+"cmc/"+code+".csv", 'a') as f:
            data.to_csv(f, header=False, line_terminator='\n')

def execute_css_csd_cmc():
    get_css()
    print('css finishes'+datetime.now().strftime("%H:%M:%S.%f"))
    get_csd()
    print('csd finishes' + datetime.now().strftime("%H:%M:%S.%f"))
    get_cmc()
    print('cmc finishes' + datetime.now().strftime("%H:%M:%S.%f"))
    tmp_var_for_execute_css_csd_cmc_at_8am_1 = datetime.today()
    tmp_var_for_execute_css_csd_cmc_at_8am_2 = \
        tmp_var_for_execute_css_csd_cmc_at_8am_1.replace(
            day=tmp_var_for_execute_css_csd_cmc_at_8am_1.day,
            hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)
    tmp_var_for_execute_css_csd_cmc_at_8am_3 = tmp_var_for_execute_css_csd_cmc_at_8am_2 - tmp_var_for_execute_css_csd_cmc_at_8am_1
    tmp_var_for_execute_css_csd_cmc_at_8am_4 = Timer(tmp_var_for_execute_css_csd_cmc_at_8am_3.total_seconds(),
                                                     execute_css_csd_cmc)
    # tmp_var_for_execute_css_csd_cmc_at_8am_4 = Timer(1,
    #                                                  execute_css_csd_cmc)
    tmp_var_for_execute_css_csd_cmc_at_8am_4.start()

tmp_var_for_execute_css_csd_cmc_at_8am_1 = datetime.today()
tmp_var_for_execute_css_csd_cmc_at_8am_2 =\
    tmp_var_for_execute_css_csd_cmc_at_8am_1.replace(
        day=tmp_var_for_execute_css_csd_cmc_at_8am_1.day,
        hour=8,minute=0,second=0,microsecond=0)+timedelta(days=1)
tmp_var_for_execute_css_csd_cmc_at_8am_3 = tmp_var_for_execute_css_csd_cmc_at_8am_2-tmp_var_for_execute_css_csd_cmc_at_8am_1
tmp_var_for_execute_css_csd_cmc_at_8am_4 = Timer(tmp_var_for_execute_css_csd_cmc_at_8am_3.total_seconds(), execute_css_csd_cmc)
# tmp_var_for_execute_css_csd_cmc_at_8am_4 = Timer(1, execute_css_csd_cmc)
tmp_var_for_execute_css_csd_cmc_at_8am_4.start()
# get_csqsnapshot()