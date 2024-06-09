# -*- coding:utf-8 -*-
from code_list import code_list_A_F as code_list
from datetime import datetime, timedelta
import os
from time import sleep
from EmQuantAPI import *
from pandas import DataFrame
import traceback
import atexit
from threading import Timer

DATA_BASE_PATH = "../data_raw_final/"
DATA_PATHS = [DATA_BASE_PATH, DATA_BASE_PATH+"cmc/", DATA_BASE_PATH+"csd/", DATA_BASE_PATH+"css/", DATA_BASE_PATH+"csqsnapshot/"]
for path in DATA_PATHS:
    if not os.path.exists(path):
        os.mkdir(path)

FIRST_DATE_AGO = -30
TODAY_STRING = datetime.today().strftime('%Y%m%d')

def mainCallback(quantdata):
    """
    mainCallback 是主回调函数，可捕捉如下错误
    在start函数第三个参数位传入，该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    print("mainCallback", str(quantdata))
    # 登录掉线或者 登陆数达到上线（即登录被踢下线） 这时所有的服务都会停止
    if str(quantdata.ErrorCode) == "10001011" or str(quantdata.ErrorCode) == "10001009":
        print("Your account is disconnect. You can force login automatically here if you need.")
    # 行情登录验证失败（每次连接行情服务器时需要登录验证）或者行情流量验证失败时，会取消所有订阅，用户需根据具体情况处理
    elif str(quantdata.ErrorCode) == "10001021" or str(quantdata.ErrorCode) == "10001022":
        print("Your all csq subscribe have stopped.")
    # 行情服务器断线自动重连连续6次失败（1分钟左右）不过重连尝试还会继续进行直到成功为止，遇到这种情况需要确认两边的网络状况
    elif str(quantdata.ErrorCode) == "10002009":
        print("Your all csq subscribe have stopped.")
    else:
        pass


def startCallback(message):
    print("[EmQuantAPI Python]", message)
    return 1


def csqCallback(quantdata):
    """
    csqCallback 是EM_CSQ订阅时提供的回调函数模板。该函数只有一个为c.EmQuantData类型的参数quantdata
    :param quantdata:c.EmQuantData
    :return:
    """
    print("csqCallback,", str(quantdata))


def cstCallBack(quantdata):
    '''
    cstCallBack 是日内跳价服务提供的回调函数模板
    '''
    for i in range(0, len(quantdata.Codes)):
        length = len(quantdata.Dates)
        for it in quantdata.Data.keys():
            print(it)
            for k in range(0, length):
                for j in range(0, len(quantdata.Indicators)):
                    print(quantdata.Data[it][j * length + k], " ", end="")
                print()


def get_css(c):
    indicators = "OPEN, CLOSE"
    selected_code_list = code_list.split(',')
    for code in selected_code_list:
        if  not os.path.exists(DATA_BASE_PATH + "css/" + code + ".csv"):
            with open(DATA_BASE_PATH + "css/" + code + ".csv", 'a') as f:
                f.write(f"DATES,{indicators}\n")
    data = DataFrame()
    count = 0
    while data.empty:
        count += 1
        if count > 10:
            print("css fails 10 times")
            break
        data = c.css(code_list, indicators, "AdjustFlag=1, IsPandas=1")
        print("in css", data)
    for code in code_list.split(','):
        with open(DATA_BASE_PATH + "css/" + code + ".csv", 'a') as f:
            data.ix[code].to_frame().transpose().to_csv(f, header=False, index=False, line_terminator='\n')

def get_csd(c):
    indicators = "OPEN, CLOSE"
    selected_code_list = code_list.split(',')
    for code in selected_code_list:
        if not os.path.exists(DATA_BASE_PATH + "csd/" + code + ".csv"):
            with open(DATA_BASE_PATH + "csd/" + code + ".csv", 'a') as f:
                f.write(f"DATES,{indicators}\n")
    data = DataFrame()
    count = 0
    while data.empty:
        count += 1
        if count > 10:
            print("csd fails 10 times")
            break
        data = c.csd(code_list, indicators, (datetime.today() + timedelta(-1)).strftime("%Y-%m-%d"), (datetime.today() + timedelta(-1)).strftime("%Y-%m-%d"), "AdjustFlag=1, IsPandas=1")
    for code in code_list.split(','):
        with open(DATA_BASE_PATH + "csd/" + code + ".csv", 'a') as f:
            data.ix[code].to_frame().transpose().to_csv(f, header=False, index=False, line_terminator='\n')

def get_cmc(c):
    indicators = "OPEN, CLOSE"
    for code in code_list.split(","):
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
            data = c.cmc(code, indicators, (datetime.today() + timedelta(-2)).strftime("%Y-%m-%d"), (datetime.today() + timedelta(-1)).strftime("%Y-%m-%d"), "IsPandas=1")
            if isinstance(data, c.EmQuantData):
                print(code)
                break
        if isinstance(data, c.EmQuantData):
            continue
        with open(DATA_BASE_PATH + "cmc/" + code + ".csv", 'a') as f:
            data.to_csv(f, header=False, line_terminator='\n',index=False)

def execute_css_csd_cmc():
    try:
        c.start("ForceLogin=1", '', mainCallback)
        get_css(c)
        print('css finishes' + datetime.now().strftime("%H:%M:%S.%f"))
        get_csd(c)
        print('csd finishes' + datetime.now().strftime("%H:%M:%S.%f"))
        get_cmc(c)
        print('cmc finishes' + datetime.now().strftime("%H:%M:%S.%f"))
        c.stop()
        tmp_var_for_execute_css_csd_cmc_at_8am_1 = datetime.today()
        tmp_var_for_execute_css_csd_cmc_at_8am_2 = \
            tmp_var_for_execute_css_csd_cmc_at_8am_1.replace(
                day=tmp_var_for_execute_css_csd_cmc_at_8am_1.day,
                hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)
        tmp_var_for_execute_css_csd_cmc_at_8am_3 = tmp_var_for_execute_css_csd_cmc_at_8am_2 - tmp_var_for_execute_css_csd_cmc_at_8am_1
        # tmp_var_for_execute_css_csd_cmc_at_8am_4 = Timer(tmp_var_for_execute_css_csd_cmc_at_8am_3.total_seconds(),
        #                                                  execute_css_csd_cmc)
        tmp_var_for_execute_css_csd_cmc_at_8am_4 = Timer(1, execute_css_csd_cmc)
        tmp_var_for_execute_css_csd_cmc_at_8am_4.start()
    except Exception as ee:
        print("error >>>", ee)
        traceback.print_exc()
    else:
        print("demo end")

try:
    tmp_var_for_execute_css_csd_cmc_at_8am_1 = datetime.today()
    tmp_var_for_execute_css_csd_cmc_at_8am_2 = \
        tmp_var_for_execute_css_csd_cmc_at_8am_1.replace(
            day=tmp_var_for_execute_css_csd_cmc_at_8am_1.day,
            hour=8, minute=0, second=0, microsecond=0) + timedelta(days=1)
    tmp_var_for_execute_css_csd_cmc_at_8am_3 = tmp_var_for_execute_css_csd_cmc_at_8am_2 - tmp_var_for_execute_css_csd_cmc_at_8am_1
    # tmp_var_for_execute_css_csd_cmc_at_8am_4 = Timer(tmp_var_for_execute_css_csd_cmc_at_8am_3.total_seconds(), execute_css_csd_cmc)
    tmp_var_for_execute_css_csd_cmc_at_8am_4 = Timer(1, execute_css_csd_cmc)
    tmp_var_for_execute_css_csd_cmc_at_8am_4.start()

    # get_csqsnapshot(c)

except Exception as ee:
    print("error >>>", ee)
    traceback.print_exc()
else:
    print("demo end")

def logout_at_abnormal_exit():
    try:
        c.stop()
    except Exception as ee:
        print("error >>>", ee)
        traceback.print_exc()

atexit.register(logout_at_abnormal_exit)