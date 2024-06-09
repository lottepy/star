# -*- coding:utf-8 -*-
from code_list import code_list_A_F as code_list
from datetime import datetime, timedelta
import os
from time import sleep
from EmQuantAPI import *
from pandas import DataFrame
import traceback
import atexit

DATA_BASE_PATH = "../data_raw_csqsnapshot/"
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

def get_csqsnapshot(c):
    indicators = "TIME,NOW,BUYPRICE1,SELLPRICE1"
    selected_code_list = code_list.split(',')
    for code in selected_code_list:
        if  not os.path.exists(DATA_BASE_PATH + "csqsnapshot/" + code + ".csv"):
            with open(DATA_BASE_PATH + "csqsnapshot/" + code + ".csv", 'a') as f:
                f.write(f"DATES,{indicators},START_TIME,END_TIME,DELAY\n")
    while True:
        data = DataFrame()
        count = 0
        try:
            while data.empty:
                count += 1
                if count > 10:
                    sleep(1)
                if count > 20:
                    print(data)
                    print("csqsnapshot fails 20 times")
                    break
                start_time = datetime.now();(st1, st2) = start_time.strftime('%H:%M:%S.%f').split('.')
                data = c.csqsnapshot(selected_code_list,indicators, "Ispandas=1")
                end_time = datetime.now();(et1,et2) = end_time.strftime('%H:%M:%S.%f').split('.')
            for code in selected_code_list:
                with open(DATA_BASE_PATH + "csqsnapshot/" + code + ".csv", 'a') as f:
                    data.ix[code].to_frame().transpose().to_csv(f, header=False, index=False, line_terminator=',')
                    f.write(f"{'%s.%03d' % (st1,int(st2)/1000)},"
                            f"{'%s.%03d' % (et1,int(et2)/1000)},"
                            f"{int(round(1000*(end_time-start_time).total_seconds()))}\n")
        except Exception as ee:
            print(datetime.now().strftime('%H:%M:%S.%f'), ee)
        sleep(3.4)

try:
    loginResult = c.start("ForceLogin=1", '', mainCallback)
    if (loginResult.ErrorCode != 0):
        print("login in fail")
        exit()

    # get_css(c)
    # get_csd(c)
    # get_cmc(c)
    get_csqsnapshot(c)
    #
    logoutResult = c.stop()
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