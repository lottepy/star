# -*- coding:utf-8 -*-
from code_list import code_list_A_F as code_list
from datetime import datetime, timedelta
import os
from time import sleep
from EmQuantAPI import *
import subprocess
import traceback
import atexit

DATA_BASE_PATH = "../test_csq/"
WHICH_TO_TEST = "FX" # FX / A / FU
DATA_PATH = DATA_BASE_PATH + WHICH_TO_TEST + "/"
if not os.path.exists(DATA_PATH):
    os.makedirs(DATA_PATH)

all_indicators = {
    "FX":"DATE,TIME,NOW,HIGH,LOW,OPEN,PRECLOSE,ROUNDLOT,BUYPRICE1,SELLPRICE1",
    "A": "DATE,TIME,NOW,HIGH,LOW,OPEN,PRECLOSE,ROUNDLOT,VOLUME,AMOUNT,VOLUMERATIO,COMMISSIONRATIO,COMMISSIONDIFF,TRADESTATUS,OUTVOLUME,HIGHLIMIT,LOWLIMIT,SPEED,AVERAGEPRICE,BUYPRICE1,BUYPRICE2,BUYPRICE3,BUYPRICE4,BUYPRICE5,BUYVOLUME1,BUYVOLUME2,BUYVOLUME3,BUYVOLUME4,BUYVOLUME5,SELLPRICE1,SELLPRICE2,SELLPRICE3,SELLPRICE4,SELLPRICE5,SELLVOLUME1,SELLVOLUME2,SELLVOLUME3,SELLVOLUME4,SELLVOLUME5",
    "FU": "DATE,TIME,NOW,HIGH,LOW,OPEN,PRECLOSE,CLEAR,PRECLEAR,ROUNDLOT,VOLUME,AMOUNT,SPREAD,OUTVOLUME,HIGHLIMIT,LOWLIMIT,CAPITALFLOW,OPENINTEREST,BUYPRICE1,BUYVOLUME1,SELLPRICE1,SELLVOLUME1"
}

all_code_list = {
    "FX":"EURUSD.FX,GBPUSD.FX,NZDUSD.FX,USDTWD.FX,USDCHF.FX".split(','),
    "A": "000410.SZ,002230.SZ,002351.SZ,002352.SZ,603042.SH,603559.SH,603569.SH,600179.SH".split(','),
    "FU": "A1909.DCE,AL1907.SHF,CJ1912.CZC,CS1909.DCE,CU1907.SHF,CY2001.CZC,AP1910.CZC,M1909.DCE,AG1912.SHF,SC1907.INE".split(',')
}


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

def csq_callback(data):
    # 每个时间可能在同一个汇率出现多笔交易，这个时候choice也会发过来，如果想完全避免记录这种重复的数据，需要用lock
    # 这里暂时不需要，我先不写了
    # 交易量过大的时候也会出现顺序混乱的情况，所以...现在也需要写lock，但是这样的话速度会跟不上？我猜？
    # print(data)
    for code in data.Data.keys():
        # last_price = subprocess.check_output(['tail', '-1', DATA_BASE_PATH + code[:6] + ".csv"]).split(',')
        f1 = open(DATA_PATH+code[:6]+".csv","r")
        last_price = f1.readlines()[-1].rstrip().split(',')
        f1.close()
        new_price = data.Data[code]
        with open(DATA_PATH + code[:6] + ".csv", 'a') as f:
            new_price = [str(new_price[i]) if new_price[i] != None else last_price[i] for i in range(len(new_price))]
            # print(new_price)
            # print(new_price, last_price)
            if not (new_price[1] == last_price[1] and new_price[2] == last_price[2]):
                # print('prints')
                f.write(",".join(new_price)+"\n")

def get_csq(c, indicators, code_list):
    for code in code_list:
        if  not os.path.exists(DATA_PATH + code[:6] + ".csv"):
            with open(DATA_PATH + code[:6] + ".csv", 'a') as f:
                f.write(f"{indicators}\n")
    while True:
        data = c.csq(code_list, indicators,"Pushtype=0", csq_callback)
        sleep(3)
try:
    loginResult = c.start("ForceLogin=1", '', mainCallback)
    if (loginResult.ErrorCode != 0):
        print("login in fail")
        exit()

    get_csq(c, all_indicators[WHICH_TO_TEST], all_code_list[WHICH_TO_TEST])
    #
    logoutResult = c.stop()
except Exception as ee:
    print("error >>>", ee)
    traceback.print_exc()
else:
    print("demo end")

def logout_at_abnormal_exit():
    try:
        c.csqcancel(0)
        c.stop()
    except Exception as ee:
        print("error >>>", ee)
        traceback.print_exc()

atexit.register(logout_at_abnormal_exit)