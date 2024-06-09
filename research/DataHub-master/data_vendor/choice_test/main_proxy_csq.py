from choice_proxy_client import choice_client as c
from datetime import datetime
import os
from pandas import DataFrame
from time import sleep

DATA_BASE_PATH = "../test_csq_proxy/"
WHICH_TO_TEST = "A" # FX / A / FU
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

def csqCallback(data):
    print(data)

def get_csq(code_list, indicators):
    for code in code_list:
        if  not os.path.exists(DATA_PATH + code[:6] + ".csv"):
            with open(DATA_PATH + code[:6] + ".csv", 'a') as f:
                f.write(f"{indicators}\n")
    while True:
        data = c.csq(code_list,indicators)
        print(data)
        # for code in code_list:
        #     with open(DATA_BASE_PATH + "csqsnapshot/" + code + ".csv", 'a') as f:
        #         data[code].to_csv(f, header=False, line_terminator=',',index=False)

        sleep(3)

# get_css()
# get_csd()
# get_cmc()
get_csq(all_code_list[WHICH_TO_TEST], all_indicators[WHICH_TO_TEST])