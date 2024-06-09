from choice_proxy_client import choice_client as c
from code_list import code_list_A_F as code_list
from datetime import datetime, timedelta
import os
from pandas import DataFrame
from time import sleep

DATA_BASE_PATH = "../data/"
DATA_PATHS = [DATA_BASE_PATH, DATA_BASE_PATH+"cmc/", DATA_BASE_PATH+"csd/", DATA_BASE_PATH+"css/", DATA_BASE_PATH+"csqsnapshot/"]
for path in DATA_PATHS:
    if not os.path.exists(path):
        os.mkdir(path)

def get_csqsnapshot():
    indicators = "DATE,TIME,NOW,BUYPRICE1,SELLPRICE1"
    selected_code_list = code_list.split(',')[:12]
    for code in selected_code_list:
        if not os.path.exists(DATA_BASE_PATH + "csqsnapshot/" + code + ".csv"):
            with open(DATA_BASE_PATH + "csqsnapshot/" + code + ".csv", 'a') as f:
                f.write(f"{indicators},START_TIME,END_TIME,DELAY\n")
    while True:
        data = DataFrame()
        count = 0
        while data.empty:
            count += 1
            if count > 10:
                sleep(1)
            if count > 20:
                print(data)
                print("csqsnapshot fails 20 times")
                break
            start_time = datetime.now();(st1,st2) = start_time.strftime('%H:%M:%S.%f').split('.')
            data = c.csqsnapshot(selected_code_list,indicators)
            end_time = datetime.now();(et1,et2) = end_time.strftime('%H:%M:%S.%f').split('.')
        for code in selected_code_list:
            with open(DATA_BASE_PATH + "csqsnapshot/" + code + ".csv", 'a') as f:
                data[code].to_csv(f, header=False, line_terminator=',',index=False)
                f.write(f"{'%s.%03d' % (st1,int(st2)/1000)},"
                        f"{'%s.%03d' % (et1,int(et2)/1000)},"
                        f"{int(round(1000*(end_time-start_time).total_seconds()))}\n")
        sleep(3)

# get_css()
# get_csd()
# get_cmc()
get_csqsnapshot()