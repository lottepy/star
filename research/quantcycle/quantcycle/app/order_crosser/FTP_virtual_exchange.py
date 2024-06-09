import time
from datetime import datetime

import numpy as np
import pandas as pd
import pysftp
import os

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
HOST = '192.168.9.170'
USER = 'dymon'
PSWD = 'm0htXo!C'
PORT = 22


if __name__ == "__main__":
    
    date_list = []
    while True:
        sftp = pysftp.Connection(host=HOST, username=USER, password=PSWD, cnopts=cnopts)
        sftp.cwd('/share/dymon-test/')
        directory_files = sftp.listdir()
        for dir in directory_files:
            direction,dir_type,date = dir.split("_")
            date = date.split(".")[0]
            if dir_type == "Order" and date not in date_list:
                date_list.append(date)
                sftp.get(dir, dir)
                time.sleep(1)
                df = pd.read_csv(dir)
                df["PriceTraded"] = 1
                df["TimeExecuted"] = datetime.now()
                df = df[["CCY1","CCY2","FixingDate","SettlementDate","Amount","PriceTraded","Portfolio","TimeExecuted"]]
                print(dir)
                df.to_csv(f"Dymon2Aqumon_Executed_{date}.csv",index = False)
                sftp.put(f"Dymon2Aqumon_Executed_{date}.csv")
                os.remove(f"Dymon2Aqumon_Executed_{date}.csv")
        sftp.close()
        time.sleep(60)

