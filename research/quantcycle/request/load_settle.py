import argparse
import datetime
import json
import math
import os
import uuid

import numpy as np
import pandas as pd
import requests
from pandas.tseries.offsets import BDay

parser = argparse.ArgumentParser()
parser.add_argument("--port", help="port for prod server", type=int)
parser.add_argument("--bday", help="port for prod server", type=int)
parser.add_argument("--dt", help="data dt for prod server", type=str)
parser.add_argument("--ip", help="data dt for prod server", type=str)
parser.add_argument("--engine_name", help="engine_name for prod server", type=str)
args = parser.parse_args()


if __name__ == "__main__":
    print(f"start") 
    port = args.port if args.port else 1234
    ip = args.ip if args.ip else "172.29.39.140"
    engine_name = args.engine_name if args.engine_name else None
    req_url = f"http://{ip}:{port}/api/engine_manager_control"
    print(f"req_url:{req_url}") 
    request_id = uuid.uuid1().hex
    bday = args.bday if args.bday else 0
    dt = args.dt if args.dt else (datetime.datetime.today() + BDay(bday)).strftime('%Y%m%d') 
    #dt = "20200922"
    params = { "task_type":"load_settle","timepoint":dt,"engine_name":engine_name}
    print(f"params:{params}") 
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
    print(res.text) 
