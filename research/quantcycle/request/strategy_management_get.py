import datetime
import json
import math
import os
import uuid

import numpy as np
import pandas as pd
import requests
from pandas.tseries.offsets import BDay

req_url = "http://172.29.39.140:1234/api/strategy"
#req_url = "http://192.168.8.130:1234/api/strategy"

if __name__ == "__main__":
    
    request_id = uuid.uuid1().hex


    params = {}
    res = requests.get(url=req_url,headers={'x-request-id': request_id}, json = params)
    print(res.text) 

