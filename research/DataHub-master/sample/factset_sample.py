import json
import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from six import string_types
import ast
from lib.commonalgo.data.factset_client import get_data

session = requests.session()


# data = get_data(['VXH19-CBF','VXJ19-CBF','VXK19-CBF']) # get future data
# data = get_data(['FDS-USA']) # get future data
# data = get_data(['CNU18-SIM','MHIU18-HKF','2822-HKG','SPY-US']) # get equity data
data = get_data(['5-HKG','2800-HKG']) # get equity data
print (data.loc['LAST_TRADED'])
print (data.loc['LAST_1'])
