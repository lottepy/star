from lib.commonalgo.data.high_frequency_data_client import *
import pandas as pd
# pip install odps

symbol_list = ["000001.SZ", "600001.SH"]
tag_list = ["symbol","timestamp","preclose","a1","last"]
start_str = '2018-01-01'
end_str = '2018-01-10'
params_str = 'preclose>0'
savefile_path = None

data = get_hf_data(
    symbols=symbol_list,
    indicators=tag_list,
    start_day=start_str,
    end_day=end_str,
    extra_requirements=params_str,
    outFilePath=savefile_path

)
data = get_hf_data(
    "000001.SZ, 600000.SH",      "symbol,timestamp,preclose,a1,last", "2018-01-02", "2018-01-04", "preclose>0", 'hf_data.csv')

print (data.head())