# required packages: pandas, odps
import re
from datetime import datetime as dt
import os

# doc: https://help.aliyun.com/document_detail/34615.html

ALLOWED_INDICATORS = ['symbol','timestamp','date','preclose','open','high','low','last','totalvolumetrade','totalvaluetrade','iopv','totalbidqty','totalaskqty',
                    *[f'{a}{b}' for a in ['a','b','av','bv'] for b in range(1,11)]]
TABLE_NAME = 'level2_data_imported'

def get_hf_data(symbols, indicators, start_day, end_day, extra_requirements=None, outFilePath=None):
    # handle symbols
    assert type(symbols) in [str, list], f"The format of symbols ({symbols}) should be either string or list, check commonalgo/data/high_frequency_data_client.py for details"
    symbol_pattern = re.compile("^\d{6}.S[H|Z]$")

    if type(symbols) == str:
        if ',' in symbols:
            symbol_list = symbols.upper().replace(' ','').split(',')
        else:
            symbol_list = symbols.upper().split()
    else:
        symbol_list = [symbol.upper() for symbol in symbols]
    symbols_in_sql = "("
    for symbol in symbol_list:
        assert symbol_pattern.match(symbol), f"The symbol you input is not valid: {symbol}, check commonalgo/data/high_frequency_data_client.py for syntax"
        if not len(symbols_in_sql) == 1:
            symbols_in_sql += " or "
        symbols_in_sql += f"symbol='{symbol}'"
    symbols_in_sql += ")"

    #handle indicators
    assert type(indicators) in [str, list], f"The format of indicators ({indicators}) should be either string or list, check commonalgo/data/high_frequency_data_client.py for details"
    if type(indicators) == str:
        if ',' in indicators:
            indicator_list = indicators.lower().replace(' ','').split(',')
        else:
            indicator_list = indicators.lower().split()
    else:
        indicator_list = [indicator.lower() for indicator in indicators]
    for indicator in indicator_list:
        assert indicator in ALLOWED_INDICATORS, f"The indicator you input is not supported: {indicator}, check commonalgo/data/high_frequency_data_client.py for allowed indicators"
    indicators_in_sql = ','.join(indicator_list)

    #handle start_day, end_day
    day_pattern = re.compile("^\d{4}[-]?\d{2}[-]?\d{2}$")
    assert start_day != None, "You should input a start_day!"
    assert end_day != None, "You should input an end_day!"
    assert day_pattern.match(start_day), f"The start_day you input is not valid: {start_day}, check commonalgo/data/high_frequency_data_client.py for syntax"
    assert day_pattern.match(end_day), f"The end_day you input is not valid: {end_day}, check commonalgo/data/high_frequency_data_client.py for syntax"
    start_day_in_sql = start_day.replace('-', '')
    end_day_in_sql = end_day.replace('-', '')
    assert dt.strptime(start_day_in_sql, "%Y%m%d") <= dt.strptime(end_day_in_sql, "%Y%m%d"), f"start_day ({start_day}) should be < end_day ({end_day})"

    #handle extra_requirements
    extra_requirements_in_sql = " and " + extra_requirements if extra_requirements else ""

    sql_str = f"select {indicators_in_sql} from {TABLE_NAME} where day>={start_day_in_sql} and day<={end_day_in_sql} and {symbols_in_sql}{extra_requirements_in_sql}"
    print(sql_str)
    home = os.path.expanduser("~")
    cache_dir = os.path.join(home, 'hf_data_cache')
    cache_file_name = os.path.join(cache_dir, ''.join(x for x in sql_str if x.isalpha() or x.isdigit()) + '.pkl')
    if os.path.exists(cache_file_name):
        from pandas import read_pickle
        result = read_pickle(cache_file_name)
    else:
        from odps import ODPS
        import time
        o = ODPS('LTAIdXteU42OrXqy', 'Cggf8jCyJ8KTAKaizDQGJOcFZDGojM',
             project='hft_hk', endpoint='http://service.cn-hongkong.maxcompute.aliyun.com/api')
        instance = o.run_sql(sql_str)  # 异步的方式执行
        current_time = time.time()
        if os.path.exists(cache_dir):
            for f in os.listdir(cache_dir):
                creation_time = os.path.getctime(os.path.join(cache_dir, f))
                if (current_time - creation_time) // (24 * 3600) >= 7:
                    os.unlink(f)
        instance.wait_for_success()  # 阻塞直到完成
        result = instance.open_reader().to_pandas()
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        result.to_pickle(cache_file_name)
    if outFilePath:
        result.to_csv(outFilePath)
    return result

        
    



# sample usages: 
# a = get_hf_data("000001.SZ, 600000.SH",      "symbol,timestamp,preclose,low,last","2019-01-02", "2019-01-04", "last=low", 'hf_data.csv')
# print(a.head())
# a = get_hf_data("000001.SZ,600000.SH",       "a1 a2  a3   a4    a5",              "20190102",   "20190104")
# print(a.head())
# a = get_hf_data("000001.SZ 600000.SH",       ["bv1","bv2","bv3","bv4","bv5"],     "2019-0102",  "2019-0104")
# print(a.head())
# a = get_hf_data("000001.Sz,      600000.sH", "IOPV,ToTaLaSkQtY,totalbidqty",      "201901-02",  "201901-04")
# print(a.head())
# a = get_hf_data("000001.sz       600000.sh", "totalvolumetrade,totalvaluetrade",  "2019-01-02", "2019-01-04")
# print(a.head())
# a = get_hf_data(["000001.SZ", "600000.SH"],  "date,open,high,low,close",          "2019-01-04", "2019-01-02") # WRONG: start_day should be < end_day and it should raise asssertion error
# print(a.head())
