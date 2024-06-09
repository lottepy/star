import json, requests, platform, re, uuid, time
import numpy as np
from pandas import DataFrame, merge
from multiprocessing import Pool, cpu_count
from functools import partial, wraps
from itertools import product
from datetime import datetime, timedelta
import logging

EXS = {"SZ":"SZSE","SH":"SSE","HK":"HKEX","DCE":"DCE","SHFE":"SHFE","CZCE":"CZCE","INE":"INE","CFFEX":"CFFEX"}
HFT_ENDPOINT = "http://192.168.11.62"
HFT_PORT = 8000
DM_ENDPOINT = 'http://algo-internal.aqumon.com/datamaster/'
PRICE_RELATED_FIELDS = ['preclose','open','high','low','close','last','price', *[aorb + str(i) for aorb in ['a','b'] for i in range(1,11)]]
logger = logging.getLogger('history_data_client')
logging.basicConfig(filename="history_data_client.log",
                            filemode='a',
                            format='%(asctime)s|%(msecs)d|%(name)s|%(levelname)s|%(message)s',
                            # datefmt='%H:%M:%S',
                            level=logging.INFO)
n_cpus = cpu_count()
_parallel = None

def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = '{}, Retrying in {} seconds...'.format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry  # true decorator
    return deco_retry

def _VALID_AQMALGO_ID(symbol):
    """Verity that a symbol is a valid AQMALGO_ID.
       Rule based verification now. Need to query database later.
    """
    regexs = [re.compile("\d{6}\.S[H|Z]"),
              re.compile("\d{1,5}\.HK"),
              re.compile("[A-Za-z]{1,2}(\d{3,4}|_M1)\.[A-Z]{3,5}")]
    return any([regex.match(symbol) for regex in regexs])

def _symbol_to_sec_type(symbol):
    if "SH" in symbol or "SZ" in symbol:
        return "cn_equity"
    elif "DCE" in symbol or "SHFE" in symbol or "CZCE" in symbol or "INE" in symbol or "CFFEX" in symbol:
        return "cn_futures"
    elif "HK" in symbol:
        return "hk_equity"
    # no need for extra else as we already check the validility before

def _make_get_request(req_url, func=lambda x:x):
    # print(req_url)
    i = 0
    while i < 10:
        request_id = uuid.uuid1().hex
        logger.info(f"{req_url}|{request_id}")
        res = requests.get(req_url, headers={'x-request-id': request_id})
        if res.status_code != 404:
            return func(res.content.decode())
        else:
            i += 1
    raise Exception(f"Cannot make a request from server, please check if {HFT_ENDPOINT}:{HFT_PORT} is down.\n\tRequest url: {req_url},\n\tStatus code: {res.status_code},\n\tText: {res.text}")

@retry(Exception, tries=10)
def _make_get_request_from_datamaster(method, params):
    req_url = DM_ENDPOINT + method
    request_id = uuid.uuid1().hex
    logger.info(f"{req_url}|{request_id}")
    res = requests.get(
        url=req_url,
        headers={'accept': 'application/json',
                    'Content-Type': 'application/json',
                    'x-request-id': request_id},
        params=params)
    if res.status_code == 200:
        return_data = json.loads(res.text)['data']['values']
        return return_data
    raise Exception(f"Cannot make a request from server, please check if {DM_ENDPOINT} is down.\n\tRequest url: {req_url},\n\tStatus code: {res.status_code},\n\tText: {res.text}")

def _request_data_for_multiprocessing(params, category, indicators):
    day_or_year = "day" if category in ['snapshot', 'trade'] else 'year'
    req_url = f'{HFT_ENDPOINT}:{HFT_PORT}/{category}/symbol={params[0]}&{day_or_year}={params[1]}&indicators={indicators}'
    func = lambda x: [y.split(',') for y in x.replace('\r','').split('\n')[:-1]]
    return _make_get_request(req_url, func)
    # res = requests.get(f'{HFT_ENDPOINT}:{HFT_PORT}/{category}/symbol={params[0]}&{day_or_year}={params[1]}')
    # if res.status_code != 404:
    #     return [x.split(',') for x in res.content.decode().replace('\r','').split('\n')[:-1]]
    # return []

def _get_snapshot_or_trade_data(symbol, indicators, category, start_day, end_day):
    ex = EXS[symbol.split('.')[1]]
    req_url = f'{HFT_ENDPOINT}:{HFT_PORT}/exchange_trading_days/{ex}&start_day={start_day}&end_day={end_day}'
    func = lambda x: json.loads(x)[ex]
    trading_days = _make_get_request(req_url, func)
    # res = requests.get(f'{HFT_ENDPOINT}:{HFT_PORT}/exchange_trading_days/{ex}&start_day={start_day}&end_day={end_day}')
    # if res.status_code != 404:
    #     trading_days = json.loads(res.content.decode())[ex]
    # else:
    #     raise Exception("needs to add retry later")
    if (type(_parallel) != type(None) and _parallel == False) or platform.system() == 'Windows':
        print('not parallel')
        ress = []
        for day in trading_days:
            req_url = f'{HFT_ENDPOINT}:{HFT_PORT}/{category}/symbol={symbol}&day={day}&indicators={indicators}'
            func = lambda x: [y.split(',') for y in x.replace('\r','').split('\n')[:-1]]
            ress.extend(_make_get_request(req_url, func))
            # res = requests.get(f'{HFT_ENDPOINT}:{HFT_PORT}/{category}/symbol={symbol}&day={day}')
            # if res.status_code != 404:
            #     ress.extend([x.split(',') for x in res.content.decode().replace('\r','').split('\n')[:-1]])
    else:
        p = Pool(n_cpus)
        make_get_req_for_category = partial(_request_data_for_multiprocessing, category=category, indicators=indicators)
        ress = p.map(make_get_req_for_category, product([symbol], trading_days))
        ress = sum(ress, [])
        p.terminate()
        p = None
    if indicators == '*':
        sec_type = _symbol_to_sec_type(symbol)
        req_url = f'{HFT_ENDPOINT}:{HFT_PORT}/data_header/category={category}&sec_type={sec_type}'
        columns = eval(_make_get_request(req_url))
    else:
        columns = indicators.split("+")
    # print(ress[:5])
    # print(indicators.split("+"))
    df = DataFrame(ress, columns=columns)
    return df

def _get_k_day_or_k_min_data(symbol, indicators, category, start_year, end_year):
    _start_year = int(start_year)
    _end_year = int(end_year)

    if (type(_parallel) != type(None) and _parallel == False) or platform.system() == 'Windows':
        print('not parallel')
        ress = []
        for year in range(_start_year, _end_year+1):
            req_url = f'{HFT_ENDPOINT}:{HFT_PORT}/{category}/symbol={symbol}&year={year}&indicators={indicators}'
            func = lambda x: [y.split(',') for y in x.replace('\r','').split('\n')[:-1]]
            ress.extend(_make_get_request(req_url, func))
            # res = requests.get(f'{HFT_ENDPOINT}:{HFT_PORT}/{category}/symbol={symbol}&year={year}')
            # if res.status_code != 404:
            #     ress.extend([x.split(',') for x in res.content.decode().replace('\r','').split('\n')[:-1]])
    else:
        p = Pool(n_cpus)
        make_get_req_for_category = partial(_request_data_for_multiprocessing, category=category, indicators=indicators)
        ress = p.map(make_get_req_for_category, product([symbol], range(_start_year, _end_year+1)))
        ress = sum(ress, [])
        p.terminate()
        p = None
    if indicators == '*':
        sec_type = _symbol_to_sec_type(symbol)
        req_url = f'{HFT_ENDPOINT}:{HFT_PORT}/data_header/category={category}&sec_type={sec_type}'
        columns = eval(_make_get_request(req_url))
    else:
        columns = indicators.split("+")
    # print(ress[:5])
    # print(indicators.split("+"))
    df = DataFrame(ress, columns=columns)
    return df

def get_trading_calendar(ex, start_day, end_day):
    req_url = f'{HFT_ENDPOINT}:{HFT_PORT}/exchange_trading_days/{ex}&start_day={start_day}&end_day={end_day}'
    func = lambda x: json.loads(x)[ex]
    return _make_get_request(req_url, func)

def get_history_data(symbol, indicators, category, start_datetime, end_datetime, options: dict=None, parallel=None):
    """Get history data

        Data Parameters
        ----------
        symbol : str
            A SINGLE AQMALGO_ID. Details please refer to: 
            https://confluence-algo.aqumon.com/pages/viewpage.action?pageId=5505565
        category : str
            One of ["trade", "snapshot", "k_day", "k_min"]
        start_day : datetime.datetime
        end_day : datetime.datetime
        options : dict
            Supported options:
                adjustflag: one of [1,2,3] 1: no adjust, 2: backward adjust (hou fu quan) 3: forward adjust (qian fu quan)
                exchange: to get the right series of time-series, you need to pass this parameter before we bind instruments to their corresponding exchange.
                          This value is SSE by default
        Configuration Parameters
        --------
        parallel: boolean
            Use parallel or not. Default is determined by platform, but you can override it here.
            Difference between parallel and concurrent programming:
                https://cs.stackexchange.com/questions/19987/difference-between-parallel-and-concurrent-programming
    """
        
    assert isinstance(symbol, str) and _VALID_AQMALGO_ID(symbol), f"{symbol} is not a valid AQMALGO_ID"
    assert isinstance(indicators, str) or isinstance(indicators, list), f"data type of {indicators} is not in [list, str]"
    assert isinstance(category, str) and category.lower() in ["trade", "snapshot", "k_day", "k_min"], f"{category} if not valid category"
    assert isinstance(start_datetime, datetime), f"{start_datetime} is not a valid start_datetime"
    assert end_datetime == None or isinstance(end_datetime, datetime) and end_datetime <= datetime.today(), f"{end_datetime} is not a valid end_datetime"
    assert type(options) == type(None) or isinstance(options, dict), "The 'options' parameter should left unfilled or be a dict"
    assert type(parallel) == type(None) or isinstance(parallel, bool), "The 'parallel' should be left unfilled or one of True or False"

    _symbol = symbol
    _indicators_input = indicators.replace(" ","").split(",") if isinstance(indicators, str) else indicators
    _indicators = _indicators_input + []
    _options = {k.lower(): v for k,v in options.items()} if type(options) != type(None) and len(options) > 0 else {}
    global _parallel
    _parallel = parallel
    if _indicators[0] != '*':
        if "timestamp" in _indicators: _indicators.remove("timestamp")
        _indicators = ["timestamp"] + _indicators
    _indicators = '+'.join(_indicators)
    _category = category.lower()
    _start_timestamp = int(start_datetime.timestamp())
    _end_timestamp = int((end_datetime + timedelta(days=1)).timestamp())
    if category in ['snapshot', 'trade']:
        _start_day = start_datetime.strftime('%Y%m%d')
        _end_day = end_datetime.strftime('%Y%m%d')
        df = _get_snapshot_or_trade_data(_symbol, _indicators, _category, _start_day, _end_day)
    else:
        _start_year = start_datetime.year
        _end_year = end_datetime.year
        df = _get_k_day_or_k_min_data(_symbol, _indicators, _category, _start_year, _end_year)

    # print(_start_timestamp, _end_timestamp)
    df = df[df.timestamp >= str(_start_timestamp)] # 这里需要用 string 进行比较，因为数据库里的单位不统一，日K分K肯定都是秒（当天GMT0点0分0秒），但是有的会到毫秒
    df = df[df.timestamp <= str(_end_timestamp)].reset_index(drop=True)

    df = df.replace(r'^\s*$', np.nan, regex=True)
    
    if 'adjustflag' in _options and _options.get('adjustflag', 1) == 2:
        res = _make_get_request_from_datamaster('historical/',{
            'symbols': _symbol,
            'fields': 'choice_fq_factor',
            'start_date': start_datetime.strftime('%Y-%m-%d'),
            'end_date': end_datetime.strftime('%Y-%m-%d')
        })[_symbol]
        timestamp_to_date = lambda x: datetime.fromtimestamp(int(x[:10])).strftime('%Y-%m-%d')
        df['INTERNAL_date'] = df['timestamp'].apply(timestamp_to_date)
        df2 = DataFrame.from_records(res)
        df2.columns = ['INTERNAL_date', 'tafactor']
        days_of_price_data = set(df['INTERNAL_date'])
        days_of_tafactor = set(df2['INTERNAL_date'])
        days = get_trading_calendar(options.get('exchange', 'SSE'), start_datetime.strftime('%Y%m%d'), end_datetime.strftime('%Y%m%d'))
        for day in days:
            day_dash = day[:4] + '-' + day[4:6] + '-' + day[6:]
            assert day_dash in days_of_price_data, f'我们的高频数据没有 {day_dash} 这一天的数据'
            assert day_dash in days_of_tafactor, f'Datamaster 里的复权因子没有 {day_dash} 这一天的数据'
        # assert num_of_days_for_tafactor == num_of_days_for_price_data,  f'复权因子有 {num_of_days_for_tafactor} 天的数据，你取的高频数据覆盖了 {num_of_days_for_price_data} 天，这两个数字对不上'

        
        df = merge(df, df2, how='left')
        for i in PRICE_RELATED_FIELDS:
            if i in df.columns:
                df[i] = df[i].astype(np.float64, errors='ignore') * df['tafactor']
        df = df.drop(['tafactor', 'INTERNAL_date'], 1)

    if not "timestamp" in indicators and indicators != '*':
        df = df.drop('timestamp', 1)
    return df[_indicators_input] if _indicators[0] != '*' else df


if __name__ == "__main__":

    ts = datetime.now()
    df = get_history_data("000001.SZ", '*', "trade", datetime(2019,12,22), datetime(2019,12,23), {'adjustflag': 2}, parallel=True)
    te = datetime.now()
    print(te - ts)
    print(df)

    # ts = datetime.now()
    # df = get_history_data("600000.SH", '*', "snapshot", datetime(2018,1,2), datetime(2018,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df[['a2','a1','b1','b2']])

    # ts = datetime.now()
    # df = get_history_data("000001.SZ", '*', "snapshot", datetime(2019,1,2), datetime(2019,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df[['a2','a1','b1','b2']])

    # ts = datetime.now()
    # df = get_history_data("600000.SH", '*', "snapshot", datetime(2019,1,2), datetime(2019,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df[['a2','a1','b1','b2']])

    # ts = datetime.now()
    # df = get_history_data("000001.SZ", 'a2,a1,b1,b2', "snapshot", datetime(2018,1,2), datetime(2018,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("600000.SH", 'a2,a1,b1,b2', "snapshot", datetime(2018,1,2), datetime(2018,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("000001.SZ", 'a2,a1,b1,b2', "snapshot", datetime(2019,1,2), datetime(2019,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("600000.SH", 'a2,a1,b1,b2', "snapshot", datetime(2019,1,2), datetime(2019,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("000001.SZ", "price,volume", "trade", datetime(2019,1,2), datetime(2019,1,10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("000001.SZ", "open,high,low,close", "k_day", datetime(2019,1,2), datetime(2019,1,10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("000001.SZ", "open,high,low,close", "k_min", datetime(2019,1,2), datetime(2019,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("00001.HK", "open,high,low,close", "k_day", datetime(2019,1,2), datetime(2019,1,10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("00001.HK", "open,high,low,close", "k_min", datetime(2019,1,2), datetime(2019,1,3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("sc_M1.INE", "bidPrice1,bidVolume1,askPrice1,askVolume1", "snapshot", datetime(2019,1,2), datetime(2019,1,10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("sc_M1.INE", "open,high,low,close", "k_day", datetime(2019,1,2), datetime(2019,1,10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    # ts = datetime.now()
    # df = get_history_data("sc_M1.INE", "*", "k_min", datetime(2019,1,2), datetime(2019,1,10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

