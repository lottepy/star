import json, requests, platform, re, uuid
from pandas import DataFrame
from multiprocessing import Pool, cpu_count
from functools import partial
from itertools import product
from datetime import datetime, timedelta
import logging

EXS = {"SZ": "SZSE", "SH": "SSE", "HK": "HKEX", "DCE": "DCE", "SHFE": "SHFE", "CZCE": "CZCE", "INE": "INE",
       "CFFEX": "CFFEX"}
ENDPOINT = "http://192.168.11.62"
PORT = 8000

logger = logging.getLogger('history_data_client')
logger.setLevel(logging.INFO)
fh = logging.FileHandler('history_data_client.log', encoding='utf-8')
formatter = logging.Formatter('%(asctime)s|%(msecs)d|%(name)s|%(levelname)s|%(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

n_cpus = cpu_count()

REGEXS = [re.compile("\d{6}\.S[H|Z]"),
          re.compile("\d{1,5}\.HK"),
          re.compile("[A-Za-z]{1,2}(\d{3,4}|_M1)\.[A-Z]{3,5}")]

def _VALID_AQMALGO_ID(symbol):
    """Verity that a symbol is a valid AQMALGO_ID.
       Rule based verification now. Need to query database later.
    """
    return any([regex.match(symbol) for regex in REGEXS])


def _symbol_to_sec_type(symbol):
    if "SH" in symbol or "SZ" in symbol:
        return "cn_equity"
    elif "DCE" in symbol or "SHFE" in symbol or "CZCE" in symbol or "INE" in symbol or "CFFEX" in symbol:
        return "cn_futures"
    elif "HK" in symbol:
        return "hk_equity"


# no need for extra else as we already check the validility before

def _make_get_request(req_url, func=lambda x: x):
    # print(req_url)
    i = 0
    while i < 10:
        try:
            request_id = uuid.uuid1().hex
            logger.info(f"{req_url}|{request_id}")
            res = requests.get(req_url, headers={'x-request-id': request_id})
            if res.status_code != 404:
                return func(res.content.decode())
            else:
                raise ConnectionError
        except:
            i += 1
            print('connection error, will try later..')
            time.sleep(5)
    raise Exception(
        f"Cannot make a request from server, please check if {ENDPOINT}:{PORT} is down.\n\tRequest url: {req_url},\n\tStatus code: {res.status_code},\n\tText: {res.text}")


def _request_data_for_multiprocessing(params, category, indicators):
    day_or_year = "day" if category in ['snapshot', 'trade'] else 'year'
    req_url = f'{ENDPOINT}:{PORT}/{category}/symbol={params[0]}&{day_or_year}={params[1]}&indicators={indicators}'
    func = lambda x: [y.split(',') for y in x.replace('\r', '').split('\n')[:-1]]
    return _make_get_request(req_url, func)


# res = requests.get(f'{ENDPOINT}:{PORT}/{category}/symbol={params[0]}&{day_or_year}={params[1]}')
# if res.status_code != 404:
#     return [x.split(',') for x in res.content.decode().replace('\r','').split('\n')[:-1]]
# return []

def _get_snapshot_or_trade_data(symbol, indicators, category, start_day, end_day):
    ex = EXS[symbol.split('.')[1]]
    req_url = f'{ENDPOINT}:{PORT}/exchange_trading_days/{ex}&start_day={start_day}&end_day={end_day}'
    func = lambda x: json.loads(x)[ex]
    trading_days = _make_get_request(req_url, func)
    # res = requests.get(f'{ENDPOINT}:{PORT}/exchange_trading_days/{ex}&start_day={start_day}&end_day={end_day}')
    # if res.status_code != 404:
    #     trading_days = json.loads(res.content.decode())[ex]
    # else:
    #     raise Exception("needs to add retry later")
    # if platform.system() == 'Windows':
    if True:
        ress = []
        for day in trading_days:
            #req_url = f'{ENDPOINT}:{PORT}/{category}/symbol={symbol}&day={day}'
            req_url = f'{ENDPOINT}:{PORT}/{category}/symbol={symbol}&day={day}&indicators={indicators}'
            func = lambda x: [y.split(',') for y in x.replace('\r', '').split('\n')[:-1]]
            ress.extend(_make_get_request(req_url, func))
        # res = requests.get(f'{ENDPOINT}:{PORT}/{category}/symbol={symbol}&day={day}')
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
        req_url = f'{ENDPOINT}:{PORT}/data_header/category={category}&sec_type={sec_type}'
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

    # if platform.system() == 'Windows':
    if True:
        ress = []
        for year in range(_start_year, _end_year + 1):
            req_url = f'{ENDPOINT}:{PORT}/{category}/symbol={symbol}&year={year}&indicators={indicators}'
            func = lambda x: [y.split(',') for y in x.replace('\r', '').split('\n')[:-1]]
            ress.extend(_make_get_request(req_url, func))
        # res = requests.get(f'{ENDPOINT}:{PORT}/{category}/symbol={symbol}&year={year}')
        # if res.status_code != 404:
        #     ress.extend([x.split(',') for x in res.content.decode().replace('\r','').split('\n')[:-1]])
    else:
        p = Pool(n_cpus)
        make_get_req_for_category = partial(_request_data_for_multiprocessing, category=category, indicators=indicators)
        ress = p.map(make_get_req_for_category, product([symbol], range(_start_year, _end_year + 1)))
        ress = sum(ress, [])
        p.terminate()
        p = None

    if indicators == '*':
        sec_type = _symbol_to_sec_type(symbol)
        req_url = f'{ENDPOINT}:{PORT}/data_header/category={category}&sec_type={sec_type}'
        columns = eval(_make_get_request(req_url))
    else:
        columns = indicators.split("+")
    # print(ress[:5])
    # print(indicators.split("+"))
    df = DataFrame(ress, columns=columns)
    return df


def get_trading_calendar(ex, start_day, end_day):
    req_url = f'{ENDPOINT}:{PORT}/exchange_trading_days/{ex}&start_day={start_day}&end_day={end_day}'
    func = lambda x: json.loads(x)[ex]
    return _make_get_request(req_url, func)


def get_history_data(symbol, indicators, category, start_datetime, end_datetime):
    """Get history data

        Parameters
        ----------
        symbol : str
            A SINGLE AQMALGO_ID. Details please refer to:
            https://confluence-algo.aqumon.com/pages/viewpage.action?pageId=5505565
        category : str
            One of ["trade", "snapshot", "k_day", "k_min"]
        start_day : datetime.datetime
        end_day : datetime.datetime
    """

    assert isinstance(symbol, str) and _VALID_AQMALGO_ID(symbol), f"{symbol} is not a valid AQMALGO_ID"
    assert isinstance(indicators, str) or isinstance(indicators,
                                                     list), f"data type of {indicators} is not in [list, str]"
    assert isinstance(category, str) and category.lower() in ["trade", "snapshot", "k_day",
                                                              "k_min"], f"{category} if not valid category"
    assert isinstance(start_datetime, datetime), f"{start_datetime} is not a valid start_day"
    assert end_datetime == None or isinstance(end_datetime,
                                              datetime) and end_datetime <= datetime.today(), f"{end_datetime} is not a valid end_day"

    _symbol = symbol
    _indicators_input = indicators.replace(" ", "").split(",") if isinstance(indicators, str) else indicators
    _indicators = _indicators_input + []
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
    df = df[df.timestamp >= str(_start_timestamp)]  # 这里需要用 string 进行比较，因为数据库里的单位不统一，日K分K肯定都是秒（当天GMT0点0分0秒），但是有的会到毫秒
    df = df[df.timestamp < str(_end_timestamp)].reset_index(drop=True) # 使用左闭右开区间, 否则会多取一天的数据. 该逻辑只测试了获取daily数据部分, 可能会影响snapshot/trade data 获取
    if not "timestamp" in indicators and indicators != '*':
        df = df.drop('timestamp', 1)
    return df[_indicators_input] if _indicators[0] != '*' else df


if __name__ == "__main__":
    # ts = datetime.now()
    # df = get_history_data("000001.SZ", '*', "snapshot", datetime(2018, 1, 2), datetime(2018, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df[['a2', 'a1', 'b1', 'b2']])
    #
    # ts = datetime.now()
    # df = get_history_data("600000.SH", '*', "snapshot", datetime(2018, 1, 2), datetime(2018, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df[['a2', 'a1', 'b1', 'b2']])
    #
    # ts = datetime.now()
    # df = get_history_data("000001.SZ", '*', "snapshot", datetime(2019, 1, 2), datetime(2019, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df[['a2', 'a1', 'b1', 'b2']])
    #
    # ts = datetime.now()
    # df = get_history_data("600000.SH", '*', "snapshot", datetime(2019, 1, 2), datetime(2019, 1, 2))
    # te = datetime.now()
    # print(te - ts)
    # print(df[['a2', 'a1', 'b1', 'b2']])
    #
    # ts = datetime.now()
    # df = get_history_data("000001.SZ", 'a2,a1,b1,b2', "snapshot", datetime(2018, 1, 2), datetime(2018, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("600000.SH", 'a2,a1,b1,b2', "snapshot", datetime(2018, 1, 2), datetime(2018, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("000001.SZ", 'a2,a1,b1,b2', "snapshot", datetime(2019, 1, 2), datetime(2019, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("600000.SH", 'a2,a1,b1,b2', "snapshot", datetime(2019, 1, 2), datetime(2019, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)

    ts = datetime.now()
    df = get_history_data("000001.SZ", "*", "trade", datetime(2019, 1, 2), datetime(2019, 1, 2))
    te = datetime.now()
    print(te - ts)
    print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("000001.SZ", "open,high,low,close", "k_day", datetime(2019, 1, 2), datetime(2019, 1, 10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("000008.SZ", "open,high,low,close", "k_min", datetime(2019, 1, 2), datetime(2019, 1, 3))
    # #df = get_history_data("000008.SZ", "*", "k_min", datetime(2019, 1, 2), datetime(2019, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    #print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("00001.HK", "open,high,low,close", "k_day", datetime(2019, 1, 2), datetime(2019, 1, 10))
    # df = get_history_data("00001.HK", "*", "k_day", datetime(2019, 1, 2), datetime(2019, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("00001.HK", "open,high,low,close", "k_min", datetime(2019, 1, 2), datetime(2019, 1, 3))
    # te = datetime.now()
    # print(te - ts)
    # print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("sc_M1.INE", "bidPrice1,bidVolume1,askPrice1,askVolume1", "snapshot", datetime(2019, 1, 2),
    # 					  datetime(2019, 1, 10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("sc_M1.INE", "open,high,low,close", "k_day", datetime(2019, 1, 2), datetime(2019, 1, 10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)
    #
    # ts = datetime.now()
    # df = get_history_data("sc_M1.INE", "*", "k_min", datetime(2019, 1, 2), datetime(2019, 1, 10))
    # te = datetime.now()
    # print(te - ts)
    # print(df)


