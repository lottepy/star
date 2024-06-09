import collections
import os
import re
import threading
import time
import uuid
from functools import wraps

import numpy as np
import pandas as pd
import requests

from ..symbol.base import Symbols
from ..symbol.base import symbol_converter
from ...data.choice_proxy_client import choice_client

NUMBER_OF_RECONNECT = 5
SECONDS_TO_RECONNECT = 3
SYMBOL_PREFIX_MAPPING = {'CN': 'choice', 'HC': 'choice', 'HK': 'iuid', 'US': 'iuid', 'SG': 'choice', 'GB': 'iuid'}  # 不同区域主动拉取实时数据用到的symbol类型
DATA_SRC_MAPPING = {'CN': 'choice', 'HC': 'choice', 'SG': 'choice'}  # 主动拉取实时数据用到的数据源


def ensure_time_interval(seconds):
    """
    ensure a function will not be called within specified time interval
    """
    lock = threading.Lock()
    def decorate(func):
        last_time_called = [0.0]
        @wraps(func)
        def wrapper(*args, **kwargs):
            lock.acquire()
            elapsed = time.clock() - last_time_called[0]
            seconds_left_to_wait = seconds - elapsed
            if seconds_left_to_wait>0:
                time.sleep(seconds_left_to_wait)
            lock.release()
            ret = func(*args, **kwargs)
            last_time_called[0] = time.clock()
            return ret
        return wrapper
    return decorate

def reconnect(func):
    """Try to reconnect"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        for n in range(NUMBER_OF_RECONNECT):
            try:
                return func(*args, **kwargs)
            except requests.exceptions.RequestException as e:
                print("ConnectionError {}\nRetry after {} seconds.".format(e, SECONDS_TO_RECONNECT))
                time.sleep(SECONDS_TO_RECONNECT)
        raise ConnectionError("Number of reconnection exceeds {}.".format(NUMBER_OF_RECONNECT))
    return wrapper

def get_df_col_without_nan(df, col_name) -> pd.Series:
    """ Get a column with out nan from df
    """
    t = df[col_name]
    return t[t.notnull()]

def split_future_stock_df(df):
    if not len(df):
        return pd.DataFrame(), pd.DataFrame()
    idxs = df.index
    futures_mask = np.array([Symbols.is_future_iuid(i) for i in idxs])
    futures = df[futures_mask]

    stocks = df[~futures_mask]
    return futures, stocks

def future_from_hand_to_position(iuid, hands, lot_size):
    if Symbols.is_future_iuid(iuid):
        return hands * lot_size
    else:
        return hands

def future_from_position_to_hand(iuid, position, lot_size):
    if Symbols.is_future_iuid(iuid):
        if position % lot_size != 0:
            raise ValueError(f"Holdings {position} and lot_size {lot_size} incompatible.")
        return position//lot_size
    else:
        return position

def get_hk_min_price(current_price: float) -> float:
    """
        给定当前某只港股的价格，返回其最小价格变动单位
        e.g. 若 0.5 < price <= 10，则返回0.01
    """
    HK_MINPRICE_MAPPING = collections.OrderedDict([  # 保证for循环时有序取出
        (0.25, 0.001),
        (0.5, 0.005),
        (10, 0.01),
        (20, 0.02),
        (100, 0.05),
        (200, 0.1),
        (500, 0.2),
        (1000, 0.5),
        (2000, 1),
        (5000, 2),
        (9995, 5)
    ])
    for k, v in HK_MINPRICE_MAPPING.items():
        if current_price <= k:
            return v

def get_min_price_series(iuids: list) -> pd.Series:
    """
        返回：最小价格变动单位的series，其index为iuid

        输入的iuids列表支持*混合不同地区*的不同品种。

        对于中国期货，使用choice获取该数据。
        对于股票，CN/US的为0.01；而港股的最小价格变动单位是会根据当前的价格变的
        (参考 https://help.futu5.com/faq/topic1683 )，所以需要获取当前价格后，才能知道最小价格变动单位。
    """
    min_prices_dict = {}
    cn_future_iuids = []
    hk_iuids = []

    for iuid in iuids:
        _region, _type, _code = iuid.split('_')
        if _type == '60':  # 期货
            if _region == 'CN':
                cn_future_iuids.append(iuid)
            else:
                raise RuntimeError('unknown region to get min price: ' + iuid)
        elif _type == '10':  # 股票
            if _region in ('CN', 'HC', 'US'):  # 可以直接确定min price
                min_prices_dict[iuid] = 0.01
            elif _region == 'HK':  # 保存到列表中 稍后一次过通过接口获取这个信息
                hk_iuids.append(iuid)
            else:
                raise RuntimeError('unknown region: ' + iuid)
        else:
            raise RuntimeError('unknown type: ' + iuid)

    # 获取中国期货的min price
    if cn_future_iuids:
        cn_future_choicesymbols = list(map(symbol_converter.from_iuid_to_choicesymbol, cn_future_iuids))
        minprice_df = choice_client.css(','.join(cn_future_choicesymbols), 'FTMINPRICECHG')

        for choicesymbol in cn_future_choicesymbols:
            min_price_str = minprice_df[choicesymbol]['FTMINPRICECHG'][0]
            min_price_float = float(re.match('[0-9.]+', min_price_str).group())

            _iuid = symbol_converter.from_choicesymbol_to_iuid(choicesymbol)
            min_prices_dict[_iuid] = min_price_float

    # 获取港股的min price 需要先获取当前价格
    if hk_iuids:
        from ..market.market import market  # 不在文件头部导入 否则循环import报错
        hk_factsetsymbols = list(map(symbol_converter.from_iuid_to_factsetsymbol, hk_iuids))
        current_prices = market.get_real_time_data_from_factset(hk_factsetsymbols)['PRICE']
        for factsetsymbol in hk_factsetsymbols:
            _iuid = symbol_converter.from_factsetsymbol_to_iuid(factsetsymbol)
            _price = current_prices[_iuid]
            min_prices_dict[_iuid] = get_hk_min_price(_price)

    return pd.Series(min_prices_dict)

def get_lot_size_series(iuids: list, rq_client=None) -> pd.Series:
    """
        返回：lot size的series，其index为iuid

        输入的iuids列表支持*混合不同地区*的不同品种。

        对于中国期货，使用choice获取multiple数据，与lot size有一点不同（交易时发送的是手数而不是股数）；
        对于A股，返回100；对于美股，返回1；
        而对于港股，需要从RQ client获取lot size
    """
    lot_sizes_dict = {}
    cn_future_iuids = []
    hk_iuids = []

    for iuid in iuids:
        _region, _type, _code = iuid.split('_')
        if _type == '60':  # 期货
            if _region == 'CN':
                cn_future_iuids.append(iuid)
            elif _region == 'SG':
                lot_sizes_dict[iuid] = 1
            elif _region == 'HK':
                lot_sizes_dict[iuid] = 1
            else:
                raise RuntimeError('unknown region to get lot size: ' + iuid)
        elif _type == '10':  # 股票
            if _region in ('CN', 'HC'):
                lot_sizes_dict[iuid] = 100
            elif _region == 'US':
                lot_sizes_dict[iuid] = 1
            elif _region == 'GB':
                lot_sizes_dict[iuid] = 1
            elif _region == 'HK':  # 保存到列表中 稍后一次过通过接口获取这个信息
                if rq_client:
                    lot_sizes_dict[iuid] = rq_client.meta_data[iuid]['lot_size']
                else:
                    raise RuntimeError('shou use RQ client to get lot size')
            else:
                raise RuntimeError('unknown region: ' + iuid)
        else:
            raise RuntimeError('unknown type: ' + iuid)

    # 获取中国期货的multiple
    if cn_future_iuids:
        cn_future_choicesymbols = list(map(symbol_converter.from_iuid_to_choicesymbol, cn_future_iuids))
        multiple_df = choice_client.css(','.join(cn_future_choicesymbols), 'CONTRACTMUL')

        for choicesymbol in cn_future_choicesymbols:
            _multiple = int(multiple_df[choicesymbol]['CONTRACTMUL'][0])
            _iuid = symbol_converter.from_choicesymbol_to_iuid(choicesymbol)
            lot_sizes_dict[_iuid] = _multiple

    # 获取港股的lot size
    # if hk_iuids:
    #     hk_choicesymbols = list(map(symbol_converter.from_iuid_to_choicesymbol, hk_iuids))
    #     lot_size_df = choice_client.css(','.join(hk_choicesymbols), 'LOTSIZE')

    #     for choicesymbol in hk_choicesymbols:
    #         _iuid = symbol_converter.from_choicesymbol_to_iuid(choicesymbol)
    #         lot_sizes_dict[_iuid] = lot_size_df[choicesymbol]['LOTSIZE'][0]

    for k, v in lot_sizes_dict.items():
        if v is None:
            raise ValueError(f"{k} has no lot size data")
    return pd.Series(lot_sizes_dict)

def signals_to_orders(signal: pd.Series, buyprice_data, sellprice_data, source: str):
    """
        由差值series生成具体的order列表，逻辑是按传入的卖一价买入，按买一价卖出

        signal: 待买入/卖出的数量的series
    """
    order = []
    order_sell = []
    order_buy = []
    for idx in signal.index:
        position = signal.loc[idx].iloc[0]  # 从长度为1的series中取值 position应为一个浮点数
        if position == 0:
            continue

        if position > 0:
            direction = "BUY"
            price = sellprice_data[idx]  # 尝试以卖一价买入
        else:
            direction = "SELL"
            price = buyprice_data[idx]  # 尝试以买一价卖出

        dic = {
            "symbol": idx,  # iuid
            'source': source,
            "side": direction,
            "type": "LIMIT",
            "price": round(price, 3) if price < 1000 else round(price, 2),  # TODO 最终应在choice端修复
            "quantity": abs(int(position))  # 原来的position可能是numpy.int64类型 导致不能json化
        }
        if dic["side"] == "SELL":
            order_sell.append(dic)
        else:
            order_buy.append(dic)
        order.append(dic)
    return order, order_sell, order_buy

def process_ctp_arbitrage_contract(holdings: list) -> list:
    """
        处理套利合约的情况，例如 http://www.dce.com.cn/dalianshangpin/yw/fw/ywcs/jycs/tlhymx/index.html

        套利合约会导致获取的iuid变为如"CN_60_SPC a1909&m1909"，实际上服务器返回的值包括组合后的和单独的，
        所以可以直接忽略套利合约。
    """
    return list(filter(lambda holding: ' ' not in holding['iuid'], holdings))  # 以是否有空格作为判断标准

def get_uuid_by_thread():
    thread_id = str(os.getpid()) + str(threading.get_ident())
    return uuid.uuid1(thread_id, int(time.time()*1e6)).hex
