# -*- coding: utf-8 -*-
# @Time    : 7/22/2019 11:50 AM
# @Author  : Joseph Chen
# @Email   : joseph.chen@magnumwm.com
# @FileName: market.py
# @Software: PyCharm

import random
import time

import pandas as pd

from ...data import intrinio_api
from ...data.choice_proxy_client import choice_client
from ...data.factset_client import get_data as get_factset_data
from ..log.get_logger import get_logger
from ..symbol.base import symbol_converter
from ..util.trading_utils import ensure_time_interval

logger = get_logger(__name__)


class Market:
    CHOICE_RATE_LIMIT = 3.1

    def __init__(self, channels=None, rq_client=None):
        self.real_time_channels = channels
        self.rq_client = rq_client

    def get_real_time_data_func(self, symbols: list):
        """ 应在运行时被覆盖后再调用，目标为做一个统一的、只有一个参数的，获取行情数据的函数 """
        raise NotImplementedError

    @staticmethod
    @ensure_time_interval(seconds=CHOICE_RATE_LIMIT)
    def get_real_time_data_from_choice(choicesymbols):
        """
            For China equities

            choicesymbols: str or list

            Returns a DataFrame, with IUID index,
            and columns=['BID_1', 'BID_VOL_1', 'ASK_1', 'ASK_VOL_1', 'PRICE']
        """
        # data = choice_client.csqsnapshot(symbols,"Time, Now, Tradestatus, BuyPrice1, BuyVolume1, SellPrice1, SellVolume1")  # 没有用到time, now

        # 以下操作后choicesymbols变为list
        if isinstance(choicesymbols, str):
            choicesymbols = choicesymbols.split(',')

        # choice_client发送的是拼接后的字符串
        t = 5
        while t:
            try:
                data_snapshot = choice_client.csqsnapshot(','.join(choicesymbols), 'BuyPrice1, BuyVolume1, SellPrice1, SellVolume1')  # (1 row, (4 x length) columns)
            except RuntimeError as e:
                t -= 1
                logger.warning('choice error: ' + str(e))
                time.sleep(random.randint(1,6))
                if not t:
                    raise e
            else:
                break

        data_dict = {}
        for symbol in choicesymbols:
            snapshot = data_snapshot[symbol].iloc[0]  # series (4,)
            assert snapshot['BUYPRICE1'] > 0, f'data for {symbol} is not normal, please check!'
            data_dict[symbol] = snapshot

        data = pd.concat(data_dict, axis=1).T  # (length rows, 4 columns)
        data.columns = ['BID_1', 'BID_VOL_1', 'ASK_1', 'ASK_VOL_1']  # 重命名各列
        data['PRICE'] = (data['BID_1'] + data['ASK_1'])/2.  # 新增一列price (length rows, 5 columns)
        data.rename(index=symbol_converter.from_choicesymbol_to_iuid, inplace=True)  # (length rows, 5 columns)
        data.index.name = 'iuid'
        return data

    @staticmethod
    def get_real_time_data_from_factset(factsetsymbols):
        """ Factset data for HK and US equities

            Returns a DataFrame, with IUID index,
            and columns=['ASK_1', 'ASK_VOL_1', 'ASK_CLOSE', 'BID_1', 'BID_VOL_1', 'BID_CLOSE', 'PRICE']
        """
        data = get_factset_data(factsetsymbols)  # 信息非常全 有100多列 下面只取用到的几列
        data = data.T
        data = data[['ASK_1','ASK_VOL_1', 'ASK_CLOSE','BID_1', 'BID_VOL_1', 'BID_CLOSE']].astype(float)
        # If market is not open, we use close price to approximate bid & ask prices instead
        if data.ASK_1.min()>0.0 and data.BID_1.min()>0.0:
            data['PRICE'] = (data['BID_1'] + data['ASK_1'])/2.
        else:
            logger.info("ALERT: Market might not be openning now!")
            data['PRICE'] = (data['BID_CLOSE'] + data['ASK_CLOSE']) / 2.
        data.rename(index=symbol_converter.from_factsetsymbol_to_iuid, inplace=True)
        return data

    @staticmethod
    def get_real_time_data_from_intrinio(intriniosymbols):
        """ Intrinio data for US equities

            Returns a DataFrame, with IUID index,
            and columns=['ASK_1', 'BID_1', 'ASK_VOL_1', 'BID_VOL_1', 'PRICE']
        """
        # if intriniosymbols is None:
        #     intriniosymbols = self.intriniosymbols
        data = intrinio_api.get_data(intriniosymbols)  # 信息有几十列 下面只取用到的几列
        data = data.T
        data = data[['ask_price_4d', 'ask_size', 'last_price_4d', 'prev_close_4d', 'bid_price_4d', 'bid_size']].astype(float)
        # If market is not open, we use close price to approximate bid & ask prices instead
        data[['ASK_1', 'BID_1']] = data[['ask_price_4d', 'bid_price_4d']] / 10000.  # 除以10000才是实际的价格
        data.rename(columns={'ask_size': 'ASK_VOL_1', 'bid_size': 'BID_VOL_1'},  inplace=True)
        if data.ASK_1.min()>0.0 and data.BID_1.min()>0.0:
            data['PRICE'] = data['last_price_4d'] / 10000.
        else:
            print("ALERT: Market might not be openning now!")
            data['PRICE'] = data['prev_close_4d'] / 10000.
        data.rename(index=symbol_converter.from_symbol_to_iuid, inplace=True)
        return data

    @staticmethod
    def get_real_time_data_from_futu(futusymbols=None):
        raise NotImplementedError

    @staticmethod
    def get_real_time_data_from_reuter(reutersymbols=None):
        raise NotImplementedError

    def get_real_time_data(self, iuids: list):
        """ 从self的各activated_symbol_types尝试拿实时数据 输入为*iuid*列表 """
        if self.rq_client:
            return self.get_real_time_data_from_rq_client(self.rq_client, iuids)

        for symbol_type in self.activated_symbol_types:  # market.activated_symbol_types在executor初始化时赋值
            _symbols = list(map(getattr(symbol_converter, f'from_iuid_to_{symbol_type}symbol'), iuids))
            data = self.get_real_time_data_func(_symbols)
            # data = data[['ASK_1', 'ASK_VOL_1', 'BID_1', 'BID_VOL_1', 'PRICE']]

            # Simple checking 我很怀疑这个check有没有效 如果传了奇怪的参数给各数据源 会抛出异常吧?
            if data['ASK_1'].apply(lambda x:x is None).any() or data['BID_1'].apply(lambda x:x is None).any():
                logger.info("Bid1 or Ask1 data from {} is None. Switch to next channel.".format(symbol_type))
                data = None
                continue
            elif data['ASK_1'].apply(lambda x:x<=0).any() or data['BID_1'].apply(lambda x:x<=0).any():
                logger.info("Bid1 or Ask1 data from {} is zero or negative. Switch to next channel.".format(symbol_type))
                data = None
                continue
            else:
                break
        if data is None:
            raise ValueError("None of the channels can provide meaningful real time data.")
        return data

    def get_real_time_data_from_rq_client(self, rq_client, iuids: list):
        """
            被动获取数据。注意不能在收市后使用，否则不会返回数据

            Returns a DataFrame, with IUID index,
            and columns=['ASK_1', 'BID_1', 'ASK_VOL_1', 'BID_VOL_1', 'PRICE']
        """
        while True:
            orderbook_data = rq_client.orderbook_data
            if set(orderbook_data.keys()) < set(iuids):  # 接收到的数据不全
                if not orderbook_data:
                    logger.warning('empty data received from rq client! retry..')
                else:
                    logger.debug(f'data received from rq client is not complete: '
                                 f'actual: {len(orderbook_data)}, expected: {len(iuids)}, will wait for more data '
                                 f'from {set(iuids) - orderbook_data.keys()}')
                time.sleep(1)  # 等待后端推数据
            else:
                break

        for iuid in orderbook_data:  # 专门处理涨跌停 例如'a1': None, 'av1': None
            if orderbook_data[iuid]['a1'] is None and orderbook_data[iuid]['b1'] is None:
                logger.warning(f'{iuid} 停牌')
            elif orderbook_data[iuid]['a1'] is None:  # a1为空 即涨停
                orderbook_data[iuid]['a1'] = orderbook_data[iuid]['b1']  # 补充卖1价 但不补充卖1量
                logger.warning(f'{iuid} 涨停')
            elif orderbook_data[iuid]['b1'] is None:  # b1为空 即跌停
                orderbook_data[iuid]['b1'] = orderbook_data[iuid]['a1']
                logger.warning(f'{iuid} 跌停')

        data = pd.DataFrame(orderbook_data).T  # dict转为df
        data.fillna(0, inplace=True)
        data = data[['a1', 'av1', 'b1', 'bv1']]  # A股可得5挡数据 只需要1挡
        data = data.loc[iuids]  # 由于是从orderbook_data中取 去掉可能在改单时因为unfilled是其中的一部分而获取到冗余的行
        data.rename(columns={'a1': 'ASK_1', 'av1': 'ASK_VOL_1', 'b1': 'BID_1', 'bv1': 'BID_VOL_1'}, inplace=True)  # column rename
        data['PRICE'] = ((data['BID_1'] + data['ASK_1'])/2).astype('float64')
        return data

    def get_real_time_data_from_socket_client(self, socket_client, iuids: list):
        """ 
            被动获取数据。注意不能在收市后使用，否则不会返回数据

            Returns a DataFrame, with IUID index,
            and columns=['ASK_1', 'BID_1', 'ASK_VOL_1', 'BID_VOL_1', 'PRICE']
        """
        t = 60
        while t:
            time.sleep(1)  # 等待后端推数据
            t -= 1
            tick_data = socket_client.tick_data
            if set(tick_data.keys()) < set(iuids):  # 接收到的数据不全
                if not tick_data:
                    logger.warning('empty orderbook data received from socket client! retry..')
                else:
                    logger.debug(f'data received from socket client is not complete: '
                                 f'actual: {len(tick_data)}, expected: {len(iuids)}, will wait for more data '
                                 f'from {set(iuids) - set(tick_data.keys())}')
            else:
                break

        if len(tick_data) < len(iuids):  # 最终接收到的数据不全 表示存在停牌股 手动设置其数据
            no_trade_iuids = set(iuids) - set(tick_data.keys())
            for iuid in no_trade_iuids:
                tick_data[iuid] = {'a1': 0.0, 'b1': 0.0, 'av1': 0, 'bv1': 0}
            logger.warning(f'{no_trade_iuids} 获取行情超时，当作已停牌处理')

        for iuid in tick_data:  # 提示涨跌停股
            if tick_data[iuid]['av1'] == 0.0 and tick_data[iuid]['bv1'] == 0.0:
                logger.warning(f'{iuid} 停牌')
            elif tick_data[iuid]['av1'] == 0.0:  # a1为空且b1非空 即涨停
                tick_data[iuid]['a1'] = tick_data[iuid]['b1']  # 补充卖1价 但不补充卖1量
                logger.warning(f'{iuid} 涨停')
            elif tick_data[iuid]['bv1'] == 0.0:  # b1为空且a1非空 即跌停
                tick_data[iuid]['b1'] = tick_data[iuid]['a1']
                logger.warning(f'{iuid} 跌停')

        data = pd.DataFrame(tick_data).T  # dict转为df
        data = data[['a1', 'av1', 'b1', 'bv1']]  # A股可得5挡数据 只需要1挡
        data = data.loc[iuids]  # 由于是从tick_data中取 去掉可能在改单时因为unfilled是其中的一部分而获取到冗余的行
        data.rename(columns={'a1': 'ASK_1', 'av1': 'ASK_VOL_1', 'b1': 'BID_1', 'bv1': 'BID_VOL_1'}, inplace=True)  # column rename
        data['PRICE'] = ((data['BID_1'] + data['ASK_1'])/2).astype('float64')
        return data

market = Market()  # 供外部调用
