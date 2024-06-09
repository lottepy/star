# coding=utf-8

"""Common executor for algo-trading"""

import logging
import os
import re
import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd

from ..data.RQClient import RQClient
from ..data.socket_client import SocketClient
from ..execution.log.get_logger import get_logger
from ..execution.market.market import market
from ..execution.symbol.base import symbol_converter
from ..execution.trade.trading import trading
from ..execution.trade.trading_controller import TradingController

logger = get_logger(__name__)

__author__="Joseph Chen"
__email__="joseph.chen@magnumwm.com"


CHOICE_RATE_LIMIT = 3.1


class TradingExecutor(object):
    region_currency_map = {
        "HK": "HKD",
        "US": "USD",
        "CN": "CNY",
        "HC": "CNH",
        'SG': 'USD',
        'GB': 'USD'
    }

    def __init__(self, paper: dict, region: str, configs, target_holding_iuids=[], endpoint='http://test.aqumon.com:5565/api/v3/',
                 activate_futu=False, activate_factset=False, activate_choice=False, activate_rq_stream=False,
                 activate_intrinio=False, activate_kuanrui=False, disable_rq_data_and_callback=False, is_subaccount=False,
                 order_callback=None,  # order状态变更
                 tick_callback=None,
                 orderbook_callback=None,
                 metadata_callback=None):
        """
            初始化，包括实例化TradingController、获取当前holdings、转换IUID为对应客户端的symbols
        """
        self.paper = paper
        self.region = region
        self.currency = self.region_currency_map[self.region]
        self.endpoint = endpoint
        self.trading_ctrl = TradingController(self.endpoint)
        if not is_subaccount:  # 如果是子账户 同步持仓的代价太高 就不强行同步了
            self.trading_ctrl.sync_holdings(paper['broker'], paper['account'])  # 查初始持仓前先同步
        self.init_holdings = trading.get_current_positions(self.trading_ctrl, self.paper, self.region, is_subaccount=is_subaccount)
        if not self.init_holdings.empty:
            self.init_iuids = list(self.init_holdings['iuid'].values)   # iuids we are currently holding
        else:
            self.init_iuids = []
        self.iuids = list(set(target_holding_iuids + self.init_iuids))  # 两者去重后的交集
        if not self.iuids:
            raise ValueError('target iuid list cannot be empty!')

        activated_symbols = []

        if activate_kuanrui:
            endpoint_params = {}
            endpoint_params['HOST'] = configs.HOST
            endpoint_params['PORT_OES'] = configs.PORT_OES
            endpoint_params['PORT_MDS'] = configs.PORT_MDS

            disable_rq_data_and_callback = True  # kuanrui在封闭环境 必须把rq关掉
            self.socket_client = SocketClient(broker_id=self.paper['broker'], account_id=self.paper['account'],
                                              iuid_list=self.iuids,
                                              order_callback=order_callback,
                                              tick_callback=tick_callback,
                                              **endpoint_params)
            activated_symbols.append("iuid")  # 启用的symbol类型

        if disable_rq_data_and_callback:  # 不需要rq client order状态的回调
            pass
        elif activate_rq_stream:  # order状态的回调与实时行情都使用rq client
            # queue实时推流
            self.rq_client = RQClient(broker_id=self.paper['broker'], account_id=self.paper['account'],
                                      iuid_list=self.iuids,
                                      order_callback=order_callback,
                                      tick_callback=tick_callback,
                                      orderbook_callback=orderbook_callback,
                                      metadata_callback=metadata_callback)
            activated_symbols.append("iuid")  # 启用的symbol类型
            t = 120
            while not self.rq_client.is_data_complete('metadata'):
                logger.debug('wait for meta data from RQ client..')
                time.sleep(1)
                t -= 1
                if t <= 0:
                    raise ConnectionError(f'cannot get metadata of {set(self.iuids) - self.rq_client.meta_data.keys()} after timeout')
        else:  # order状态的回调使用rq client 实时行情需手动经不同数据源获取
            self.rq_client = RQClient(broker_id=self.paper['broker'], account_id=self.paper['account'], order_callback=order_callback)

        if activate_factset:
            self.factset_symbols = list(map(symbol_converter.from_iuid_to_factsetsymbol, self.iuids))
            activated_symbols.append("factset")
        if activate_choice:
            self.choice_symbols = list(map(symbol_converter.from_iuid_to_choicesymbol, self.iuids))
            activated_symbols.append("choice")
        if activate_futu:
            self.futu_symbols = list(map(symbol_converter.from_iuid_to_futusymbol, self.iuids))
            self.lot_size = self.lot_size_from_futu(iuids=self.iuids)
            activated_symbols.append("futu")
        if activate_intrinio:
            self.intrinio_symbols = list(map(symbol_converter.from_iuid_to_intriniosymbol, self.iuids))
            activated_symbols.append("intrinio")
        market.activated_symbol_types = activated_symbols  # 把用到的channel传给market实例 之后market就可以用get_real_time_data()方法

    # def wait_until_msq_subscribed(self, timeout=180):
    #     tic = datetime.now()
    #     while True:
    #         subscribed = [iuid.replace(".","_") for iuid in self.msq_client.orderbook_data.keys()]
    #         subscribing = list(filter(lambda x: x not in subscribed, self.iuids))
    #         toc = datetime.now()
    #         past_time = (toc - tic).seconds
    #         logger.info("\n[{}] - [{}] ".format(tic.strftime("%Y-%m-%d %H:%M:%S"), toc.strftime("%Y-%m-%d %H:%M:%S")) +
    #               "({} seconds past): subscribing iuids in msq...\n".format(past_time) +
    #               "\tiuids: {}\n".format(self.iuids) +
    #               "\tsubscribed iuids: {}\n".format(subscribed) +
    #               "\tsubscribing iuids: {}.".format(subscribing ))
    #         if not subscribing:
    #             logger.info("Successfully subscribed all iuids in msq.")
    #             break
    #         if past_time>timeout:
    #             moretime = input("Fail to subscribe {} in msq within {} seconds. Try more time? " +
    #                              "(input <int> seconds, OR Y/y, otherwise program will be terminated).\n".format(subscribing, timeout))
    #             if moretime in ('Y','y'):
    #                 moretime = 180
    #             elif isinstance(moretime, int) or isinstance(moretime, float):
    #                 moretime = int(moretime)
    #             else:
    #                 raise ConnectionError("Program terminated. Fail to subscribe {} in msq.".format(subscribing))
    #             timeout = moretime
    #             tic = datetime.now()
    #         time.sleep(3)

    # def futu_subscription(self, iuids=None):
    #     # futu_symbols = ['HK.00700', 'HK.00005', 'HK.02823']
    #     if iuids is None:
    #         iuids = self.iuids
    #     futu_symbols = [symbol_converter.from_iuid_to_futusymbol(iuid) for iuid in iuids]
    #     # quote_ctx.subscribe(futu_symbols, [SubType.QUOTE,SubType.ORDER_BOOK])
    #     quote_ctx.subscribe(futu_symbols, [SubType.ORDER_BOOK])
    #     return futu_symbols

    # def lot_size_from_futu(self, iuids):
    #     code, subscription = quote_ctx.query_subscription()
    #     assert code == 0, "Fail to query subscription in Futu.\n{}".format(subscription)
    #     if subscription.get("remain") <= 0:
    #         raise ValueError("There is not sufficient subscription quota in Futu!\n{}".format(subscription))
    #     # # Unsubscribe all
    #     # for k, v in subscription.get('sub_list').items():
    #     # 	code, err_msg = quote_ctx.unsubscribe(code_list=v, subtype_list=[k])
    #     # 	assert code == 0, "Fail to unsubscribe in Futu!\n{}".format(err_msg)
    #     # Subscribe
    #     futu_symbols = self.futu_subscription(iuids=iuids)
    #     mkt_snapshot = self.get_market_snapshot_from_futu(futusymbols=futu_symbols)
    #     lot_size = list(mkt_snapshot['lot_size'].values)
    #     return lot_size

    # def get_market_snapshot_from_futu(self, futusymbols=None):
    #     if futusymbols is None:
    #         futusymbols = self.futu_symbols
    #     code, mkt_snapshot = quote_ctx.get_market_snapshot(futusymbols)
    #     if code!=0:
    #         raise ValueError("Futu market snapshot is not available!")
    #     return mkt_snapshot

    # def get_real_time_data_from_futu(self, futusymbols=None):
    #     if futusymbols is None:
    #         futusymbols = self.futu_symbols
    #     data = pd.DataFrame(columns=['iuid', 'symbol', 'ASK_1', 'ASK_VOL_1', 'BID_1', 'BID_VOL_1', 'PRICE'])
    #     for n,futusymbol in enumerate(futusymbols):
    #         iuid = self.iuids[self.futu_symbols.index(futusymbol)]
    #         lot = self.lot_size[self.futu_symbols.index(futusymbol)]
    #         code, futudata = quote_ctx.get_order_book(futusymbol)
    #         assert code==0, "Fail to get order book from futu. Symbol={}".format(futusymbol)
    #         assert isinstance(futudata.get('Bid'),list), "Bid information of symbol {} not available in Futu".format(futusymbol)
    #         assert len(futudata.get('Bid'))>=1, "Bid information of symbol {} not available in Futu".format(futusymbol)
    #         bid_1, bid_vol_1, _ = futudata.get('Bid')[0]
    #         assert isinstance(futudata.get('Ask'),list), "Ask information of symbol {} not available in Futu".format(futusymbol)
    #         assert len(futudata.get('Ask')) >= 1, "Ask information of symbol {} not available in Futu".format(futusymbol)
    #         ask_1, ask_vol_1, _ = futudata.get('Ask')[0]
    #         data.loc[n, 'iuid'] = iuid
    #         data.loc[n, 'symbol'] = futusymbol
    #         data.loc[n, 'ASK_1'] = ask_1
    #         data.loc[n, 'ASK_VOL_1'] = ask_vol_1
    #         data.loc[n, 'BID_1'] = bid_1
    #         data.loc[n, 'BID_VOL_1'] = bid_vol_1
    #         data.loc[n, 'PRICE'] = (bid_1 + ask_1)/2.
    #         data.loc[n, 'ORDER_LOT_SIZE'] = lot
    #     return data

    # def get_real_time_data_from_msq(self, msqsymbols=None):
    #     if msqsymbols is None:
    #         msqsymbols = self.msq_symbols
    #     data = pd.DataFrame(self.msq_client.orderbook_data).T
    #     data = data[['a1','av1','b1','bv1']]
    #     data = data.reset_index()
    #     data.columns = ['iuid', 'ASK_1', 'ASK_VOL_1', 'BID_1', 'BID_VOL_1']
    #     data.loc[:, 'PRICE'] = (data['BID_1'] + data['ASK_1']) / 2.
    #     data.loc[:, 'ORDER_LOT_SIZE'] = self.lot_size
    #     data = data.where((pd.notnull(data)), None)
    #     return data

    # def get_real_time_data(self):
    #     for n, real_time_data_channel in enumerate(self.real_time_channels):
    #         data = real_time_data_channel()
    #         data = data[['iuid', 'ASK_1', 'ASK_VOL_1', 'BID_1', 'BID_VOL_1', 'PRICE', 'ORDER_LOT_SIZE']]
    #         # Simple checking
    #         if data['ASK_1'].apply(lambda x:x is None).any() or data['BID_1'].apply(lambda x:x is None).any():
    #             logger.info("Bid1 or Ask1 data from {} is None. Switch to next channel.".format(real_time_data_channel.__name__))
    #             data = None
    #             continue
    #         elif data['ASK_1'].apply(lambda x:x<=0).any() or data['BID_1'].apply(lambda x:x<=0).any():
    #             logger.info("Bid1 or Ask1 data from {} is zero or negative. Switch to next channel.".format(real_time_data_channel.__name__))
    #             data = None
    #             continue
    #         else:
    #             break
    #     if data is None:
    #         raise ValueError("None of the channels can provide meaningful real time data.")
    #     return data

    # def get_target_weights(self, target_csv:str)->pd.DataFrame:
    #     df = pd.read_csv(target_csv)
    #     if 'iuid' not in df.columns:
    #         df.loc[:, 'iuid'] = df.symbol.apply(symbol_converter.from_symbol_to_iuid)
    #     if 'symbol' in df.columns:
    #         df['symbol'] = df['symbol'].astype(str)
    #     return df

    # def _estimate_position(self, estimation:pd.Series, real_time_data:pd.DataFrame)->float:
    #     rt_data = real_time_data[real_time_data.iuid==estimation.iuid]
    #     price = rt_data['PRICE'].values[0]
    #     lot_size = rt_data['ORDER_LOT_SIZE'].values[0]
    #     est_position = np.floor(estimation.target_value / price / lot_size) * lot_size
    #     return est_position

    # def _estimate_value(self, estimation:pd.Series, real_time_data:pd.DataFrame)->float:
    #     rt_data = real_time_data[real_time_data.iuid==estimation.iuid]
    #     price = rt_data['PRICE'].values[0]
    #     est_pos = estimation.est_position
    #     est_val = est_pos * price
    #     return est_val

    # def _join_current_positions_to_target_positions(self, current_positions:pd.DataFrame, target_positions:pd.DataFrame):
    #     target_positions['current_position'] = 0.0
    #     for n, row in current_positions.iterrows():
    #         iuid = row['iuid']
    #         symbol = symbol_converter.from_iuid_to_symbol(iuid)
    #         cur_pos = row['holdingPosition']
    #         if iuid in target_positions.iuid.values:
    #             idx = target_positions[target_positions.iuid==iuid].index[0]
    #             target_positions.loc[idx, 'current_position'] = cur_pos
    #         else:
    #             add_data = {'iuid':iuid, 'symbol':symbol, 'current_position':cur_pos, 'target_position':0.0}
    #             target_positions = target_positions.append(add_data, ignore_index=True)

    #     return target_positions

    # def try_to_fill_positions_with_cash(self, cash_remaining:float, target_positions:pd.DataFrame,
    #                                     real_time_data:pd.DataFrame):

    #     for n, row in target_positions.iterrows():
    #         target_value = row['target_value']
    #         est_value = row['est_value']

    #         iuid = row['iuid']
    #         idx = target_positions[target_positions['iuid'] == iuid].index[0]
    #         lot_size = real_time_data[real_time_data.iuid==iuid].ORDER_LOT_SIZE.values[0]
    #         price = real_time_data[real_time_data.iuid==iuid].PRICE.values[0]

    #         # est_number_of_lot is always 1.0?
    #         est_number_of_lot = np.ceil((target_value - est_value) / (price * lot_size))
    #         assert est_number_of_lot==1.0, "Estimated lot shoule be 1.0, got {} instead".format(est_number_of_lot)
    #         est_add_pos = est_number_of_lot * lot_size
    #         est_add_value = price * est_add_pos

    #         # initialize adj position and adj value
    #         target_positions.loc[idx, 'adj_position'] = target_positions.loc[idx, 'est_position']
    #         target_positions.loc[idx, 'adj_value'] = target_positions.loc[idx, 'est_value']
    #         # If we have enough money, we will check whether we should adjust based on new drift
    #         if cash_remaining>=est_add_value:
    #             adj_position = target_positions.loc[idx, 'est_position'] + est_add_pos
    #             adj_value = target_positions.loc[idx, 'est_value'] + est_add_value
    #             target_value = target_positions.loc[idx, 'target_value']
    #             current_drift = target_positions.loc[idx, 'est_drift']
    #             new_drift = (adj_value/target_value-1.)**2
    #             # If adjusting can reduce drift, we adjust
    #             if new_drift<current_drift:
    #                 target_positions.loc[idx, 'adj_position'] = adj_position
    #                 target_positions.loc[idx, 'adj_value'] = adj_value
    #                 cash_remaining -= est_add_value

    #     target_positions.loc[:, 'adj_drift'] = (target_positions.loc[:, 'adj_value'] / target_positions.loc[:,'target_value'] - 1.) ** 2
    #     target_positions['target_position'] = target_positions['adj_position']

    #     return cash_remaining, target_positions

    # def calculate_target_positions(self, est_commission_rate=0.0, maximum_capital=100000., minimum_capital=70000.,
    #                                minimum_cash_buffer=50., percentage_of_cash_buffer=0.005):
    #     target_weights = self.target_weight
    #     current_positions = self.get_current_positions()
    #     if not current_positions.empty:
    #         real_time_data = self.get_real_time_data()
    #         current_positions = pd.merge(current_positions, real_time_data, on='iuid', how='outer')
    #         current_positions['holdingPosition'] = current_positions['holdingPosition'].fillna(0.0)
    #     else:
    #         real_time_data = self.get_real_time_data()
    #         current_positions = real_time_data.copy()
    #         current_positions['holdingPosition'] = 0.0

    #     # cash info
    #     current_cash = self.get_current_cash()
    #     available_cash = current_cash['purchasePower'].values[0] # 'purchasePower'
    #     position_value = 0.0
    #     if not current_positions.empty:
    #         # holdingAmount is incorrect, use holdingPosition and prices instead
    #         current_positions['actualHoldingAmount'] = (
    #                 current_positions['holdingPosition'] * (current_positions['ASK_1'] + current_positions['BID_1']) / 2.
    #         )
    #         position_value = current_positions['actualHoldingAmount'].sum()

    #     # We don't want to borrow money
    #     total_value = available_cash + position_value
    #     # If current asset is less than minimum_capital(70000), we use margin account to initialize capital to maximum_capital(100000.)
    #     if total_value<minimum_capital:
    #         confirm = input("Total value {} is less than minimum required {}. Use margin account to reset ".format(total_value, maximum_capital) +
    #                         "the total value to maximum capital {} (Note: if not agree, will terminate the program)\n".format(maximum_capital) +
    #                         "1. Confirm to continue?(Y/N)\n" +
    #                         "2. No, I want to use only {} to proceed. (N/n)\n".format(total_value) +
    #                         "3. I don't want to proceed. (Hit other keys)\n"
    #                         )
    #         if confirm in ('Y','y'):
    #             total_value = maximum_capital
    #         elif confirm in ('N', 'n'):
    #             pass
    #         else:
    #             raise ValueError("Program is terminated.")

    #     elif total_value>maximum_capital:
    #         confirm = input("Total value {}={}(cash)+{}(market value of holdings) ".format(total_value, available_cash, position_value) +
    #                         "is more than maximum required {}. We simply reset ".format(maximum_capital) +
    #                         "the total value to {}\n".format(maximum_capital) +
    #                         "1. Confirm to continue (Y/y)\n" +
    #                         "2. No, I want to rebalance with all {} (N/n)\n".format(total_value) +
    #                         "3. I don't want to proceed. (Hit other keys)\n"
    #                         )
    #         if confirm in ('Y', 'y'):
    #             total_value = maximum_capital
    #         elif confirm in ('N', 'n'):
    #             pass
    #         else:
    #             raise ValueError("Program is terminated.")

    #     # If we consider commission, est_commission_rate should be larger than 0
    #     total_value = total_value * (1 - est_commission_rate)
    #     cash_buffer = max(minimum_cash_buffer, total_value * percentage_of_cash_buffer)

    #     assert total_value>0, "There is not sufficient money to trade!"

    #     target_positions = target_weights.copy()
    #     target_positions.loc[:, 'target_value'] = target_positions.loc[:, 'weight'] * total_value
    #     target_positions.loc[:, 'est_position'] = target_positions[['iuid','target_value']].apply(
    #         lambda x: self._estimate_position(estimation=x, real_time_data=real_time_data),
    #         axis=1,
    #     )
    #     target_positions.loc[:, 'est_value'] = target_positions[['iuid','est_position']].apply(
    #         lambda x: self._estimate_value(estimation=x, real_time_data=real_time_data),
    #         axis=1,
    #     )
    #     target_positions.loc[:, 'est_drift'] = (target_positions.loc[:, 'est_value']  / target_positions.loc[:, 'target_value']-1.)**2

    #     # rank from largest drift to smallest drift
    #     target_positions = target_positions.sort_values(by='est_drift',ascending=False)

    #     # Remaining cash
    #     cash_remaining = total_value - cash_buffer - target_positions['est_value'].sum()

    #     # try to fill positions
    #     cash_remaining, target_positions = self.try_to_fill_positions_with_cash(cash_remaining, target_positions, real_time_data)

    #     target_positions = self._join_current_positions_to_target_positions(current_positions, target_positions)

    #     target_positions = pd.merge(target_positions, real_time_data, how='left', on='iuid')

    #     target_positions.loc[:, 'delta_position'] = (
    #             np.round((target_positions.loc[:, 'target_position'] - target_positions.loc[:, 'current_position'])
    #             / target_positions.loc[:, 'ORDER_LOT_SIZE'])
    #             * target_positions.loc[:, 'ORDER_LOT_SIZE']
    #     )

    #     target_positions.loc[:, 'order_size'] = (target_positions.loc[:, 'delta_position'] *
    #                                              target_positions.loc[:, 'PRICE'])
    #     return target_positions

    # def create_order_basket(self, target_positions:pd.DataFrame, order_type='LIMIT'):  # 只保留ctp的对应实现
    #     target_positions_sell = target_positions[target_positions.order_size < 0]
    #     target_positions_buy = target_positions[target_positions.order_size > 0]

    #     orders_sell = []
    #     if not target_positions_sell.empty:
    #         # create sell order basket
    #         # re-order selling
    #         target_positions_sell = target_positions_sell.sort_values(by='order_size', ascending=True)
    #         # order_type = 'LIMIT'
    #         order_side = 'SELL'
    #         for n,row in target_positions_sell.iterrows():
    #             order_price = row['BID_CLOSE'] if row['BID_1']==0.0 else row['BID_1']
    #             # round down order price to 0.1 to avoid rejection
    #             order_price = np.floor(order_price*10.)/10.
    #             order_symbol = row['iuid']
    #             order_qty = -row['delta_position']
    #             orders_sell.append(
    #                 dict(
    #                     type=order_type,
    #                     side=order_side,
    #                     symbol=order_symbol,
    #                     quantity=order_qty,
    #                     price=order_price
    #                 )
    #             )

    #     orders_buy = []
    #     if not target_positions_buy.empty:
    #         # create buy order basket
    #         # re-order buying
    #         target_positions_buy = target_positions_buy.sort_values(by='order_size', ascending=False)
    #         # order_type = 'LIMIT'
    #         order_side = 'BUY'
    #         for n,row in target_positions_buy.iterrows():
    #             order_price = row['ASK_CLOSE'] if row['ASK_1']==0.0 else row['ASK_1']
    #             # round up order price to 0.1 to avoid rejection
    #             order_price = np.ceil(order_price * 10.) / 10.
    #             order_symbol = row['iuid']
    #             order_qty = row['delta_position']
    #             orders_buy.append(
    #                 dict(
    #                     type=order_type,
    #                     side=order_side,
    #                     symbol=order_symbol,
    #                     quantity=order_qty,
    #                     price=order_price
    #                 )
    #             )

    #     return orders_sell, orders_buy

    # def wait_and_check_uncancelled_orders(self, orderids:list):  # 没有用到过
    #     global order_status
    #     # wait until all the submitted orders have callbacks
    #     while len(order_status) != len(orderids):
    #         logger.info('len(order_status)={}, but len(orderids)={}. Waiting for order_status to update `Cancelled`...'.format(
    #             len(order_status), len(orderids)
    #         ))
    #         time.sleep(2.)
    #     submitted_orderids = [o.get('id') for o in orderids]
    #     uncancelled_orderids = []
    #     cancelled_orderids = []
    #     for n, o in enumerate(submitted_orderids):
    #         if o in order_status:
    #             if order_status.get(o).get('status') != 'CANCELLED':
    #                 uncancelled_orderids.append(order_status.get(o))
    #             else:
    #                 cancelled_orderids.append(order_status.get(o))
    #         else: # This should never execute.
    #             # uncancelled_orderids.append(orderids[n])
    #             raise ValueError("Fail in cancelling order.\norder {} not in order_status:\n{}".format(o, order_status))
    #     return uncancelled_orderids, cancelled_orderids

    # def place_order(self, order=None):
    #     cash = pd.DataFrame(self.get_current_cash())
    #     positions = self.get_current_positions()
    #     # cash.to_csv("data/before_order_cash.csv")
    #     # positions.to_csv("data/before_order_positions.csv")
    #     if order is None:
    #         order = dict(
    #             type='MARKET',  # Market
    #             side='BUY',
    #             symbol='GB_40_USDHKD',
    #             quantity=100,
    #         )
    #     order_info = self.trading_ctrl.submit_order(baccount=self.paper.get('account'),
    #                                               brokerid=self.paper.get('broker'),
    #                                                 subaccount=self.paper.get('subaccount'),
    #                                               order=order)
    #     logger.info(order_info)
    #     orderid = order_info.get('data').get('id')

    #     # wait until the order is filled:
    #     global order_status

    #     logger.info("Wait for order_status to be activated in msq...")
    #     while True:
    #         try:
    #             os = order_status.get(orderid).get('status')
    #             break
    #         except NameError:
    #             time.sleep(1)

    #     while order_status.get(orderid).get('status') != 'FILLED':
    #         time.sleep(2.5)
    #         logger.info("Waiting for order {} to be filled...".format(orderid))

    #     # cash = pd.DataFrame(trading_exe.get_current_cash())
    #     # positions = trading_exe.get_current_positions()
    #     # cash.to_csv("data/after_order_cash.csv")
    #     # positions.to_csv("data/after_order_positions.csv")
    #     print("Successfully execute the order!")
    #     return order_info


# class TradingExecutorHuaTai(TradingExecutor):

# 	def __init__(self, *args, **kwargs):
# 		super().__init__(*args, **kwargs)

# 	def update_orders_status(self, orders):
# 		global order_status
# 		order_status = {}
# 		for order in orders:
# 			orderid = order.get('id')
# 			result = self.trading_ctrl.get_order_status(orderid).get('data')
# 			order_status[orderid] = result
# 		return

# 	def wait_and_check_unfilled_orders(self, orderids: list):
# 		global order_status
# 		# update order status by API
# 		self.update_orders_status(orderids)
# 		# wait until all the submitted orders have callbacks
# 		while len(order_status) != len(orderids):
# 			logger.info('len(order_status)={}, but len(orderids)={}. Waiting for order_status to update `Filled`...'.format(
# 				len(order_status), len(orderids)
# 			))
# 			time.sleep(2.)

# 		submitted_orderids = [o.get('id') for o in orderids]
# 		unfilled_orderids = []
# 		filled_orderids = []
# 		partial_filled_orderids = []
# 		for n, o in enumerate(submitted_orderids):
# 			if o not in order_status:
# 				raise ValueError("Fail in filling orders.\norder {} not in order_status:\n{}".format(o, order_status))
# 			if order_status.get(o).get('status') != 'FILLED':
# 				unfilled_orderids.append(order_status.get(o))
# 				if order_status.get(o).get('status') == 'FILLING' and order_status.get(o).get('filledQuantity') > 0.0:
# 					partial_filled_orderids.append(order_status.get(o))
# 			else:
# 				filled_orderids.append(order_status.get(o))

# 		return unfilled_orderids, filled_orderids, partial_filled_orderids


class TradingExecutorCTP(TradingExecutor):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # def create_order_basket(self, target_positions:pd.DataFrame, order_type='LIMIT'):  # 保留这个？
    # 	target_positions_sell = target_positions[target_positions.order_size < 0]
    # 	target_positions_buy = target_positions[target_positions.order_size > 0]

    # 	orders_sell = []
    # 	if not target_positions_sell.empty:
    # 		# create sell order basket
    # 		# re-order selling
    # 		target_positions_sell = target_positions_sell.sort_values(by='order_size', ascending=True)
    # 		# order_type = 'LIMIT'
    # 		order_side = 'SELL'
    # 		for n,row in target_positions_sell.iterrows():
    # 			order_price = row['BID_CLOSE'] if row['BID_1']==0.0 else row['BID_1']
    # 			# round down order price to 0.1 to avoid rejection
    # 			# order_price = np.floor(order_price*10.)/10.
    # 			order_price = np.floor(order_price / row['FTMINPRICECHG']) * row['FTMINPRICECHG']
    # 			order_symbol = row['iuid']
    # 			order_qty = -row['delta_position']
    # 			orders_sell.append(
    # 				dict(
    # 					type=order_type,
    # 					side=order_side,
    # 					symbol=order_symbol,
    # 					quantity=order_qty,
    # 					price=order_price
    # 				)
    # 			)

    # 	orders_buy = []
    # 	if not target_positions_buy.empty:
    # 		# create buy order basket
    # 		# re-order buying
    # 		target_positions_buy = target_positions_buy.sort_values(by='order_size', ascending=False)
    # 		# order_type = 'LIMIT'
    # 		order_side = 'BUY'
    # 		for n,row in target_positions_buy.iterrows():
    # 			order_price = row['ASK_CLOSE'] if row['ASK_1']==0.0 else row['ASK_1']
    # 			# round up order price to 0.1 to avoid rejection
    # 			# order_price = np.ceil(order_price * 10.) / 10.
    # 			order_price = np.ceil(order_price / row['FTMINPRICECHG']) * row['FTMINPRICECHG']
    # 			order_symbol = row['iuid']
    # 			order_qty = row['delta_position']
    # 			orders_buy.append(
    # 				dict(
    # 					type=order_type,
    # 					side=order_side,
    # 					symbol=order_symbol,
    # 					quantity=order_qty,
    # 					price=order_price
    # 				)
    # 			)

    # 	return orders_sell, orders_buy


if __name__=="__main__":
    pass
    # for acc in ['a28e4a93-dc43-479b-807f-89642e53fce0']:#['50000119']: # ['DU1161135','DU1161136','DU1161137','DU1161138','DU1161139','DU1161140']:
    #     logger.info(acc)

    #     paper1 = {
    #         'broker': '2', # '102',
    #         'account': acc,
    #     }


    #     """ # CTP """


    #     target_weight_csv = 'data/target_weight_CTP.csv'
    #     region = 'CN'

    #     logger.info()

    #     trading_exe = TradingExecutorCTP(paper=paper1,
    #                                      target_weight_csv=target_weight_csv,
    #                                      region=region,
    #                                      activate_futu=False,      # HK/US/CN
    #                                      activate_factset=False,   # HK/US
    #                                      activate_choice=True,     # CN/CTP
    #                                      activate_msq=False,       # HK/US/CN
    #                                      channel_priority = ['choice'],
    #                                      disable_msq = False,
    #                                     )

    #     # Cancel all existing orders
    #     trading_exe.cancel_all_active_orders()

    #     # Save info before trading
    #     cash = trading_exe.get_current_cash()
    #     positions = trading_exe.get_current_positions()
    #     cash.to_csv("data/before_trading_cash.csv")
    #     positions.to_csv("data/before_trading_positions.csv")

    #     # Optional: liquidate your current positions
    #     if (not positions.empty):
    #         trading_exe.liquidate(current_positions=positions)

    #     # get price
    #     price = trading_exe.get_real_time_data()
    #     price_IC1902 = price[price['iuid']=='CN_60_IC1903']['ASK_1'].values[0]
    #     minchg_IC1902 = price[price['iuid']=='CN_60_IC1903']['FTMINPRICECHG'].values[0]
    #     order_price = np.ceil(price_IC1902 / minchg_IC1902) * minchg_IC1902

    #     # # test order
    #     # order = dict(
    #     # 	type='LIMIT',  # Market
    #     # 	side='BUY',
    #     # 	symbol='CN_60_IC1902',
    #     # 	quantity=1,
    #     # 	price=order_price,
    #     # )
    #     #
    #     # trading_exe.place_order(order=order)
    #     #
    #     # logger.info()

    #     # Rebalance
    #     target_positions = trading_exe.calculate_target_positions()
    #     target_positions.to_csv("data/target_positions.csv")
    #     orders_sell, orders_buy = trading_exe.create_order_basket(target_positions)
    #     # Note: If you want to terminate the execution manually, you can set breakpoint
    #     # in the while loop, and save 'sell_results' or 'buy_results' to local
    #     # drive. This will give you a record of filled orders and partial filled orders.
    #     sell_results = trading_exe.execute_sell(orders_sell)
    #     buy_results = trading_exe.execute_buy(orders_buy)

    #     # Save info after trading
    #     cash = trading_exe.get_current_cash()
    #     positions = trading_exe.get_current_positions()
    #     cash.to_csv("data/after_trading_cash.csv")
    #     positions.to_csv("data/after_trading_positions.csv")

    #     # Finished
    #     logger.info("All done.")
