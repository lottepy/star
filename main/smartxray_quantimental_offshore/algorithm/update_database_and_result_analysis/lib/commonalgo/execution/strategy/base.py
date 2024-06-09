#!/usr/bin/env python
import os
import time

import pandas as pd

from ..config import constant
from ..log.get_logger import logger
from ..log import get_logger
from ..market.market import market, Market
from ..symbol.base import symbol_converter, Symbols
from ..trade.trading import trading, trading_callback
from ..trade.trading_controller import TradingController
from ..order.manager import OrderManager

from ...data.RQClient import RQClient

class TradingExecutor(object):
    """
    Trading module.
    """
    region_currency_map = {
        "HK": "HKD",
        "US": "USD",
        "CN": "CNY",
        "HC": "CNH"
    }

    def __init__(self, paper: dict, region: str, target_holding_iuids=[], endpoint='http://test.aqumon.com:5565/api/v3/',
                 activate_futu=False, activate_factset=False,
                 activate_choice=False,activate_rq_data=False,
                 activate_intrinio=False, activate_kuanrui=False,
                 disable_rq_data_callback=False):
        """
            初始化，包括实例化TradingController、获取当前holdings、转换IUID为对应客户端的symbols
        """
        logger.debug(f"init {self.__class__.__name__}")
        self.model_save_dir = get_logger.current_file_abs_path
        self.load_model_status(self.model_save_dir)
        self.paper = paper
        self.region = region
        self.currency = constant.region_currency_map[self.region]
        self.endpoint = endpoint
        self.trading_ctrl = TradingController(self.endpoint)
        self.symbol_converter = Symbols(region)

        self.trading_ctrl.sync_holdings(paper['broker'], paper['account'])  # 查初始持仓前先同步
        logger.debug(f"Holdings synced")
        self.init_holdings = trading.get_current_positions(self.trading_ctrl,
                                                           self.paper,
                                                           self.region)
        self.init_holdings.set_index('iuid', inplace=True)

        if not self.init_holdings.empty:
            self.init_iuids = list(self.init_holdings.index.values)   # iuids we are currently holding
        else:
            self.init_iuids = []
        self.iuids = list(set(target_holding_iuids + self.init_iuids))  # 两者去重后的交集

        # symbols
        symbol_prefix = constant.symbol_prefix_mapping[region]  # 会影响到iuid与不同symbol互转时用到的函数
        self.symbols = [Symbols.get_symbol(i, symbol_prefix) for i in self.iuids]

        self.activate_rq_data = activate_rq_data

        self.market = self._get_market(activate_factset, activate_choice, activate_futu,
                                        activate_kuanrui,
                                        activate_intrinio)

        # trading_callback = TradingCallback(self.msg_queue)

        self.order_manager = OrderManager(self.market,
                                          self.trading_ctrl,
                                          self.paper,
                                          self.trading_directions,
                                          timeout=60)

        if not disable_rq_data_callback:
            self.__get_rq_client(activate_rq_data)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.clean_up()

    def __del__(self):
        self.clean_up()

    def _get_market(self, activate_factset,
                     activate_choice,
                     activate_futu,
                     activate_kuanrui,
                     activate_intrinio):

        self.activated_symbols = set()
        if activate_kuanrui:
            self.disable_rq_data_and_callback = True  # kuanrui在封闭环境 必须把rq关掉
            _market_iuids = list(map(symbol_converter.from_iuid_to_marketiuid, self.iuids))
            self.socket_client = SocketClient(broker_id=self.paper['broker'], account_id=self.paper['account'],
                                              order_callback=trading_callback, iuid_list=_market_iuids)
            self.activated_symbols.append("iuid")  # 启用的symbol类型
        else:
            self.socket_client = None

        if activate_factset:
            self.factset_symbols = list(map(symbol_converter.from_iuid_to_factsetsymbol, self.iuids))
            self.activated_symbols.add("factset")
        if activate_choice:
            self.choice_symbols = list(map(symbol_converter.from_iuid_to_choicesymbol, self.iuids))
            self.activated_symbols.add("choice")
        if activate_futu:
            self.futu_symbols = list(map(symbol_converter.from_iuid_to_futusymbol, self.iuids))
            self.lot_size = self.lot_size_from_futu(iuids=self.iuids)
            self.activated_symbols.add("futu")
        if activate_intrinio:
            self.intrinio_symbols = list(map(symbol_converter.from_iuid_to_intriniosymbol, self.iuids))
            self.activated_symbols.add("intrinio")
        market.activated_symbol_types = self.activated_symbols

        self.market = Market(self.activated_symbols, socket_client=self.socket_client)
        return self.market

    def __get_rq_client(self, activate_rq_data):
        if activate_rq_data:
            # queue实时推流
            rq_client = RQClient(broker_id=self.paper['broker'],
                                 account_id=self.paper['account'],
                                 order_callback=self.order_manager.update_message,
                                 iuid_list=self.iuids)
            self.market.rq_client = rq_client
            market.rq_client = rq_client
        else:
            # 没有推流 需手动经不同数据源获取当前价格
            rq_client = RQClient(broker_id=self.paper['broker'],
                                 account_id=self.paper['account'],
                                 order_callback=self.order_manager.update_message)
        self.rq_client = rq_client
        return self.rq_client

    def clean_up(self):
        try:
            self.rq_client.disconnect_client()  # 最后记得关闭market consumer client, 否则后台线程关不掉
        except AttributeError as e:
            logger.debug("Disconnection failed: RQ client not in this executor")

        if not self.trading_directions is None and len(self.trading_directions) > 0:
            self.trading_directions.to_csv(self.direction_path)

    def load_model_status(self, model_dir):
        self.direction_path = os.path.join(model_dir,
                                           f'trading_direction_{time.strftime("%Y%m%d")}.csv')

        self.trading_directions = pd.Series(dtype=int)
        if os.path.exists(self.direction_path):
            self.trading_directions = pd.read_csv(self.direction_path,
                                                  index_col=0,
                                                  header=None,
                                                  squeeze=True)
            self.trading_directions = self.trading_directions.astype(int)

    def get_real_time_data_from_msq(self, msqsymbols=None):
        if msqsymbols is None:
            msqsymbols = self.msq_symbols
        data = pd.DataFrame(self.msq_client.orderbook_data).T
        data = data[['a1','av1','b1','bv1']]
        data = data.reset_index()
        data.columns = ['iuid', 'ASK_1', 'ASK_VOL_1', 'BID_1', 'BID_VOL_1']
        data.loc[:, 'PRICE'] = (data['BID_1'] + data['ASK_1']) / 2.
        data.loc[:, 'ORDER_LOT_SIZE'] = self.lot_size
        data = data.where((pd.notnull(data)), None)
        return data

    def get_real_time_data(self):
        return self.market.get_real_time_data(self.iuids)

    def update_direction(self, orders):
        for o in orders:
            iuid = o['symbol']
            if iuid not in self.trading_directions or self.trading_directions[iuid] == 0:
                self.trading_directions[iuid] = 1 if o['direction'] == 'BUY' else -1

    # def message_handling(self):
    #     while True:
    #         if self.msg_queue.empty():
    #             time.sleep(.5)
    #         else:
    #             msg = msg_queue.pop()
    #             self.orderpool.update_message()
