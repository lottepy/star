import collections
import threading
import time
import pandas as pd
from queue import Queue

from ...data import RQClient, socket_client
from ..config import constant
from ..log.get_logger import get_logger
from ..trade.trading import trading
from ..trade.trading_controller import TradingController


class Order():

    def __init__(self, symbol, price, direction, quantity,trading_type,source):
        self.price = price
        self.symbol = symbol
        self.direction = direction
        self.quantity = quantity
        self.trading_type = trading_type
        self.source = source

    @property
    def order_dict(self):
        dic = {
            "symbol": self.symbol,
            "side": self.direction,
            "type": self.trading_type,
            "price": self.price,
            "quantity": self.quantity,
            "source": self.source,
        }
        return dic


class OrderRecord():

    def __init__(self, symbol, oid, direction):
        
        self.symbol = symbol
        self.direction = direction
        self.filled_price = 0
        self.filled_quantity = 0
        self.filled_amount = 0
        self.oid = oid
        self.status = None
        self.renew_time = time.time()

    def __str__(self):
        return f"symbol:{self.symbol},direction:{self.direction},filled_price:{self.filled_price},filled_quantity:{self.filled_quantity},filled_amount:{self.filled_amount} \
                ,oid:{self.oid},status:{self.status},renew_time:{self.renew_time}"



class OrderSubmitter():

    def __init__(self, config, iuids, on_tick, on_trade, on_info,logger=get_logger()):
        self.configs = config
        self.iuids = iuids
        self.on_tick = on_tick
        self.on_trade = on_trade
        self.on_info = on_info
        self.logger = logger

        self.paper = {'broker': self.configs .broker,'account': self.configs .account,'subaccount': self.configs .subaccount }
        self.region = self.configs.region
        self.currency = constant.region_currency_map[self.region]

        self.order_queue = Queue()
        #self.current_holding = collections.defaultdict(int)
        #self.short_volume = collections.defaultdict(int)
        self.id2order_record = {}
        self.current_cash = 0
        
        self.reserved_cash = {}
        self.order_thread = threading.Thread(target=self.order_dequeue,name="OrderSubmitter")
        self.order_thread.setDaemon(True)
        self.regalur_thread = threading.Thread(target=self.regular_task,name="RegalurThread")
        self.regalur_thread.setDaemon(True)
        self.client = None
        self.lot_size = {}
        self.reserved_short_volume = {}
        self.reserved_long_volume = {}

    


    def run(self):
        self.trading_ctrl = TradingController(self.configs.endpoint)
        self.trading_ctrl.sync_holdings(self.paper['broker'],self.paper['account'])  # 查初始持仓前先同步
        self.logger.debug(f"Holdings synced")
        self.current_asset, self.current_cash = self.get_asset(self.trading_ctrl, self.configs.asset_limit,self.configs.cash_limit)
        self.logger.info(f"current_cash:{self.current_cash}")
        self.logger.info(f"reserved_cash:{sum(map(lambda x: x[1], self.reserved_cash.items()))}")
        init_holdings = trading.get_current_positions(self.trading_ctrl,self.paper,self.region)
        init_holdings.set_index('iuid', inplace=True)
        self.logger.info(f"init_holdings:\n{init_holdings}")
        self.current_holding = init_holdings['holdingPosition']
        self.short_volume = init_holdings['shortVolumeToday']
        self.init_volume = init_holdings['longVolumeHistory'] 
        for iuid in self.iuids:
            if iuid not in self.current_holding:
                self.current_holding[iuid] = 0
                self.short_volume[iuid] = 0
                self.init_volume[iuid] = 0
        self.long_volume = self.current_holding-self.init_volume + self.short_volume
        self.reserved_short_volume= pd.Series(index=self.current_holding.index, dtype='int32')
        self.reserved_long_volume = pd.Series(index=self.current_holding.index, dtype='int32')

        self.order_thread.start()
        self.regalur_thread.start()
        self.send_account_info()
        self.client = self.get_client()
        self.logger.info("OrderSubmitter run")

    def get_client(self):
        configs = self.configs
        if configs.kuanrui_enabled:
            client = socket_client.SocketClient(
                iuid_list=self.iuids,
                broker_id=self.paper['broker'],
                account_id=self.paper['account'],
                tick_callback=self.tick_enqueue,
                orderbook_callback=self.orderbook_enqueue,
                order_callback=self.trade_callback,
                HOST=self.configs.HOST,
                PORT_MDS=self.configs.PORT_MDS,
                PORT_OES=self.configs.PORT_OES
            )
            self.lot_size = collections.defaultdict(lambda x: 100)
            for iuid in self.iuids:
                self.lot_size[iuid] = 100
        else:
            client = RQClient.RQClient(
                iuid_list=self.iuids,
                broker_id=self.paper['broker'],
                account_id=self.paper['account'],
                tick_callback=self.tick_enqueue,
                orderbook_callback=self.orderbook_enqueue,
                order_callback=self.trade_callback,
                metadata_callback=self.metadata_callback,
                force_streaming=True,
            )
        return client

    def tick_enqueue(self, msg):
        # if `snapshot` is already in MsgType, then there is no need to add it manually.
        if not msg.get("MsgType"):
            msg["MsgType"] = constant.MessageType.TICK
        self.on_tick(msg)

    def orderbook_enqueue(self, msg):
        msg["MsgType"] = constant.MessageType.ORDERBOOK
        self.on_tick(msg)

    def metadata_callback(self, msg):
        """Only for RQClient, record lotsize.
        """
        if 'lot_size' in msg:
            self.lot_size[msg['iuid']] = int(msg['lot_size'])
        msg["MsgType"] = constant.MessageType.METADATA
        self.on_tick(msg)

    def trade_callback(self, msg):
        self.logger.debug(f"TradeCallback: {msg}")
        iuid = msg['iuid']
        oid = msg['id']
        status = msg['status']

        if oid not in self.id2order_record:
            self.logger.info(f"Order {oid} {iuid} {status} is not belong here")
        else:
            order_record = self.id2order_record[oid]
            order_record.filled_price = msg['filledPrice']
            order_record.filled_quantity = msg['filledQuantity']
            order_record.filled_amount = msg['filledAmount']
            order_record.direction = msg['direction']
            order_record.status = msg['status']
            quantity = msg['quantity']
            status = msg['status']
            if status in ["FILLED", "REJECTED", "CANCELLED"]:
                self.reserved_cash[oid] = 0
                side = 1 if order_record.direction == "BUY" else -1
                self.current_cash += order_record.filled_amount * -1 * side
                self.current_holding[iuid] += order_record.filled_quantity * side
                self.short_volume[iuid] += order_record.filled_quantity * max(-1*side, 0)
                self.long_volume [iuid] += order_record.filled_quantity * max(side, 0)
                self.reserved_short_volume[iuid] -= quantity * max(-1*side, 0)
                self.reserved_long_volume[iuid] -= quantity * max(side, 0)

            self.logger.info(f"Order {oid} {iuid} {status}")
            self.send_account_info()
            self.on_trade(msg)
            



    def submit_order(self, symbol, price, direction, quantity,trading_type,source):
        if direction == "SELL" and symbol in self.base_volume and self.base_volume[symbol] != 0 and quantity > self.base_volume[symbol]:
            quantity = int(min(self.base_volume[symbol],quantity)) 
            self.logger.info(f"id:{source} with symbol:{symbol} sell too much, now quantity is changed to {quantity}")
        o = Order(symbol, price, direction, quantity,trading_type,source)
        self.order_queue.put(o)
        return None

    def order_dequeue(self):
        while True:
            order = self.order_queue.get()
            try:
                orderid = self.handle_order(order)
            except Exception as e:
                orderid = None
                self.logger.error(f"{e}", exc_info=True)
            if orderid:
                self.id2order_record[orderid] = OrderRecord( order.symbol, orderid, order.direction)
                self.reserved_cash[orderid] = order.price * order.quantity if order.direction == "BUY" else 0
                self.reserved_long_volume[order.symbol] += order.quantity if order.direction == "BUY" else 0
                self.reserved_short_volume[order.symbol] += order.quantity if order.direction == "SELL" else 0


    def handle_order(self, order):
        order_dict = order.order_dict
        submition_result = self.trading_ctrl.submit_orders(baccount=self.paper['account'], brokerid=self.paper['broker'],subaccount=self.paper.get('subaccount'),orders=[order_dict])
        self.logger.debug(f"Order submition result: {submition_result}")
        if submition_result['status']['ecode'] != 0:
            self.logger.error("Submitted orders failed.\n{}".format(submition_result))
            return None
        oid = submition_result["data"][0]["id"]
        return oid

    def regular_task(self):
        while True:
            time.sleep(1)
            for orderid,record in self.id2order_record.items():
                if record.status not in ["FILLED", "REJECTED", "CANCELLED"] and record.status is not None and record.renew_time + self.configs.timeout <time.time():
                    self.logger.info(f"cancelling order:{str(record)}")
                    self.cancel_order(orderid)


    def cancel_order(self,oid):
        cancel_result = self.trading_ctrl.cancel_order(oid)
        if cancel_result.get('status').get('ecode') == 0:
            self.logger.info( "Successfully cancelled unfilled orderid {}".format(oid))
        else:
            self.logger.error("Unfilled orderid {} unable to cancel, as: {}.".format(oid, cancel_result['status'].get('message')))



    


    def send_account_info(self):
        msg = {"purchase_power": self.purchase_power,"current_holding": self.current_holding, "short_volume": self.short_volume+self.reserved_short_volume , \
                "long_volume": self.long_volume+self.reserved_long_volume ,"init_volume": self.init_volume}
        #self.logger.info(f"Account_info:\n {msg}")
        self.on_info(msg)
   

    @property
    def purchase_power(self):
        return self.current_cash - sum(map(lambda x: x[1], self.reserved_cash.items()))

    @property
    def base_volume(self):
        return self.init_volume - self.short_volume

    @property
    def execution_status(self):
        return "Idle"


    def get_asset(self, trading_ctrl, asset_limit=-1, cash_limit=-1):
        cash_info = trading.get_current_cash_info(trading_ctrl, self.paper, self.currency)
        current_asset = cash_info['asset']
        current_cash = cash_info['balance']
        if asset_limit > 0:
            current_asset = min(current_asset, asset_limit)
        if cash_limit > 0:
            current_cash = min(current_cash, cash_limit)
        return current_asset, current_cash

    

