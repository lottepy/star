import json
import sys
import logging
import threading
import argparse
import uuid
import time
from threading import Thread
from functools import partial
from os import path
import numpy as np

import pika
import urllib
from requests import Session

WATCH_INTERVAL = 1

MARKET_URL_BASE = 'http://172.31.74.168:36872'
STREAMING_URL = urllib.parse.urljoin(MARKET_URL_BASE, 'v2/market/realtime/subscription')
TICK_EXCHANGE = 'market.full.tick'
ORDERBOOK_EXCHANGE = 'market.full.orderbook'
METADATA_EXCHANGE = 'market.full.metadata'

StreamingWhiteList = {"HK_10", "HK_60", "US_10", "SG_60","DY_FX"}

def logconfig():
    # set up logging to file - see previous section for more details
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M',
                        filename='log/RQClient.log',
                        filemode='a')
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    # set a format which is simpler for console use
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console)

logger = logging.getLogger('RQClient')
logging.getLogger("pika").setLevel(logging.WARNING)
# TODO: move to RQClient
_session = Session()

def ack_message(channel, delivery_tag):
    """Note that `channel` must be the same pika channel instance via which
    the message being ACKed was retrieved (AMQP protocol constraint).
    """
    if channel.is_open:
        channel.basic_ack(delivery_tag)
    else:
        # Channel is already closed, so we can't ACK this message;
        # log and/or do something that makes sense for your app in this case.
        pass


class RQClient:
    """
    params: iuid_list, broker_id, account_id, tick_callback, orderbook_callback,
    order_callback, metadata_callback, execution_callback
    pass the ones you need

    E.g.

        def order_callback(data):
            print(data)

        c = RQClient(iuid_list=['GB_40_USDCAD_full','CN_10_000001'],
                     broker_id='101',account_id='010101010',order_callback=order_callback)
        print(c.tick_data)
    """

    def __init__(self, snapshot_timeout=60,
                 snapshot_refresh_interval=10,
                 market_url_base=MARKET_URL_BASE,
                 exchanges=[TICK_EXCHANGE, ORDERBOOK_EXCHANGE, METADATA_EXCHANGE],
                 force_streaming=False,
                 **kwargs):
        """Pass the parameters you need.

        Parameters
        ----------
        iuid_list : list, optional
            A list of iuids
        broker_id : str, optional
            Has to be with account_id if passed
        account_id : int, optional
            Has to be with broker_id if passed
        force_streaming: 
        tick_callback : function, optional
        orderbook_callback : function, optional
        metadata_callback : fucntiuon, optional
        order_callback : function, optional
        execution_callback : function, optional
        opts: options
        """
        assert bool(kwargs.get('iuid_list')) >= bool(kwargs.get('tick_callback')), "You provided tick_callback without proving iuid_list."
        assert bool(kwargs.get('iuid_list')) >= bool(kwargs.get('orderbook_callback')), "You provided orderbook_callback without proving iuid_list."
        assert bool(kwargs.get('broker_id')) == bool(kwargs.get('account_id')), "You should provide both / none of broker_id and account_id."
        assert bool(kwargs.get('broker_id')) >= bool(kwargs.get('order_callback')), "You provided order_callback without proving broker_id & account_id."
        assert bool(kwargs.get('broker_id')) >= bool(kwargs.get('execution_callback')), "You provided execution_callback without proving broker_id & account_id."
        now = time.time()

        self.MARKET_URL_BASE = market_url_base
        self.snapshot_refresh_interval = snapshot_refresh_interval
        self.snapshot_timeout = snapshot_timeout
        self.force_streaming = force_streaming
        self.logger = logging.getLogger('RQClient')
        self.opts = kwargs.get('opts')
        self.last_receive_streaming_time = now
        self.last_snapshot_time = 0
        self.last_subscribe_time = 0
        self.keep_running = True
        self.exchanges=exchanges

        self.iuid_list =  set(kwargs.get('iuid_list', []))
        if isinstance(self.iuid_list, str):
            self.iuid_list = set([self.iuid_list])
        self.streaming_iuids, self.snapshot_iuids = set(), set()
        if self.iuid_list:
            for iuid in self.iuid_list:
                if self.force_streaming or iuid[:5] in StreamingWhiteList:
                    self.streaming_iuids.add(iuid)
                else:
                    self.snapshot_iuids.add(iuid)
        

        streaming_routing_keys = [self.iuid2routingkey(iuid)  for iuid in self.streaming_iuids]

        self.tick_args = {
            'name': 'tick',
            'exchange': exchanges[0],
            'routing_keys': streaming_routing_keys,
            'callback': kwargs.get('tick_callback') if kwargs.get('tick_callback') else (lambda x:x)
        } if kwargs.get('iuid_list') else None

        self.orderbook_args = {
            'name': 'orderbook',
            'exchange': exchanges[1],
            'routing_keys': self.tick_args['routing_keys'],
            'callback': kwargs.get('orderbook_callback') if kwargs.get('orderbook_callback') else (lambda x:x)
        } if kwargs.get('iuid_list') else None

        self.metadata_args = {
            'name': 'metadata',
            'exchange': exchanges[2],
            'routing_keys': self.tick_args['routing_keys'],
            'callback': kwargs.get('metadata_callback') if kwargs.get('metadata_callback') else (lambda x: x)
        } if kwargs.get('iuid_list') else None

        self.order_args = {
            'name': 'order',
            'exchange': 'trading',
            'routing_keys': ["order.{}.{}".format(kwargs.get('broker_id'), kwargs.get('account_id'))],
            'callback': kwargs.get('order_callback') if kwargs.get('order_callback') else (lambda x:x)
        } if kwargs.get('broker_id') else None

        self.execution_args = {
            'name': 'execution',
            'exchange': 'trading',
            'routing_keys': ["execution.{}.{}".format(kwargs.get('broker_id'), kwargs.get('account_id'))],
            'callback': kwargs.get('execution_callback') if kwargs.get('execution_callback') else (lambda x:x)
        } if kwargs.get('broker_id') else None

        self.args = {
            k:v for k,v in (
                ('tick', self.tick_args),
                ('orderbook', self.orderbook_args),
                ('order', self.order_args),
                ('execution', self.execution_args),
                ('metadata', self.metadata_args),
            ) if v is not None
        }

        self.data = {
            'tick': {},
            'orderbook': {},
            'order': {},
            'execution': {},
            'metadata': {},
        }

        self._init_rq()
        # time.sleep(0.5)
        # self.update_subscribe_list(kwargs.get('iuid_list'))

        self.watcher = Thread(target=self.watch, name="RQClientWatcherThread")
        self.watcher.setDaemon(True)
        self.watcher.start()

    def __del__(self):
        self.disconnect_client()

    def _init_rq(self):
        credentials = pika.PlainCredentials('aqumon', 'aqumon2050')
        connection_param = pika.ConnectionParameters(
            host='172.31.74.168',
            port=5673,
            credentials=credentials,
            virtual_host='/v3'
        )
        self.connection = pika.BlockingConnection(connection_param)
        channel = self.connection.channel()
        for name, arg in self.args.items():
            result = channel.queue_declare(exclusive=True, auto_delete=True)
            queue_name = result.method.queue
            for routing_key in arg['routing_keys']:
                channel.queue_bind(
                    exchange=arg['exchange'],
                    queue=queue_name,
                    routing_key=routing_key
                )
            # self.logger.info(f"{arg['exchange']}, {queue_name}, {arg['routing_keys']}")
            consumer_callback = partial(self.callback, message_type=name)
            channel.basic_consume(
                consumer_callback,
                queue=queue_name,
                no_ack=False
            )
        t = threading.Thread(target=channel.start_consuming, name="PikaConnectionThread")
        t.setDaemon(True)
        t.start()

    def _update_dict_in_callback(self, old_dict, new_dict):
        # temporary solution for incremental data.
        # but this should be done in server side
        return_dict = {}
        for k in new_dict.keys():
            if new_dict[k] == None:
                return_dict[k] = old_dict[k]
            else:
                return_dict[k] = new_dict[k]
        return return_dict

    def callback(self, ch, method, properties, body, message_type=None):
        self.last_receive_streaming_time = time.time()
        data = json.loads(body)
        if self.opts and self.opts.print_message:
            self.logger.info(f"RQClient  - {message_type} "
                        f"{method.routing_key} | {body}")
        key = self.routingkey2iuid(method.routing_key)
        if data.get('id'): # Order/ Execution
            self.data[message_type][key+"."+str(data.get('id'))] = data
        else: # Tick/Orderbook
            if self.opts and self.opts.print_data_num and key not in self.data[message_type]:
                self.logger.info(f"{message_type} Len: {len(self.data[message_type])}")
            self.data[message_type][key] = data
            data['iuid'] = key
        self.args[message_type]['callback'](data)

    def update_subscribe_list(self, iuid_list):
        """subscribe the needed info (i.e., orderbook, tick, trades) from data vendors
        (i.e., futu, Bloomberg) before using them by `self.callback` function."""
        url = urllib.parse.urljoin(self.MARKET_URL_BASE, 'v2/market/realtime/subscription')
        if not iuid_list:
            self.logger.info("No iuid provided.")
            return
        params = {
            'iuids': ','.join(iuid_list),
        }
        req_id = uuid.uuid4().hex
        logger.info(f"RQServer_Subscribe_Request_Sent: {url} {req_id} {params}")
        # self.logger.info(f"If you find the subscription is not successful, please send the following string to the person in charge: {req_id}")
        resp = _session.post(url, json=params, headers={'x-request-id': req_id}).json()
        logger.info(f"RQClient_Subscribe_Data_Receive: {resp}")
        if isinstance(resp, dict) and 'data' in resp:
            return resp['data']
        return resp

    def get_snapshot_data(self, iuid_list):
        if not iuid_list:
            return
        url = urllib.parse.urljoin(self.MARKET_URL_BASE,
                                   'v2/market/instruments/bbg/snapshoot')
        params = {
            'iuids': ','.join(iuid_list),
        }
        req_id = uuid.uuid4().hex
        logger.info(f"RQClient_Snapshot_Request_Sent: {url} {req_id} {params}")
        resp = _session.post(url, json=params, headers={'x-request-id': req_id},verify=False
                             ).json()
        logger.info(f"RQClient_Snapshot_Data_Receive: {resp}")
        if isinstance(resp, dict) and 'data' in resp:
            return resp['data']
        else:
            logger.error(f"RQClient Abnormal_response:{resp}")
        return resp

    def watch(self):
        """Watch loop."""
        while self.iuid_list and self.keep_running:
            now = time.time()
            if now - self.last_snapshot_time > self.snapshot_refresh_interval:
                snapshot_iuids = self.snapshot_iuids
                if now - self.last_receive_streaming_time > self.snapshot_timeout:
                    snapshot_iuids = snapshot_iuids.union(self.streaming_iuids)
                if self.snapshot_iuids:
                    self.last_snapshot_time = now
                    market_data = self.get_snapshot_data(snapshot_iuids)
                    for iuid, data in market_data.items():
                        # TODO: data_type naming standerd
                        for data_type, update in data.items():
                            self.data[data_type][iuid] = update
                            self.args[data_type]['callback'](update)
            if now - self.last_subscribe_time > 40*60:
                self.update_subscribe_list(self.streaming_iuids)
                self.last_subscribe_time = now
            time.sleep(WATCH_INTERVAL)

    def disconnect_client(self):
        # TODO: kill thread
        self.keep_running = False
        try:
            return self.connection.close()
        except:
            pass

    @property
    def tick_data(self):
        return self.data['tick']

    @property
    def orderbook_data(self):
        return self.data['orderbook']

    @property
    def order_data(self):
        return self.data['order']

    @property
    def meta_data(self):
        return self.data['metadata']

    @property
    def execution_data(self):
        return self.data['execution']

    @staticmethod
    def iuid2routingkey(iuid):
        return iuid.replace("_", ".")

    @staticmethod
    def routingkey2iuid(key):
        return key.replace(".", "_")

    @staticmethod
    def get_parser():
        parser = argparse.ArgumentParser()
        parser.add_argument('--market_url', '-m',  default='local')
        parser.add_argument('--iuid_nums', '-n',  default=-1, type=int)
        parser.add_argument('--list', '-l', nargs="*",  default=['HK_10'])
        parser.add_argument('--print_message', '-pm',  action='store_true')
        parser.add_argument('--print_data_num', '-pdn', action='store_true')
        parser.add_argument('--pdb', '-pdb', action='store_true')
        parser.add_argument('--streaming', '-s', action='store_true')
        return parser

    def get_data_sizes(self):
        return {k:len(v) for k, v in self.data.items()}

    def is_data_complete(self, name='metadata'):
        return set(self.data[name].keys()) == set(self.iuid_list)


if __name__ == '__main__':

    logconfig()
    parser = RQClient.get_parser()
    args = parser.parse_args(sys.argv[1:])
    if args.market_url == 'local':
        MARKET_URL_BASE = 'http://localhost:36872'
    elif args.market_url == '62':
        MARKET_URL_BASE = 'http://192.168.11.62:36872'
    elif args.market_url == 'HK':
        MARKET_URL_BASE = 'http://172.31.74.168:36872'
    else:
        raise ValueError(f"{args[0]} not known.")

    if args.market_url=='HK':
        TICK_EXCHANGE = 'market.full.tick'
        ORDERBOOK_EXCHANGE = 'market.full.orderbook'
        METADATA_EXCHANGE = 'market.full.metadata'
    else:
        TICK_EXCHANGE = 'market.test.tick'
        ORDERBOOK_EXCHANGE = 'market.test.orderbook'
        METADATA_EXCHANGE = 'market.test.metadata'

    _market_iuids = {
        # 'HK_10': ['HK_10_700'],
        'HK_10': ["HK_10_700", "HK_10_1128", "HK_10_1513", "HK_10_2666", "HK_10_1458",
        "HK_10_1816", "HK_10_410", "HK_10_1919", "HK_10_525", "HK_10_3799", "HK_10_570",
        "HK_10_1530", "HK_10_1558", "HK_10_1233", "HK_10_1600", "HK_10_916", "HK_10_3633",
        "HK_10_1381", "HK_10_548", "HK_10_586", "HK_10_743", "HK_10_1099", "HK_10_3360",
        "HK_10_902", "HK_10_2768", "HK_10_3301", "HK_10_3883", "HK_10_2588", "HK_10_1610",
        "HK_10_1157", "HK_10_2888", "HK_10_2", "HK_10_3", "HK_10_4", "HK_10_5", "HK_10_6",
        "HK_10_19", "HK_10_1810", "HK_10_20", "HK_10_23", "HK_10_26", "HK_10_29", "HK_10_41",
        "HK_10_45", "HK_10_50", "HK_10_51", "HK_10_56", "HK_10_62", "HK_10_78",
        ],
        # 'US_10': ['US_10_AAPL', 'US_10_GE', 'US_10_MSFT', 'US_10_IBM'],
        'US_10': [
            'US_10_AABA', 'US_10_AAC', 'US_10_AAL', 'US_10_AAMC', 'US_10_AAME', 'US_10_AAN',
            'US_10_AAOI', 'US_10_AAON', 'US_10_AAP', 'US_10_AAPL', 'US_10_AAT', 'US_10_AAU',
            'US_10_AAWW', 'US_10_AAXN', 'US_10_AB', 'US_10_ABB', 'US_10_ABBV', 'US_10_ABC', 'US_10_ABCB',
            'US_10_ABDC', 'US_10_ABEO', 'US_10_ABEV', 'US_10_ABG', 'US_10_ABIL', 'US_10_ABIO', 'US_10_ABM',
            'US_10_ABMD',
        ],
        'HK_60':[
            'HK_60_HSI', "HK_60_MHI",
            ],
        'HK_ETF': [
            "HK_10_3141",'HK_10_3081'
        ],
        'GB_40': [
            'GB_40_SGDHKD', 'GB_40_EURUSD', 'GB_40_USDJPY'
        ],
        'SG_60': ['SG_60_CNZ8', 'SG_60_CNG12'],
        'GB_10': ['GB_10_WTB', 'GB_10_IMI'],
    }

    logger.info(f"RQClient Market Url: {MARKET_URL_BASE} ListName: {args.list}")
    iuid_list = []
    for lname in args.list:
        iuid_list.extend(_market_iuids.get(lname, [lname]))
    # iuid_list = _market_iuids.get(args.list, [args.list])

    if args.iuid_nums > 0 and args.iuid_nums < len(iuid_list):
        np.random.shuffle(iuid_list)
        iuid_list = iuid_list[:args.iuid_nums]

    logger.info(f"RQClient Market Url: {MARKET_URL_BASE} ListName: {args.list} "
                f"ListCount: {len(iuid_list)}")
    start_time = time.time()


    rq_client = RQClient(iuid_list=iuid_list, opts=args,
                         market_url_base=MARKET_URL_BASE,
                         exchanges=[TICK_EXCHANGE, ORDERBOOK_EXCHANGE, METADATA_EXCHANGE],
                         snapshot_timeout=6000,
                         force_streaming=args.streaming,
                         )
    rq = rq_client

    if args.pdb:
        import ipdb; ipdb.set_trace()

    full_flag = {'tick':False, 'orderbook':False, 'metadata': False}
    start_time = time.time()
    while True:
        time.sleep(.5)
        for name in full_flag:
            if not full_flag[name] and rq.is_data_complete(name):
                logger.info(f"**** {name} is full {rq.data[name]}")
                full_flag[name] = True
        if args.pdb and time.time() - start_time > 8:
            import ipdb; ipdb.set_trace()

