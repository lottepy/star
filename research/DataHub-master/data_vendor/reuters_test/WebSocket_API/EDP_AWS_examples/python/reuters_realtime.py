#!/usr/bin/env python
import time
import logging
import requests
import socket
import json
import threading
from collections import defaultdict
from recordclass import recordclass
from datetime import datetime

import websocket  # NOTE: websocket=0.53

# Global Default Variables

tickdata_field_map = {
    ':': ('TRADE_DATE', 'SALTIM_MS' ),
    '=' : 'timestamp',
    'TRDVOL_1'  : 'last_vol',
    'PRIMACT_1' : 'last',
    'ACVOL_1'   : 'vol',}
    # Accumulated number of shares, lots or contracts traded according to the market convention

# 2019-08-14 15:14:43.918|WARNING|28173:140246940765528|MessageInterFace|Empty time field HK_10_700 metadata QUOTIM_MS {'LOW_1': 337.4, 'ACVOL_1': 12283349, 'TRDVOL_1': 400, 'PRIMACT_1': 337.4, 'SALTIM_MS': 26083000}
metadata_field_map = {
    'OFF_CLOSE'  : 'last_close',
    'OPEN_PRC'   : 'open',
    'HIGH_1'     : 'high',
    'LOW_1'      : 'low',
    'LOT_SIZE_A' : 'lot_size',
    'DSPLY_NAME' : 'dsply_name',
    'PROV_SYMB'  : 'prov_symb',}

orderbook_field_map = {
    ':': ('QUOTE_DATE','QUOTIM_MS'),
    '=' : 'timestamp',
    'BID'     : 'b1',
    'BIDSIZE' : 'bv1',
    'ASK'     : 'a1',
    'ASKSIZE' : 'av1',
    }

Fields = [
    'QUOTE_DATE', 'QUOTIM_MS', 'BID', 'BIDSIZE', 'ASK', 'ASKSIZE',  # orderbook info
    'TRADE_DATE', 'SALTIM_MS', 'PRIMACT_1', 'TRDVOL_1',              # trade info
    'HST_CLOSE', 'OPEN_PRC', 'HIGH_1', 'LOW_1', 'ACVOL_1',
    'LOT_SIZE_A', 'OFF_CLOSE', 'PROV_SYMB', 'DSPLY_NAME'            # other info
]

Fields = [
    'TRADE_DATE', 'SALTIM_MS', 'PRIMACT_1', 'TRDVOL_1'
]

Region = 'HK'

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)

class ReutersRealtime:
    """
        最多订阅500个ticker
    """
    MAX_CODES = 480
    WATCH_INTERVAL = 1

    def __init__(self):
        self._app_id = '256'
        self._auth_hostname = 'api.edp.thomsonreuters.com'
        self._auth_port = '443'
        self._auth_path = 'auth/oauth2/beta1/token'
        self._discovery_path = 'streaming/pricing/v1/'
        # self._password = '[zvd@QyN6H6fPwMs' # 'A2m6GbHKbCV)oLuU'
        # self._user = 'GE-A-01603835-3-1751' #'GE-A-01603835-3-1752'
        self._user = 'GE-A-01603835-3-1752'
        self._password = 'A2m6GbHKbCV)oLuU'
        self._scope = 'trapi'
        self._client_secret = ''
        self._hosts = []
        self._region = 'amer'
        self._ws = None
        self._sts_token = None
        self._refresh_token = None
        self._expire_in = 0      # int（秒级）
        self._login_status = False
        self._stream_id_map = {}  # ric2msgid
        self.logger = log
        self.websocket_connected=threading.Event()


    def get_sts_token(self, current_refresh_token):
        url = 'https://{}:{}/{}'.format(self._auth_hostname, self._auth_port, self._auth_path)
        if not current_refresh_token:
            # First time through, send password
            data = {
                'username': self._user,
                'password': self._password,
                'grant_type': 'password',
                'takeExclusiveSignOnControl': True,
                'scope': self._scope
            }
        else:
            data = {
                'username': self._user,
                'refresh_token': current_refresh_token,
                'grant_type': 'refresh_token',
                'takeExclusiveSignOnControl': True
            }
        try:
            r = requests.post(url=url,
                              headers={'Accept': 'application/json'},
                              data=data,
                              auth=(self._user, self._client_secret),
                              verify=True)
        except requests.exceptions.RequestException as e:
            log.exception('Reuters_authentication|{0}'.format(e))
            return None, None, None
        if r.status_code != 200:
            log.exception('Reuters_authentication|status_code:{0}, reason:{1}, text:{2}'.
                          format(r.status_code, r.reason, r.text))
            if r.status_code == 401 and current_refresh_token:
                # Refresh token may have expired. Try using our password.
                return self.get_sts_token(None)
            return None, None, None
        auth_json = r.json()
        auth_str = '{'
        for k,v in auth_json.items():
            auth_str += f"'{k}':"
            auth_str += f" '{v}'," if not isinstance(v, str) or len(v) < 10 else f" '{v[:20]}...',"
        auth_str += '}'
        log.info('Reuters_authentication|{0}'.format(auth_str))
        return auth_json['access_token'], auth_json['refresh_token'], time.time()*1000 + int(auth_json['expires_in'])

    def query_service_discovery(self):
        url = 'https://{}/{}'.format(self._auth_hostname, self._discovery_path)
        try:
            r = requests.get(url=url,
                             headers={"Authorization": "Bearer " + self._sts_token},
                             params={"transport": "websocket"})

        except requests.exceptions.RequestException as e:
            log.exception('Reuters_query_service|{0}'.format(e))
            return False
        if r.status_code != 200:
            log.exception('Reuters_query_service|status_code:{0}, reason:{1}, text:{2}'.
                          format(r.status_code, r.reason, r.text))
            return False
        response_json = r.json()
        log.info('Reuters_query_service|{0}'.format(response_json))
        for index in range(len(response_json['services'])):
            if self._region == "amer":
                if not response_json['services'][index]['location'][0].startswith("us-"):
                    continue
            elif self._region == "emea":
                if not response_json['services'][index]['location'][0].startswith("eu-"):
                    continue
            elif self._region == "ap":
                if not response_json['services'][index]['location'][0].startswith("ap-"):
                    continue
            if len(response_json['services'][index]['location']) == 1:
                self._hosts.append(':'.join([response_json['services'][index]['endpoint'],
                                             str(response_json['services'][index]['port'])]))
        if len(self._hosts) == 0:
            log.exception('Reuters_query_service|No host found from EDP service discovery')
            return False
        return True

    def send_login_request(self, auth_token, is_refresh_token):
        try:
            position_host = socket.gethostname()
            position = socket.gethostbyname(position_host) + "/" + position_host
        except socket.gaierror:
            position = "127.0.0.1/net"

        login_json = {
            'ID': 1,
            'Domain': 'Login',
            'Key': {
                'NameType': 'AuthnToken',
                'Elements': {
                    'ApplicationId': '',
                    'Position': '',
                    'AuthenticationToken': ''
                }
            }
        }
        login_json['Key']['Elements']['ApplicationId'] = self._app_id
        login_json['Key']['Elements']['Position'] = position
        login_json['Key']['Elements']['AuthenticationToken'] = auth_token

        if is_refresh_token:
            login_json['Refresh'] = False
        self._ws.send(json.dumps(login_json))
        login_json['Key']['Elements']['AuthenticationToken'] = auth_token[:10] + '...'
        log.info('Reuters_request_send|{0}'.format(login_json))

    def send_market_price_request(self, to_subscribe=None):
        tickers = []
        if not to_subscribe:
            to_subscribe = set().union(*[iuids for _, iuids in self.subscribed.items()])
        if not to_subscribe:
            return

        tickers = self.iuid2code(to_subscribe)
        if isinstance(tickers, str):
            tickers = [tickers]
        tickers = [t for t in tickers if t not in self._stream_id_map]

        req_body = {
            'ID': 1 + max(self._stream_id_map.values()) if self._stream_id_map else 2,
            'Domain': 'MarketPrice',
            'Key': {
                'Name': tickers
            },
            'View': Fields
        }
        self._ws.send(json.dumps(req_body))
        log.info('Reuters_request_send|{0}'.format(req_body))

    def send_close_request(self, iuids: list):
        # TODO: unit test
        if not iuids:
            return
        code_list = self.iuid2code(iuids)
        log.debug(f'Reuters_prepare_close_request|Iuids: {iuids} CodeList: {code_list}'
                  f'StreamIdMap: {self._stream_id_map.items()}')
        stream_ids = []
        for code in code_list:
            if code in self._stream_id_map:
                stream_ids.append(self._stream_id_map.pop(code))
        if not stream_ids:
            return
        req_body = {
            'ID': stream_ids,
            'Type': 'Close'
        }
        self._ws.send(json.dumps(req_body))
        log.info(f'Reuters_request_send|{req_body} '
                 f'{code_list}')

    def iuid2code(self, iuid_list):
        # TODO: mysql
        if isinstance(iuid_list, str):
            iuid_list = [iuid_list]
        tickers = []
        for iuid in iuid_list:
            if iuid.endswith('.SZ') or iuid.endswith('.SH'):
                tickers.append(iuid)
                continue
            code = iuid[6:].zfill(4)
            ticker = ''.join([code, '.HK'])
            tickers.append(ticker)
        return tickers

    def code2iuid(self, code_list):
        if not code_list:
            return
        if isinstance(code_list, str):
            return self.code_map.get(code_list)
        else:
            return [self.code_map.get(i) for k in code_list]

    def localize_ticker(self, instrument):
        if instrument.iuid.startswith('HK_10'): # instrument.category == constants.Category.STOCK:
            code = instrument.ticker.zfill(4)
            ticker = ''.join([code, '.HK'])
        else:#  instrument.category == constants.Category.FUTURE or \
             # instrument.category == constants.Category.FX:
            ticker = instrument.tickermap.thomsonreutersid
        # else:
        #     ticker = instrument.ticker
        # log.info(f'Get Ins {instrument.category} {type(instrument.category)} Ticker {ticker}, Insticker: {instrument.ticker}, ')
        return ticker # or instrument.ticker

    def _publish_message(self, message):
        data = message['Fields']
        ric = message['Key']['Name']
        iuid = self.code2iuid(ric)
        routing_key = self.iuid2routing_key(iuid)
        msg_time = message.get('OnMsgTime')
        is_update = message.get('Type') == 'Update'

        return

    def _on_message(self, message, *args, **kwargs):
        msgs = json.loads(message)  # list
        for msg in msgs:
            msg_type = msg['Type']
            if msg_type == 'Update':
                return
        log.info(f'Reuters_message_receive|{message} {args} {kwargs}')
        try:
            msgs = json.loads(message)  # list
            for msg in msgs:
                msg_type = msg['Type']
                if msg_type == 'Refresh':
                    # 处理登陆请求的响应
                    if 'Domain' in msg:
                        message_domain = msg['Domain']
                        if message_domain == 'Login':
                            if msg['State']['Stream'] != "Open" or msg['State']['Data'] != "Ok":
                                self.logger.exception('Reuters_login|Fail '
                                              f"{msg['State']['Text']}")
                            else:
                                self.logger.info('Reuters_login|Success')
                                self._login_status = True
                                #self.send_market_price_request()
                                self.websocket_connected.set()
                    else:  # key
                        self._stream_id_map[msg['Key']['Name']] = msg['ID']
                elif msg_type == 'Ping':
                    # keep conn alive
                    pong_json = {'Type': 'Pong'}
                    self._ws.send(json.dumps(pong_json))
                elif msg_type == 'Status':
                    # TODO: code2routing_key
                    msg_state = msg['State']
                    if msg_state.get('Code', None) == 'NotFound':
                        code = msg['Key']['Name']
                        iuid = self.code2iuid(code)
                        routing_key = self.iuid2routing_key(iuid)
                        self.logger.warning(f'{self.__class__.__name__}|CodeNotFound|'
                                    f'{msg}')
        except Exception as e:
            self.logger.exception('Reuters_handle_response|{0}'.format(e))
        self.websocket_connected.set()

    def _on_error(self, error, *args, **kwargs):
        log.info(f"OnError {args} {kwargs}")
        # log.info('Reuters_websocket_error|{0}'.format(error))
        self._on_close()

    def _on_close(self):
        self._login_status = False
        log.info('Reuters_websocket_close|')

    def _on_open(self, *args, **kwargs):
        print(args, kwargs)
        log.info('Reuters_websocket_open|')
        self.send_login_request(self._sts_token, False)

    def connect(self):
        self._sts_token, self._refresh_token, self._expire_in = self.get_sts_token(None)
        print("Query", self.query_service_discovery())
        self._ws = websocket.WebSocketApp(url="wss://{}/WebSocket".format(self._hosts[0]),
                                          on_message=self._on_message,
                                          on_error=self._on_error,
                                          on_close=self._on_close,
                                          subprotocols=['tr_json2'])
        self._ws.on_open = self._on_open
        # Event loop
        wst = threading.Thread(target=self._ws.run_forever,
                               kwargs={'sslopt': {'check_hostname': False}})
        wst.start()

    def keep_alive(self):
        if not self._hosts:
            return
        self._ws.run_forever(sslopt={'check_hostname': False})

    def subscribe(self, iuid_list):
        if not iuid_list:
            return
        self.send_market_price_request(iuid_list)

    def unsubscribe(self, iuid_list):
        if not iuid_list:
            return
        self.send_close_request(iuid_list)

    def _get_refresh_token(self):
        try:
            sts_token, refresh_token, expire_in = self.get_sts_token(None)
            # TODO: refresh_token or login token?
            if not sts_token:
                return
            self._sts_token, self._refresh_token, self._expire_in = sts_token, refresh_token, expire_in
            if self._login_status:
                self.send_login_request(self._sts_token, True)
            return True

        except Exception as e:
            # import bdb
            # if e is bdb.BbqQuit:
            #     raise(e)
            log.exception('Reuters|get_refresh_token_error|{0}'.format(e))
            return False


if __name__ == '__main__':
    rt = ReutersRealtime()
    rt.connect()
    log.info("wating")
    iuid_list = ['HK_10_1', 'HK_10_2']
        # ['000001.SZ', '000001.SH']
    rt.websocket_connected.wait()
    rt.subscribe(iuid_list)
    try:
        while True:
            time.sleep(10)
            # rt.subscribe(iuid_list)
            # count = 0
            rt._get_refresh_token()
            # Update token.
            # rt.refresh_token(refresh_token)
    except KeyboardInterrupt:
        pass
        # session1.disconnect()
        # if hotstandby:
        #     session2.disconnect()

