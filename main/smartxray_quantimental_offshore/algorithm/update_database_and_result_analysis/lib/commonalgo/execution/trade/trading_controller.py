import json
import time
from functools import wraps

import requests

from ..log.get_logger import get_logger

logger = get_logger(__name__)

_session = requests.session()

NUMBER_OF_RECONNECT = 5
SECONDS_TO_RECONNECT = 3

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


class TradingController(object):
    """
        TradingController(endpoint='http://test.aqumon.com:5565/api/v3/')
    """
    def __init__(self, endpoint='http://test.aqumon.com:5565/api/v3/'):
        if not endpoint:
            self.endpoint = 'http://test.aqumon.com:5565/api/v3/'
        else:
            self.endpoint = endpoint

    @reconnect
    def get_cash_info(self, brokerid='1', baccount='U123456', is_subaccount=False):

        if is_subaccount:
            endpoint = self.endpoint + 'SubAccount/SubAccount'
            params = {
                'subAccount': baccount
            }
        else:
            endpoint = self.endpoint + 'trading/brokerAccount'
            params = {
                'brokerId': brokerid,
                'brokerAccount': baccount,
            }

        t = NUMBER_OF_RECONNECT
        while t:
            response = _session.get(url=endpoint, params=params)
            result = response.json()

            # Verify the data is normal
            if result.get('status'):
                if result['status']['ecode'] == 0 and result['data'].get('cashInfoMap'):
                    return result['data'].get('cashInfoMap')
                # if result['status']['ecode'] == 100008 or not reslut['data'].get('cashInfoMap'):
                logger.warning(f'get cash info from server error: {result}, will retry!')
                t -= 1

        raise ValueError("Cash info abnormal. Check {}".format(response.url))

        # if result.get('success'):
        #     return result.get('data').get('cashInfoMap')
        # else:
        #     return result

    @reconnect
    def get_executions(self, brokerid='1', baccount='U123456'): # FIXME: API change

        endpoint = self.endpoint + 'trading/accounts/executions'
        params = {
            'brokerId': brokerid,
            'brokerAccount': baccount,
        }
        result = _session.get(url=endpoint, params=params).json()

        if not result.get('status').get("ecode"):
            return result.get('data')
        else:
            return result

    @reconnect
    def get_holdings(self, brokerid='1', baccount='U123456'):
        """ 从服务器后端获取holdings """
        endpoint = self.endpoint + 'trading/holdings'
        params = {
            'brokerId': brokerid,
            'brokerAccount': baccount,
        }
        response = _session.get(url=endpoint, params=params)
        result = response.json()

        # Verify the data is normal
        if result.get('status'):
            if result.get('status').get('ecode') == 0:
                return result.get('data')

        raise ValueError("Holding info abnormal. Check {}".format(response.url))

        # if result.get('success'):
        #     return result.get('data')
        # else:
        #     return result

    @reconnect
    def get_broker_holdings(self, brokerid='1', baccount='U123456'):
        """ 直接从broker获取holdings """
        endpoint = self.endpoint + 'trading/brokerHoldings'
        params = {
            'brokerId': brokerid,
            'brokerAccount': baccount,
        }
        response = _session.get(url=endpoint, params=params)
        result = response.json()

        # Verify the data is normal
        if result.get('status'):
            if result.get('status').get('ecode') == 0:
                return result.get('data')

        raise ValueError("Holding info abnormal. Check {}".format(response.url))

    @reconnect
    def get_iuid_holding(self, brokerid='1', baccount='U123456', iuid='US_10_VTI'):
        endpoint = self.endpoint + 'trading/holding'
        params = {
            'brokerId': brokerid,
            'brokerAccount': baccount,
            'iuid': iuid

        }
        result = _session.get(url=endpoint, params=params).json()

        if result.get('success'):
            return result.get('data')
        else:
            return result

    @reconnect
    def get_orders(self, brokerid='1', baccount='U123456'):  # FIXME: API change
        endpoint = self.endpoint + 'trading/accounts/holdings'
        params = {
            'brokerId': brokerid,
            'brokerAccount': baccount,
        }
        result = _session.get(url=endpoint, params=params).json()

        if not result.get('status').get("ecode"):
            return result.get('data')
        else:
            return result

    # never used
    # def submit_order(self, brokerid='1', baccount='U123456', subaccount=None, modelcode=None, order={}):
    #     # order = dict(
    #     #     type = 'Limit', # Market
    #     #     side = 'Buy',
    #     #     symbol = 'US_10_VTV',
    #     #     quantity = 100,
    #     #     price = 139
    #     # )
    #     endpoint = self.endpoint + 'trading/order'
    #     payload = {
    #         "amount": 0,
    #         "bactchId": "string",
    #         "brokerAccount": baccount,
    #         "brokerId": brokerid,
    #         "modelCode": modelcode,
    #         "direction": order.get('side'),
    #         "iaccountId": 0,
    #         "iuid": order.get('symbol'),
    #         "localId": "string",
    #         "portfolioId": 0,
    #         "price": round(order.get('price'), 2),  # 保留两位小数 避免出现类似9.8000000000001的值
    #         "priceType": order.get('type'),
    #         "quantity": order.get('quantity'),
    #         "source": "string",
    #     }
    #     if subaccount is not None:
    #         payload["subAccount"] = subaccount
    #     result = _session.post(url=endpoint, json=payload).json()

    #     if result.get('success'):
    #         return result.get('data')
    #     else:
    #         return result

    def submit_orders(self, brokerid='1', baccount='U123456', orders=[]):
        # order = dict(
        #     type = 'Limit', # Market
        #     side = 'Buy',
        #     symbol = 'US_10_VTV',
        #     quantity = 100,
        #     price = 139
        # )
        endpoint = self.endpoint + 'trading/orders'
        payload = []
        for order in orders:
            assert order['price'] > 0
            assert order['quantity'] > 0
            payload_data = {
                "amount": 0,
                "bactchId": "string",
                "brokerAccount": baccount,
                "brokerId": brokerid,
                "direction": order['side'],
                "iaccountId": 0,
                "iuid": order['symbol'],
                "localId": "string",
                "portfolioId": 0,
                "price": order['price'],
                "priceType": order['type'],
                "quantity": order['quantity'],
                "source": order.get('source', 'string')  # 默认为'string'，可标明order的来源，可用于子账户/子策略不同order来源的分辨
            }
            payload.append(payload_data)
        # result = _session.post(url=endpoint, data= json.dumps(payload), headers =_headers)
        logger.debug(f'EXECUTING:  _session.post(url="{endpoint}", json={payload})\n')
        t1 = time.time()
        result = _session.post(url=endpoint, json=payload).json()
        t2 = time.time()
        logger.debug(f'************* the request took {t2-t1}s, average {(t2-t1)/len(payload)*1000}ms')

        if result.get('success'):
            return result.get('data')
        else:
            return result

    @reconnect
    def cancel_order(self, orderid=None):
        endpoint = self.endpoint + 'trading/orders/{}'.format(orderid)
        result = _session.delete(url=endpoint).json()
        # if result.get('success'):
        #     return result.get('data')
        # else:
        #     return result
        return result

    @reconnect
    def get_order_status(self, orderid= None):
        endpoint = self.endpoint + 'trading/order'
        params = {
            'orderId': orderid
        }
        result = _session.get(url=endpoint, params=params).json()

        if result.get('success'):
            return result.get('data')
        else:
            return result

    @reconnect
    def get_order_executions(self, brokerid='1', baccount='U123456', orderid=''): # FIXME: 'message': "Request method 'GET' not supported"
        endpoint = self.endpoint + 'trading/orders/{}/executions'.format(orderid)
        params = {
            'orderId': orderid
        }
        result = _session.get(url=endpoint, params=params).json()

        if not result.get('status').get("ecode"):
            return result.get('data')
        else:
            return result

    @reconnect
    def get_account_orders(self, brokerid='1', baccount='DU1161140'):
        endpoint = self.endpoint + 'trading/orders'
        params = {
            "brokerId": brokerid,
            "brokerAccount": baccount,
        }
        result = _session.get(url=endpoint, params=params).json()
        return result

    @reconnect
    def sync_holdings(self, brokerid, baccount):
        logger.debug('sync holdings..')
        endpoint = self.endpoint + 'trading/manage/syncHoldings'
        params = {
            "brokerId": brokerid,
            "brokerAccount": baccount,
        }

        t = NUMBER_OF_RECONNECT
        while t:
            result = _session.put(url=endpoint, params=params).json()

            if result.get('status'):
                if result['status']['ecode'] == 0:
                    return result
                elif result['status']['ecode'] == 100006:
                    logger.warning(f'sync error: {result}, will retry!')
                    t -= 1
                else:
                    t -= 1

        raise RuntimeError(f"sync holdings abnormal: {result}")

    @reconnect
    def get_subaccount_list(self, broker_account):
        endpoint = self.endpoint + 'SubAccount/SubAccounts'
        params = {
            "brokerAccount": broker_account
        }
        result = _session.get(url=endpoint, params=params).json()

        if result.get('status'):
            if result['status'].get('ecode') == 0:
                return result.get('data')
        raise RuntimeError('get subaccount error')
