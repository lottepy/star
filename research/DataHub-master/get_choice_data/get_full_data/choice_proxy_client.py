import json
import uuid
from logging import getLogger

from requests import session
import pandas as pd

_session = session()
logger = getLogger('main')

HOST = '139.224.234.6'
PORT = 8394
class ChoiceCaller(object):
    def __init__(self, end_point):
        self._end_point = end_point

    def call(self, method, *args, **kwargs):
        end_point = self._end_point
        params = {
            'method': method,
            'args': json.dumps(args),
            'kwargs': json.dumps(kwargs)
        }
        request_id = uuid.uuid1().hex
        resp = _session.get(url=end_point, params=params, headers={'x-request-id': request_id})
        logger.info('msg_id={0}|choice_request|{1}'.format(request_id, params))
        try:
            resp = resp.json()
        except:
            resp = {}
        return resp


class ChoiceClient(object):
    def __init__(self, host, port):
        self._caller = ChoiceCaller(''.join(['http://', host, ':', str(port), '/']))

    def __getattr__(self, item):
        if item not in self.__dict__:
            def func(*args, **kwargs):
                as_raw = kwargs.pop('as_raw', False)
                result = self._caller.call(item, *args, **kwargs)
                if result['status']['ecode'] != 0:
                    raise RuntimeError(result['status']['message'])
                result = result['data']
                indicators = result['Indicators']
                if result['ErrorCode'] > 0:
                    raise RuntimeError(result['ErrorCode'], result['ErrorMsg'])
                elif as_raw:
                    return result
                elif result['Dates'] and isinstance(result['Data'], dict):
                    series = {}
                    for code, data in result['Data'].items():
                        for i, v in enumerate(data):
                            s = pd.Series(v, index=result['Dates'])
                            series[(code, indicators[i])] = s
                    return pd.DataFrame.from_dict(series)
                elif isinstance(result['Data'], list):
                    if len(indicators) == len(result['Data']):
                        return pd.DataFrame(data=result['Data'], index=indicators, columns=result['Dates']).T
                    else:
                        return pd.DataFrame(data=result['Data'], columns=indicators)
                elif not result['Dates']:
                    df = pd.DataFrame(result['Data'], index=result['Indicators'])
                    return df.T
                else:
                    raise Exception('unhandle result from choice proxy')
            self.__setattr__(item, func)
            return func

choice_client = ChoiceClient(HOST,PORT)