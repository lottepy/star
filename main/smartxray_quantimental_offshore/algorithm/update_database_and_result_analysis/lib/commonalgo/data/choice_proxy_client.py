import json
import uuid
from logging import getLogger

from requests import session
import pandas as pd
from pandas import MultiIndex, DataFrame
from ..setting.network_utils import C_HOST,C_PORT


_session = session()
logger = getLogger('main')

# HOST = '139.224.234.6'
# PORT = 8394

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
                    mi = MultiIndex.from_product([result['Codes'], indicators])
                    # build multi-index from cartesian product of two lists
                    if item != 'csd':
                        return DataFrame([sum([result['Data'][code] for code in result['Codes']], [])], index=result['Dates'], columns=mi)
                        # sum([[],[]], []) flattens list 1 as in https://stackoverflow.com/a/952946
                        # This is the fastest solution I can find, even faster than https://stackoverflow.com/a/952952
                    else:
                        return DataFrame(list(zip(*sum([result['Data'][code] for code in result['Codes']], []))), index=result['Dates'], columns=mi)
                        # zip(*[[],[]]) transposes a matrix as in https://stackoverflow.com/a/2921713
                        # This is the fastest I can find.
                elif isinstance(result['Data'], list):
                    df = pd.DataFrame(result['Data'])
                    if len(indicators) == len(df):
                        return pd.DataFrame(data=df.T.values, index=result['Dates'],columns=indicators)
                    elif len(indicators) == df.shape[1]:
                        return pd.DataFrame(data=df.values, columns=indicators)
                    else:
                        # parse c.sector result
                        name_list = [x for x in result['Data'] if x not in result['Codes']]
                        return pd.DataFrame(data=[result['Codes'], name_list]).T
                elif not result['Dates']:
                    df = pd.DataFrame(result['Data'], index=result['Indicators'])
                    return df.T
                else:
                    raise Exception('unhandle result from choice proxy')
            self.__setattr__(item, func)
            return func

choice_client = ChoiceClient(C_HOST,C_PORT)