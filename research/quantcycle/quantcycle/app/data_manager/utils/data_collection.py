from collections import defaultdict


class BaseData():
    def __init__(self, symbol=None, data=None, timestamp=None, fields=None, info=None):
        self.symbol = symbol
        self.data = data
        self.info = info
        self.timestamp = timestamp
        self.fields = fields

    def verify(self):
        raise NotImplementedError


class TwoDimData(BaseData):
    '''
        2-D data. First dimension is time, second dimension is fields,
        symbol must be one ticker.
    '''

    def verify(self):
        '''
            verify every attr is correct
        '''
        raise NotImplementedError


class ThreeDimData(BaseData):
    '''
        3-D data. First dimension is time,
        second dimension symbols,
        third dimension is fields
    '''

    def verify(self):
        '''
            verify every attr is correct
        '''
        raise NotImplementedError


def DataCollection():
    '''
        return a defaultdict
    '''
    return defaultdict(TwoDimData)


def update_data(origin, new):
    '''
        add downloaded data into bar data
    '''
    for k, v in new.items():
        true_k, attr = long_key2key_attrs(k)
        # TODO might need a better place to do this checking
        if attr == 'info' and hasattr(origin[true_k], 'info') and origin[true_k].info:
            tmp = origin[true_k].info
            tmp.update(v)
            v=tmp
        setattr(origin[true_k], attr, v)
    return origin


def long_key2key_attrs(k):
    if "/" in k:
        tmp = k.split("/")
        true_k = tmp[0]
        attr = tmp[-1]
    else:
        true_k = k
        attr = "data"
    return true_k, attr
