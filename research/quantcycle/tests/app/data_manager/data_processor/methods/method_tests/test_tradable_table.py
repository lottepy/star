import unittest

import numpy as np
import numpy.testing as npt
import pandas as pd
from pandas.testing import assert_frame_equal

from quantcycle.app.data_manager.data_processor.method.tradable_table import MethodTRADABLE
from quantcycle.utils.time_manipulation import timestamp2datetimestring


class MethodTradableDailyTestCase(unittest.TestCase):
    def test_tradable_table_daily(self):
        m = MethodTRADABLE()
        m.time = [1580428800, 1580860800]
        m.end_ts = 1580860800
        m.settlement_type = ''
        
        exchange_calendar_map = {'HKEX':['2020-02-01','2020-02-02','2020-02-04'],'NASDAQ':['2020-02-01','2020-02-03','2020-02-04']}
        all_exchanges = ['HKEX', 'NASDAQ']
        exchange2sym = ['HKEX', 'NASDAQ', 'HKEX']
        trade_time_map = {'HKEX':['09:30-12:00','13:00-16:00'],'NASDAQ':['09:30-16:00'], 'SZSE':['9:30-11:30','13:00-14:57']}
        time_zone_map = {'HKEX':'Asia/Hong_Kong','NASDAQ':'US/Eastern', 'SZSE':'Asia/Shanghai'}
        res = m._daily(exchange_calendar_map, all_exchanges, exchange2sym,trade_time_map, time_zone_map)
        res.index = timestamp2datetimestring(res.index)
        
        assert (res.values == np.array([[2., 2., 2.],
                [2., 2., 2.],
                [1., 2., 1.],
                [2., 1., 2.],
                [2., 2., 2.],
                [1., 2., 1.],
                [2., 2., 2.],
                [2., 1., 2.],
                [2., 2., 2.],
                [1., 2., 1.],
                [2., 1., 2.],
                [2., 2., 2.]])).all()
        assert (res.index == np.array(['2020-01-31 00:00:00 UTC', '2020-02-01 00:00:00 UTC',
            '2020-02-01 08:00:00 UTC', '2020-02-01 21:00:00 UTC',
                '2020-02-02 00:00:00 UTC', '2020-02-02 08:00:00 UTC',
                '2020-02-03 00:00:00 UTC', '2020-02-03 21:00:00 UTC',
                '2020-02-04 00:00:00 UTC', '2020-02-04 08:00:00 UTC',
                '2020-02-04 21:00:00 UTC', '2020-02-05 00:00:00 UTC'])).all()

class MethodTradableIntradayTestCase(unittest.TestCase):
    def test_tradable_table_intraday(self):
        m = MethodTRADABLE()
        m.time = [1580428800, 1580860800]
        m.end_ts = 1580860800

        exchange_calendar_map = {'HKEX':['2020-02-01','2020-02-02','2020-02-04'],'NASDAQ':['2020-02-01','2020-02-03','2020-02-04'], 'SZSE':['2020-02-01','2020-02-03','2020-02-04']}
        trade_time_map = {'HKEX':['09:30-12:00','13:00-16:00'],'NASDAQ':['09:30-16:00'], 'SZSE':['9:30-11:30','13:00-14:57']}
        time_zone_map = {'HKEX':'Asia/Hong_Kong','NASDAQ':'US/Eastern', 'SZSE':'Asia/Shanghai'}
        all_exchanges = ['HKEX', 'NASDAQ', 'SZSE']
        exchange2sym = ['SZSE', 'NASDAQ', 'HKEX']
        

        res = m._intraday(exchange_calendar_map, all_exchanges, exchange2sym, trade_time_map, time_zone_map)
        res.index = pd.to_datetime(timestamp2datetimestring(res.index))

        
        assert (res.loc[pd.to_datetime("2020-02-01 00:00")] == np.array([2., 2., 2.])).all()
        assert (res.loc[pd.to_datetime("2020-02-02 00:00")] == np.array([2., 2., 2.])).all()
        assert (res.loc[pd.to_datetime("2020-02-03 00:00")] == np.array([2., 2., 2.])).all()
        assert (res.loc[pd.to_datetime("2020-02-04 00:00")] == np.array([2., 2., 2.])).all()

        assert (res.loc[pd.to_datetime("2020-02-01 20:30")] == np.array([2., 1., 2.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 21:00")] == np.array([2., 1., 2.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 21:00:01")] == np.array([2., 2., 2.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 21:30")] == np.array([2., 2., 2.])).all()

        assert (res.loc[pd.to_datetime("2020-02-01 03:30")] == np.array([1., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 04:00")] == np.array([2., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 04:00:01")] == np.array([2., 2., 2.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 04:30")] == np.array([2., 2., 2.])).all()

        assert (res.loc[pd.to_datetime("2020-02-01 07:30")] == np.array([2., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 08:00")] == np.array([2., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 08:00:01")] == np.array([2., 2., 2.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 08:30")] == np.array([2., 2., 2.])).all()

        assert (res.loc[pd.to_datetime("2020-02-01 03:00")] == np.array([1., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 03:30")] == np.array([1., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 03:30:01")] == np.array([2., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 04:00")] == np.array([2., 2., 1.])).all()

        assert (res.loc[pd.to_datetime("2020-02-01 06:30")] == np.array([1., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 06:57")] == np.array([1., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 06:57:01")] == np.array([2., 2., 1.])).all()
        assert (res.loc[pd.to_datetime("2020-02-01 07:00")] == np.array([2., 2., 1.])).all()
