import unittest

import numpy as np
import numpy.testing as npt
import pandas as pd
from pandas.testing import assert_frame_equal

from quantcycle.app.data_manager.data_processor.method.ccyfx import MethodCCYFX, _construct_ccyfx_df


class MethodCCYFXTestCase(unittest.TestCase):
    def test_create_ccy_data_mapping(self):
        m = MethodCCYFX()
        m.symbols = ['EURCHF', 'EURUSD', 'GBPUSD', 'USDIDR']
        data_bundle = {
            'Symbol': ['EURCHF', 'EURUSD', 'GBPUSD', 'USDIDR'],
            "SymbolArgs": {"DataSource": 'BT150'}
        }

        m.create_ccys_data_mapping(data_bundle)
        assert (m.data_mapping == {
            'EURCHF info': 'EURCHF BT150/info',
            'EURUSD info': 'EURUSD BT150/info',
            'GBPUSD info': 'GBPUSD BT150/info',
            'USDIDR info': 'USDIDR BT150/info'
        })

    def test_create_data_mapping(self):
        m = MethodCCYFX()

        data_bundle = {
            'Symbol': ['EURCHF', 'EURUSD', 'GBPUSD', 'USDIDR'],
            "SymbolArgs": {"DataSource": 'BT150'}
        }

        m.data_mapping = {}
        m.ccys = ['CHF', 'USD', 'USD', 'IDR']
        m.create_data_mapping(data_bundle)

        assert (m.data_mapping == {
            'CHF fxrate': 'USDCHF BT150/fxrate',
            'IDR fxrate': 'USDIDR BT150/fxrate'
        })


class ConstructCcyfxDfTestCase(unittest.TestCase):
    def test_fillna(self):
        symbols = ['EURCHF', 'EURUSD', 'GBPUSD', 'USDIDR']
        data_dict = {
            'CHF fxrate': pd.DataFrame({'close': [4.6, 1.6, 5.7, 5.8]}, index=[1, 2, 3, 4]),
            'IDR fxrate': pd.DataFrame({'close': [1, 4, 1]}, index=[1, 2, 4])
        }
        ccys = ['CHF', 'USD', 'USD', 'IDR']
        rslt = _construct_ccyfx_df(data_dict, ccys, symbols, [], 1)

        ans_df = pd.DataFrame({
            'EURCHF': [1/4.6, 1/1.6, 1/5.7, 1/5.8],
            'EURUSD': [1, 1, 1, 1],
            'GBPUSD': [1, 1, 1, 1],
            'USDIDR': [1, 1/4, 1/4, 1]
        }, index=[1, 2, 3, 4]
        )

        npt.assert_allclose(rslt.values, ans_df.values)