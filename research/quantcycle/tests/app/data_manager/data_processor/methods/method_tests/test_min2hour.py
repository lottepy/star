from unittest import TestCase

import pandas as pd
import numpy as np
from pandas.testing import assert_frame_equal
import pytest

from quantcycle.app.data_manager.data_processor.method.min2hour import MethodMIN2HOUR

index = pd.date_range('1/1/2000 09:30:00', periods=91, freq='1T')
old_df = pd.DataFrame(index = index)
old_df['open'] = 1.0
old_df['high'] = 1.0
old_df['low'] = 1.0
old_df['close'] = 1.0
old_df.iloc[3, :] = 0.1
old_df.iloc[-2, :] = 9
old_df.iloc[30, :] = 5
old_df.iloc[40, :] = np.nan
old_df.index = old_df.index.values.astype(np.int64) // 10 ** 9


class Min2hourTestCase(TestCase):
    def test_all(self):
        m = MethodMIN2HOUR()
        m.symbols = ["AUDCAD"]
        m.data_dict = {"AUDCAD old": old_df}
        m.slot = 'data'
        m.data_source = 'BT150'
        dict_df = m.run()

        index = [946717200, 946720800, 946724400]
        data = [[1.0, 1.0, 0.1, 1.0],
                [5.0, 9.0, 1.0, 9.0],
                [1.0, 1.0, 1.0, 1.0]]
        ans_df = pd.DataFrame(data=data, index = index)
        ans_df.columns = ['open', 'high', 'low', 'close']
        ans = {'AUDCAD BT150/data hour': ans_df}
        assert (ans.keys() == dict_df.keys())
        assert_frame_equal(dict_df['AUDCAD BT150/data hour'], ans['AUDCAD BT150/data hour'])

