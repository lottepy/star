from unittest import TestCase

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from quantcycle.app.data_manager.data_processor.method.append_window_data import MethodAPPEND

old_df = pd.DataFrame(data=[[1, 2, 3],
                            [4, 4, 6],
                            [7, 8, 9]],
                      index=[1, 2, 3])
append_df = pd.DataFrame(data=[[100, 2, 3],
                               [4, 5, 6],
                               [10, 11, 12]],
                         index=[4, 5, 6])


class CorrectnessTestCase(TestCase):
    def _test_all(self):
        m = MethodAPPEND()
        m.symbols = ["AUDCAD"]
        m.data_dict = {"AUDCAD old": old_df, "AUDCAD append_df": append_df}
        m.slot = 'data'
        m.data_source = 'BT150'
        df = m.run()

        ans_df = pd.DataFrame(data=[[1, 2, 3],
                                    [4, 4, 6],
                                    [7, 8, 9],
                                    [100, 2, 3],
                                    [4, 5, 6],
                                    [10, 11, 12]],
                              index=[1, 2, 3, 4, 5, 6])
        ans = {'AUDCAD BT150/data': ans_df}
        assert (ans.keys() == df.keys())
        assert_frame_equal(df['AUDCAD BT150/data'], ans['AUDCAD BT150/data'])
