from unittest import TestCase, mock

import numpy as np
import pandas as pd
import pytest

from quantcycle.app.data_manager.data_processor.method.backup import MethodBACKUP

old_df = pd.DataFrame(data=[[1, 2, np.nan],
                            [4, np.nan, 6],
                            [7, 8, 9]],
                      index=[1, 2, 3])
new_df = pd.DataFrame(data=[[100, 2, 3],
                            [4, np.nan, 6],
                            [10, 11, 12]],
                      index=[1, 2, 4])


class CorrectnessTestCase(TestCase):
    def test_all(self):
        m = MethodBACKUP()
        m.symbols = ["AUDCAD"]
        m.data_dict = {"AUDCAD old": old_df, "AUDCAD new": new_df}
        m.slot = 'new_slot'
        m.new_data_source = 'new_data_source'
        rslt = m.run()
        # test key
        assert "AUDCAD new_data_source/new_slot" in rslt
        # test shape
        df = rslt["AUDCAD new_data_source/new_slot"]
        assert df.shape == (4, 3)
        # test index
        # for data that only exist in new_df, shouldn't be added to old
        assert np.array_equal(df.index, [1, 2, 3, 4])
        # test correctly fill
        assert df.iloc[0, 2] == 3
        assert df.iloc[3, 0] == 10
        # test no nan value will not be overwrite
        assert df.iloc[0, 0] == 1
        # test nan in both data frame remain
        assert np.isnan(df.iloc[1, 1])
