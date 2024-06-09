import os
import unittest

import numpy as np
import pandas as pd
from quantcycle.utils.production_constant import InstrumentType, OrderStatus,OrderFeedback
from quantcycle.utils.production_helper import WindowDataGenerator,get_symbol2ccy_matrix

class TestHelper(unittest.TestCase):

    
    def test_get_symbol2ccy_matrix_01(self):
        symbols = ["AUDUSD.FX","AUDCAD.FX","AUDJPY.FX"]
        symbol2ccy_dict = {"AUDUSD.FX":"USD","AUDCAD.FX":"CAD","AUDJPY.FX":"JPY"}
        ccy_matrix = get_symbol2ccy_matrix(symbols,symbol2ccy_dict)
        target_ccy_matrix_df = np.array([[1., 0., 0.],
                                        [0., 1., 0.],
                                        [0., 0., 1.]])
        target_ccy_matrix_df = pd.DataFrame(target_ccy_matrix_df,columns=["AUDCAD.FX","AUDJPY.FX","AUDUSD.FX"],index = ["CAD","JPY","USD"])
        assert (ccy_matrix == target_ccy_matrix_df ).values.all()