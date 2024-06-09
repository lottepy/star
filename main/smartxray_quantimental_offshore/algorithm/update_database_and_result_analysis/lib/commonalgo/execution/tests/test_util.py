#!/usr/bin/env python


import os
from os import path
import sys
import unittest
import pandas as pd
import numpy as np

from ..util import trading_utils as tu


class TestPositionUtilFunctions(unittest.TestCase):
    def setUp(self):
        base_path = path.join(path.dirname(
            os.path.abspath(__file__)), 'test_data')
        self.ts3 = pd.read_csv(
            os.path.join(base_path, 'target_stock_2.csv'),
            index_col=0,)
    def test_get_df_col_without_nan1(self):
        df = self.ts3
        col = 'target_position'
        ans = df[col].iloc[3:]
        ret = tu.get_df_col_without_nan(df, col)
        self.assertTrue(ans.equals(ret))

    def test_get_df_col_without_nan2(self):
        df = self.ts3
        col = 'target_weight'
        ans = df[col].iloc[:3]
        ret = tu.get_df_col_without_nan(df, col)
        self.assertTrue(ans.equals(ret))

    def test_get_df_col_without_nan3(self):
        df = self.ts3
        col = 'symbol'
        ret = tu.get_df_col_without_nan(df, col)
        self.assertTrue(ret.empty)

