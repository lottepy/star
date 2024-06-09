#!/usr/bin/env python

import os
from os import path
import unittest
import pandas as pd
import numpy as np


from ..strategy import twap


class TestTWAPStatic(unittest.TestCase):
    def setUp(self):
        self.prices = []
        self.targets = []
        base_path = path.join(path.dirname(
            os.path.abspath(__file__)), 'test_data')
        for i in range(5):
            self.prices.append(pd.read_csv(os.path.join(base_path,
                                                        f'position_stock_{i}.csv'),
                                           index_col=0,
                                           dtype={'PRICE': float,
                                                  'ORDER_LOT_SIZE': int,
                                                  'current_position': int}))
            self.targets.append(pd.read_csv(os.path.join(base_path,
                                                         f'target_stock_{i}.csv'),
                                            index_col=0))

    def test_filter_orders(self):
        rt = self.prices[2]
        current_position = rt['current_position']
        idxs = current_position.index
        trading_direction = pd.Series([1,0,-1], idxs[-3:])
        target_position = current_position+np.array(
            [1, -10, 30, 50, 100])
        target_position.rename('target_position', inplace=True)
        positions = pd.concat([current_position, target_position], axis=1)
        positions = twap.TWAPExecutor.filter_orders(positions,
                                                    rt['PRICE'],
                                                    trading_direction,
                                                    20)

        self.assertTrue((idxs[[1,2,3]] == positions.index).all())

    def test_get_flip_orders(self):
        rt = self.prices[2]
        current_position = rt['current_position']
        idxs = current_position.index
        trading_direction = pd.Series([1,0,-1], idxs[-3:])
        target_position = current_position+np.array(
            [1, -50, 30, 0, -100])
        target_position.rename('target_position', inplace=True)
        positions = pd.concat([current_position, target_position], axis=1)
        diff1, diff2 = twap.TWAPExecutor.get_flip_orders(positions)
        self.assertEqual(2, len(diff2))
        self.assertEqual(4, len(diff1))
        positions = pd.concat([positions, diff1, diff2], axis=1).fillna(0)
        cal_targ = (current_position + positions.iloc[:, -1] + positions.iloc[:, -2])
        cal_targ = target_position[target_position.index]
        self.assertTrue((target_position == cal_targ).all())
