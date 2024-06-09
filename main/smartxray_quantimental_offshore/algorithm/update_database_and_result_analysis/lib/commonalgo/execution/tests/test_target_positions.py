#!/usr/bin/env python

import os
from os import path
import sys
import unittest
import pandas as pd
import numpy as np


from ..util.target_positions import (calculate_positions_from_weights,
                                     cal_stock_position )
from ..util import target_positions as tp


class TestPositionUtilFunctions(unittest.TestCase):
    def test_cal_stock_position_array(self):
        target_value = np.array([100., 44.4, 51.1])
        price = np.array([1., 2., 25.])
        lot_size = np.array([1, 20, 2], dtype=int)
        answer = np.array([100, 20, 2])
        res = cal_stock_position(target_value, price, lot_size)
        self.assertTrue(np.array_equal(answer, res))

    def test_cal_stock_position_scalar(self):

        target_value = 210.3
        price = 105.1
        lot_size = 2
        answer = 2
        self.assertEqual(answer,
                         cal_stock_position(target_value, price, lot_size))

    def test_fill_positions_with_cash(self):
        sorted_idxs = np.array([2,0,1])
        target_value = np.array([30, 30, 60])
        price = np.array([ 8, 30, 11])
        lot_size = np.array([2, 1, 3])
        est_position = cal_stock_position(target_value, price, lot_size)
        est_value = est_position * price
        cash = np.sum(target_value) - np.sum(est_value)
        ret = tp._fill_positions_with_cash( sorted_idxs, cash, target_value, est_position,
                              est_value, lot_size, price)
        answer = np.array([ 2, 1, 6])
        self.assertTrue(np.array_equal(answer,
                                       ret[1]))

class TestTargetPositions(unittest.TestCase):
    def setUp(self):
        estimation = pd.Series({'target_value': 5005, 'iuid': 1})
        base_path = path.join(path.dirname(
            os.path.abspath(__file__)), 'test_data')
        self.target_weight = pd.read_csv(
            os.path.join(base_path, 'target1.csv'),
            index_col=0,)
        self.rt_data = pd.read_csv(os.path.join(base_path, 'position1.csv'),
                                   index_col=0,
                                   dtype={'PRICE': float,
                                          'ORDER_LOT_SIZE': int,
                                          'current_position': int
                                          })
        self.estimation = estimation
        self.prices = []
        self.targets = []

        for i in range(7):
            self.prices.append(pd.read_csv(os.path.join(base_path, f'position_stock_{i}.csv'),
                                           index_col=0,
                                           dtype={'PRICE': float,
                                                  'ORDER_LOT_SIZE': int,
                                                  'current_position': int}))
            self.targets.append(pd.read_csv(os.path.join(base_path, f'target_stock_{i}.csv'),
                                            index_col=0))

    def test_int_val_position(self):
        current_asset = 1000000.
        target_weight = self.target_weight
        rt = self.rt_data

        ret = calculate_positions_from_weights(
            current_asset,
            self.target_weight,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],)
        self.assertEqual(int, ret.dtypes)

    def test_simple_common(self):
        current_asset = 1000000.
        target_weight = self.target_weight
        rt = self.rt_data

        ret = calculate_positions_from_weights(
            current_asset,
            self.target_weight,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],)
        # golden = pd.Series{'CN_60_IH1907': 20, 'CN_60_IF1907': 67, 'CN_60_IC1907': 458}
        self.assertFalse(ret.isnull().values.any())

    def test_simple_int_asset(self):
        """ Int asset as input
        """
        rt = self.rt_data
        tar = self.target_weight
        current_asset = 1000000

        ret = calculate_positions_from_weights(
            current_asset,
            tar,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],)

        self.assertFalse(ret.isnull().values.any())

    def test_simple_stock_common(self):
        current_asset = 1000000.
        rt = self.prices[0]
        tar = self.targets[0]

        ret = calculate_positions_from_weights(
            current_asset,
            tar,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],)
        self.assertFalse(ret.isnull().values.any())

    def test_simple_stock_asset(self):
         current_asset = 1000000.
         rt = self.prices[0]
         tar = self.targets[0]
         ret = calculate_positions_from_weights(
             current_asset,
             tar,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],)
         self.assertLess(np.sum(rt['PRICE'] * ret), current_asset)
         # self.assertTrue(ret)
    def _fill_cash_position(self):
        current_asset = 120
        rt = self.prices[1]
        tar = self.targets[1]
        ret = calculate_positions_from_weights(
            current_asset,
            tar,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],
            strategy='min_drift',
            minimum_cash_buffer=0,
            percentage_of_cash_buffer=0.,
        )
        answer = pd.Series([ 4, 1, 3], index=tar.index)

        self.assertTrue(answer.equals(ret))

    def test_pre_tar_pos(self):
        tar = self.targets[2]
        rt = self.prices[2]
        current_asset = 240
        answer = rt['min_drift_answer']
        ret = calculate_positions_from_weights(
            current_asset,
            tar,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],
            strategy='min_drift',
            minimum_cash_buffer=0,
            percentage_of_cash_buffer=0.,
        )
        self.assertTrue(answer.equals(ret[answer.index]))

    def test_simple_common(self):
        current_asset = 1000000.
        target_weight = self.targets[2]
        rt = self.prices[2]

        ret = calculate_positions_from_weights(
            current_asset,
            target_weight,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],)
        # golden = pd.Series{'CN_60_IH1907': 20, 'CN_60_IF1907': 67, 'CN_60_IC1907': 458}
        self.assertFalse(ret.isnull().values.any())

    def test_zero_weight(self):
        # TODO
        current_asset = 1000000.
        rt = self.prices[5]
        target_weight = self.targets[5]
        ret = calculate_positions_from_weights(
            current_asset,
            target_weight,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],)
        # golden = pd.Series{'CN_60_IH1907': 20, 'CN_60_IF1907': 67, 'CN_60_IC1907': 458}
        self.assertTrue((ret==0).all())
        ret_iuids = set(rt.index).union(set(target_weight.index))
        self.assertEqual(len(ret_iuids), len(ret))

    def test_fill_min_cash(self):
        tar = self.targets[6]
        rt = self.prices[6]
        current_asset = 570487.01
        # answer = rt['min_drift_answer']
        ret = calculate_positions_from_weights(
            current_asset,
            tar,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],
            strategy='min_drift',
            minimum_cash_buffer=0,
            percentage_of_cash_buffer=0.,
        )
        # import ipdb; ipdb.set_trace()
        self.assertEqual(1, sum(ret == 0))

    def test_all_empty(self):
        tar = self.targets[5]
        tar = pd.DataFrame(columns=(list(tar.columns)+['iuid'])).set_index('iuid')
        rt = self.prices[5]
        
        current_asset = 570487.01
        ret = calculate_positions_from_weights(
            current_asset,
            tar,
            rt['current_position'],
            rt['PRICE'],
            rt['ORDER_LOT_SIZE'],
            strategy='min_drift',
            minimum_cash_buffer=0,
            percentage_of_cash_buffer=0.,
        )
        self.assertTrue((ret==0).all())
        ret_iuids = set(rt.index).union(set(rt.index))
        self.assertEqual(len(ret_iuids), len(ret))
