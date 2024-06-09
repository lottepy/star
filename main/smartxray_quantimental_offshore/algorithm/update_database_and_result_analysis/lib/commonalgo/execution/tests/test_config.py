import os
from os import path
import unittest
import pandas as pd

from ..config.loader import target_file_validation
from ..config import loader
from ..config import constant


class TestTargetFile(unittest.TestCase):
    def setUp(self):
        self.prices = []
        self.targets = []

        base_path = path.join(path.dirname(
            os.path.abspath(__file__)), 'test_data')
        for i in range(5):
            self.prices.append(pd.read_csv(os.path.join(base_path, f'position_stock_{i}.csv'),
                                           index_col=0,
                                           dtype={'PRICE': float,
                                                  'ORDER_LOT_SIZE': int,
                                                  'current_position': int}))
            self.targets.append(pd.read_csv(os.path.join(base_path, f'target_stock_{i}.csv'),
                                            index_col=0))

    def test_targetFileValidation0(self):
        target, price = self.targets[0], self.prices[0]
        self.assertTrue(target_file_validation(target, 200, price['PRICE']))

    def test_targetFileValidation1(self):
        target, price = self.targets[1], self.prices[1]
        self.assertTrue(target_file_validation(target, 200, price['PRICE']))

    def test_targetFileValidation2(self):
        target, price = self.targets[2], self.prices[2]
        self.assertTrue(target_file_validation(target, 240, price['PRICE']))

    def test_targetFileValidation3(self):
        target, price = self.targets[3], self.prices[3]
        with self.assertRaises(ValueError) as ast:
            target_file_validation(target, 240, price['PRICE'])

    def test_target_get_del_line(self):
        target = self.targets[4]
        true_val = self.targets[3]
        ret, val = loader.target_get_del_add_value(target)
        self.assertTrue(true_val.equals(ret))

    def test_target_add_value_invariant(self):
        target = self.targets[3]
        true_val = self.targets[3].copy()
        ret, val = loader.target_get_del_add_value(target)
        self.assertTrue(true_val.equals(ret))

    def test_target_get_add_value(self):
        target = self.targets[4]
        true_val = target.loc[constant.CASH_IUID, 'target_position']
        ret, val = loader.target_get_del_add_value(target)
        self.assertEqual(true_val, val)
