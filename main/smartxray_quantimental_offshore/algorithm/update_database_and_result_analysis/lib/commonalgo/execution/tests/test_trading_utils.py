#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest

from lib.commonalgo.execution.util import trading_utils


class TestConvert(unittest.TestCase):
    def test_process_ctp_arbitrage_contract(self):
        # 普通
        result_1 = trading_utils.process_ctp_arbitrage_contract([{
            'iuid': 'CN_60_m1909',
            'holdingPosition': 10
        }, {
            'iuid': 'CN_60_ab1909',
            'holdingPosition': 3
        }])
        expect_1 = [{'iuid': 'CN_60_m1909', 'holdingPosition': 10}, {'iuid': 'CN_60_ab1909', 'holdingPosition': 3}]
        self.assertEqual(result_1, expect_1)

        # 忽略全部
        result_2 = trading_utils.process_ctp_arbitrage_contract([{
            'iuid': 'CN_60_SPC m1909&a1909',
            'holdingPosition': 10
        }, {
            'iuid': 'CN_60_SPC a1909&m1909',
            'holdingPosition': 10
        }])
        expect_2 = []
        self.assertEqual(result_2, expect_2)

        # 忽略后者
        result_3 = trading_utils.process_ctp_arbitrage_contract([{
            'iuid': 'CN_60_m1909',
            'holdingPosition': 100
        }, {
            'iuid': 'CN_60_SPC a1909&m1909',
            'holdingPosition': 1
        }])
        expect_3 = [{'iuid': 'CN_60_m1909', 'holdingPosition': 100}]
        self.assertEqual(result_3, expect_3)

        # 抵消一部分 反过来
        result_4 = trading_utils.process_ctp_arbitrage_contract([{
            'iuid': 'CN_60_SPC a1909&m1909',
            'holdingPosition': -1
        }, {
            'iuid': 'CN_60_m1909',
            'holdingPosition': 100
        }])
        expect_4 = [{'iuid': 'CN_60_m1909', 'holdingPosition': 100}]
        self.assertEqual(result_4, expect_4)

if __name__ == '__main__':
    unittest.main()
