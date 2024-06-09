#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import sys
import unittest

from ..symbol.base import symbol_converter

testcases = """
CN_60_rb1910
CN_60_au1912
CN_60_ag1912
CN_60_cu1908
CN_60_ni1908
CN_60_al1908
CN_60_zn1908
CN_60_ru1909
CN_60_bu1912
CN_60_hc1910
CN_60_pb1908
CN_60_i1909
CN_60_m1909
CN_60_p1909
CN_60_y1909
CN_60_l1909
CN_60_v1909
CN_60_pp1909
CN_60_j1909
CN_60_jm1909
CN_60_a1909
CN_60_c1909
CN_60_jd1909
CN_60_cs1909
CN_60_SR1909
CN_60_RM1909
CN_60_CF1909
CN_60_TA1909
CN_60_MA1909
CN_60_ZC1909
CN_60_FG1909
CN_60_OI1909
CN_60_AP1910
CN_60_IH1907
CN_60_IC1907
CN_10_600000
CN_10_000001
""".split()

class TestConvert(unittest.TestCase):
    def test_iuid_choice_convert(self):
        """ 输入IUID 先把它转为choice 再转回IUID 比较前后的IUID是否相等 """
        for iuid in testcases:
            choice_symbol = symbol_converter.from_iuid_to_choicesymbol(iuid)
            iuid_new = symbol_converter.from_choicesymbol_to_iuid(choice_symbol)

            self.assertEqual(iuid, iuid_new)

if __name__ == '__main__':
    unittest.main()
