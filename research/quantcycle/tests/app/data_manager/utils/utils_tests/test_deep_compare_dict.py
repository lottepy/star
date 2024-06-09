from unittest import TestCase, mock

import numpy as np
import pytest

from quantcycle.app.data_manager.utils.update_nested_dict import deep_compare_dict


class DeepCompareDictTestCase(TestCase):
    def test_true_1(self):
        d1 = {'s': ['a','b'], '1': 'b'} 
        d2 = {'1': 'b', 's': ['b','a','a']} 
        assert deep_compare_dict(d1, d2)

    def test_true_2(self):
        d1 = {'s': ['a','b'], '1': 'b'} 
        d2 = {'1': 'b', 's': ['b','a']} 
        assert deep_compare_dict(d1, d2)    
    
    def test_true_3(self):
        d1 = {'s': ['a',2], '1': 2.3}
        d2 = {'1': 2.3, 's': [2,'a']} 
        assert deep_compare_dict(d1, d2)

    def test_false_1(self):
        d1 = {'s': ['a','b'], '1': 'b'} 
        d2 = {'1': 'b', 's': ['b','a','c']} 
        assert deep_compare_dict(d1, d2) == False

    def test_false_2(self):
        d1 = {'s': '1', '1': 'b'} 
        d2 = {'1': '2', 's': '3', '2': '2'} 
        assert deep_compare_dict(d1, d2) == False
