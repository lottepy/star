'''
    This test is set for testing the importabilities of different modules
'''
import pytest
from quantcycle.app.data_manager import DataManager, DataDistributorSub

def test_import_data_manager():
    data_manager = DataManager()
    assert True

def test_import_data_distributor_sub():
    dds = DataDistributorSub()
    assert True