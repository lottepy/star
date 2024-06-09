import unittest

import pandas as pd

from quantcycle.app.data_manager.data_processor.method.calculate_interest_rate import \
    _ccys_interest_rate


class TestDataProcessor(unittest.TestCase):
    def test_cal_interest_rate_three_ccys(self):
        list_df = []
        for i in range(3):
            list_df.append(pd.read_csv(
                f'tests/app/data_manager/data_processor/methods/method_tests/csv_for_test/in{i}.csv', index_col=0))
        a = _ccys_interest_rate(['JPY', 'SEK', 'EUR'], list_df)
        b = pd.read_csv(
            'tests/app/data_manager/data_processor/methods/method_tests/csv_for_test/out.csv', index_col=0)
        assert all(a.round(3) == b.round(3))

    def test_cal_interest_rate_TN(self):
        list_df = []
        list_df.append(pd.read_csv(
            f'tests/app/data_manager/data_processor/methods/method_tests/csv_for_test/input_cal_interest_rate_TN.csv', index_col=0))
        a = _ccys_interest_rate(['EUR'], list_df)
        b = pd.read_csv(
            'tests/app/data_manager/data_processor/methods/method_tests/csv_for_test/result_cal_interest_rate_test_TN.csv', index_col=0)
        assert all(a.round(3) == b.round(3))

    def test_cal_interest_rate_FP(self):
        list_df = []
        list_df.append(pd.read_csv(
            f'tests/app/data_manager/data_processor/methods/method_tests/csv_for_test/input_cal_interest_rate_FP.csv', index_col=0))
        a = _ccys_interest_rate(['EUR'], list_df)
        b = pd.read_csv(
            'tests/app/data_manager/data_processor/methods/method_tests/csv_for_test/result_cal_interest_rate_test_FP.csv', index_col=0)
        assert all(a.round(3) == b.round(3))
