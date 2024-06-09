import glob
import os
from unittest import TestCase

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from quantcycle.app.data_manager import DataManager
from quantcycle.app.data_manager.data_processor import DataProcessor
from quantcycle.app.data_manager.utils.data_collection import update_data


class TestDataProcessorFxOutput(TestCase):

    def test_fx_daily_data_handle_data_function(self):

        dic = {}

        columns = ['timestamp', 'open', 'high', 'low', 'close']
        data = [[1577836800, 0.9142, 0.9148, 0.9102, 0.9115], [1577923200, 0.9144,
                                                               0.9144, 0.9091, 0.9093],       [1578009600, 0.9093, 0.9093, 0.9046, 0.9046]]
        dic['AUDCAD BT150/data'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')
        data = [[1577836800, 71.5, 71.62, 71.37, 71.5], [1577923200, 71.52, 71.55, 71.32, 71.53],
                [1578009600, 71.53, 71.94, 71.43, 71.9]]
        dic['USDINR BT150/data'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')
        data = [[1577836800, 0.7006, 0.703, 0.6986, 0.7016], [1577923200, 0.7008, 0.7024, 0.6996, 0.7],
                [1578009600, 0.7, 0.7002, 0.696, 0.6962]]
        dic['AUDUSD BT150/fxrate'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')
        data = [[1577836800, 1.3048, 1.3076, 1.2962, 1.299], [1577923200, 1.305, 1.305, 1.2973, 1.299],
                [1578009600, 1.299, 1.3, 1.2978, 1.2994]]
        dic['USDCAD BT150/fxrate'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')
        data = [[1577836800, 71.5, 71.62, 71.37, 71.5], [1577923200, 71.52, 71.55, 71.32, 71.53],
                [1578009600, 71.53, 71.94, 71.43, 71.9]]
        dic['USDINR BT150/fxrate'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')

        columns = ['timestamp', 'close']
        data = [[1577836800, -0.229], [1577923200, -0.16], [1578009600, -0.035]]
        dic['CAD TN/data'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')
        data = [[1577836800, 21.52], [1577923200, 20.28], [1578009600, 22.7]]
        dic['INR TN/data'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')
        data = [[1577836800, 1.555], [1577923200, 1.575], [1578009600, 1.575]]
        dic['USD INT/data'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')
        data = [[1577836800, 0.19], [1577923200, 0.7], [1578009600, 0.15]]
        dic['AUD TN/data'] = pd.DataFrame(
            data=data, columns=columns).set_index('timestamp')

        data_manager = DataManager()
        data_manager.prepare()

        update_data(data_manager.data_processor.data_collection, dic)
        data_manager.data_processor.data_bundles.append({'Actions': 'STACK', 'DataCenter': 'DataMaster', 'EndTS': 1579046400, 'Fields': 'OHLC',
                                                                   'Frequency': 'DAILY', 'Label': 'FX/MAIN', 'StartTS': 1577836800, 'Symbol': ['AUDCAD', 'USDINR'], 'SymbolArgs': {'DataSource': 'BT150'}, 'Type': 'Process'})
        data_manager.data_processor.data_bundles.append({'Actions': 'INT', 'DataCenter': 'DataMaster', 'EndTS': 1579046400, 'Fields': 'INT', 'Frequency': 'DAILY',
                                                                  'Label': 'FX/INT', 'StartTS': 1577836800, 'Symbol': ['AUDUSD', 'USDCAD', 'USDINR'], 'SymbolArgs': {'DataSource': 'INT', 'UnderlyingSource': 'BT150'}, 'Type': 'Process'})

        # step 1 in run

        data_manager.data_processor._handle_data()

        columns = ['AUDUSD', 'USDCAD', 'USDINR']
        index = [1577836800, 1577923200]
        values = np.array([[-0.000100,  0.000012317, -0.000284],
                           [-2.15455329e-05,  2.69355087e-06, -1.05238758e-04]])
        df = pd.DataFrame(data=values, index=index, columns=columns)
        df.index.name = 'timestamp'
        df_interest = df

        columns = ['open', 'high', 'low', 'close',
                   'open', 'high', 'low', 'close']
        index = [1577836800, 1577923200, 1578009600]
        values = np.array([[0.9142, 0.9148, 0.9102, 0.9115, 71.5, 71.62, 71.37, 71.5],
                           [0.9144, 0.9144, 0.9091, 0.9093,
                               71.52, 71.55, 71.32, 71.53],
                           [0.9093, 0.9093, 0.9046, 0.9046, 71.53, 71.94, 71.43, 71.9]])
        df = pd.DataFrame(data=values, index=index, columns=columns)
        df.index.name = 'timestamp'
        df_main_OHLC = df

        assert_frame_equal(
            df_interest, data_manager.data_processor.dict_data_array['FX/INT'][0], check_less_precise=2)
        assert_frame_equal(
            df_main_OHLC, data_manager.data_processor.dict_data_array['FX/MAIN'][0], check_less_precise=8)

    def test_fx_daily_data_convert_df_to_numpy_function(self):

        data_manager = DataManager()
        data_manager.prepare()

        data_manager.data_processor.data_bundles.append({'Actions': 'STACK', 'DataCenter': 'DataMaster', 'EndTS': 1579046400, 'Fields': 'OHLC',
                                                                   'Frequency': 'DAILY', 'Label': 'FX/MAIN', 'StartTS': 1577836800, 'Symbol': ['AUDCAD', 'USDINR'], 'SymbolArgs': {'DataSource': 'BT150'}, 'Type': 'Process'})

        columns = ['open', 'high', 'low', 'close',
                   'open', 'high', 'low', 'close']
        index = [1577836800, 1577923200, 1578009600]
        values = np.array([[0.9142, 0.9148, 0.9102, 0.9115, 71.5, 71.62, 71.37, 71.5],
                           [0.9144, 0.9144, 0.9091, 0.9093,
                               71.52, 71.55, 71.32, 71.53],
                           [0.9093, 0.9093, 0.9046, 0.9046, 71.53, 71.94, 71.43, 71.9]])

        df = pd.DataFrame(data=values, index=index, columns=columns)
        df.index.name = 'timestamp'
        df_main_OHLC = df

        data_manager.data_processor.dict_data_array['FX/MAIN'] = (df_main_OHLC, ['AUDCAD', 'USDINR'], ['open', 'high', 'low', 'close'])

        # step 2 in run
        data_manager.data_processor._convert_df_to_np()

        np_fx_interest = np.array([[[0.9142, 0.9148, 0.9102, 0.9115],
                                    [71.5, 71.62, 71.37, 71.5]],
                                   [[0.9144, 0.9144, 0.9091, 0.9093],
                                    [71.52, 71.55, 71.32, 71.53]],
                                   [[0.9093, 0.9093, 0.9046, 0.9046],
                                    [71.53, 71.94, 71.43, 71.9]]])

        np_time_array = np.array([[1577836800, 1577836800,          2,       2020,          1,
                                   1,          0,          0,          0],
                                  [1577923200, 1577923200,          3,       2020,          1,
                                   2,          0,          0,          0],
                                  [1578009600, 1578009600,          4,       2020,          1,
                                   3,          0,          0,          0]])

        assert (np_fx_interest ==
                data_manager.data_processor.dict_data_array['FX/MAIN']['data_arr']).all()
        assert (np_time_array ==
                data_manager.data_processor.dict_data_array['FX/MAIN']['time_arr']).all()
