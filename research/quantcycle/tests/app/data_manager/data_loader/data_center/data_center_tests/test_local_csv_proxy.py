import pandas as pd
import pytest
import numpy.testing as npt
from unittest import TestCase, mock

from quantcycle.app.data_manager.utils.handle_data_bundle import \
    download_data_bundle
from quantcycle.app.data_manager.utils.samples import *

from quantcycle.app.data_manager.data_loader.data_center.local_csv.proxy import LocalCSV

 
class LocalCSVProxyTestCase(TestCase):
    def test_load_data_from_csv_with_LocalCSV(self):
        localcsv = LocalCSV()

        proxies = {}
        data = download_data_bundle(sample_data_bundle_csv_proxy, proxies)

        assert set(data.keys()) == set(['coinA coin_source1/info', 'coinB coin_source_from_config/info', 'coinA coin_source1/data', 'coinB coin_source_from_config/data'])
        npt.assert_almost_equal(data['coinA coin_source1/data'].values[0,:], [ 1.973, 73.382, 72.045, 72.201], decimal=3)
        npt.assert_almost_equal(data['coinB coin_source_from_config/data'].values[0,:], [2.006, 73.104, 72.702, 72.972], decimal=3)
        assert data['coinA coin_source1/info'] == {'bloomberg_code': 'bbg_code_coinA', 'csv_name': 'ASSET_A', 'list_exchange': 'ALL_TRADABLE', 'source': 'coin_source1', 'symbol': 'coinA', 'symboltype': 'crypto'.upper(), 'timezone': 'Asia/Hong_Kong', 'trade_time': ['00:00-00:00'], 'trading_currency': 'USD'}
        assert data['coinB coin_source_from_config/info'] == {'csv_name': 'ASSET_B', 'list_exchange': 'ALL_TRADABLE', 'symbol': 'coinB', 'timezone': 'UTC', 'trade_time': ['00:00-00:00'], 'trading_currency': 'USD'}

    def test_csv_no_symbol_info(self):
        localcsv = LocalCSV()

        proxies = {}
        data = download_data_bundle(sample_data_bundle_csv_proxy_no_coinB_info, proxies)

        assert set(data.keys()) == set(['coinA coin_source1/info', 'coinB coin_source_from_config/info', 'coinA coin_source1/data', 'coinB coin_source_from_config/data'])
        npt.assert_almost_equal(data['coinA coin_source1/data'].values[0,:], [1.006, 73.104, 72.702, 72.972], decimal=3)
        assert data['coinB coin_source_from_config/data'].empty
        assert data['coinA coin_source1/info'] == {'bloomberg_code': 'bbg_code_coinA', 'csv_name': 'ASSET_A', 'list_exchange': 'ALL_TRADABLE', 'source': 'coin_source1', 'symbol': 'coinA', 'symboltype': 'crypto'.upper(), 'timezone': 'UTC', 'trade_time': ['00:00-00:00'], 'trading_currency': 'USD'}
        assert data['coinB coin_source_from_config/info'] == {'list_exchange': 'ALL_TRADABLE', 'symbol': 'coinB coin_source_from_config', 'timezone': 'UTC', 'trade_time': ['00:00-00:00']}

    def test_csv_no_symbol_info_no_input_symbols(self):
        localcsv = LocalCSV()

        proxies = {}
        data = download_data_bundle(sample_data_bundle_csv_proxy_no_coinB_info_no_symbols_list, proxies)

        assert set(data.keys()) == set(['ASSET_B coin_source_from_config/info', 'ASSET_C coin_source_from_config/info', 'coinA coin_source1/info', 'coinZ coin_source_from_config/info', 'ASSET_B coin_source_from_config/data', 'ASSET_C coin_source_from_config/data', 'coinA coin_source1/data', 'coinZ coin_source_from_config/data'])
        npt.assert_almost_equal(data['coinA coin_source1/data'].values[0,:], [1.006, 73.104, 72.702, 72.972], decimal=3)
        npt.assert_almost_equal(data['coinZ coin_source_from_config/data'].values[0,:], [7.006, 73.104, 72.702, 72.972], decimal=3)
        npt.assert_almost_equal(data['ASSET_B coin_source_from_config/data'].values[0,:], [2.006, 73.104, 72.702, 72.972], decimal=3)
        npt.assert_almost_equal(data['ASSET_C coin_source_from_config/data'].values[0,:], [3.006, 73.104, 72.702, 72.972], decimal=3)
        assert data['coinA coin_source1/info'] == {'bloomberg_code': 'bbg_code_coinA', 'csv_name': 'ASSET_A', 'source': 'coin_source1', 'symbol': 'coinA', 'symboltype': 'crypto'.upper(), 'trading_currency': 'USD', 'timezone': 'UTC', 'trade_time': ['00:00-00:00'], 'list_exchange': 'ALL_TRADABLE'}
        assert data['coinZ coin_source_from_config/info'] == {'list_exchange': 'ALL_TRADABLE', 'symbol': 'coinZ coin_source_from_config', 'timezone': 'UTC', 'trade_time': ['00:00-00:00']}
        assert data['ASSET_B coin_source_from_config/info'] == {'list_exchange': 'ALL_TRADABLE', 'symbol': 'ASSET_B coin_source_from_config', 'timezone': 'UTC', 'trade_time': ['00:00-00:00']}
        assert data['ASSET_C coin_source_from_config/info'] == {'list_exchange': 'ALL_TRADABLE', 'symbol': 'ASSET_C', 'timezone': 'UTC', 'trade_time': ['00:00-00:00']}

    def test_csv_hourly(self):
        localcsv = LocalCSV()

        proxies = {}
        data = download_data_bundle(sample_data_bundle_csv_proxy_hourly, proxies)

        assert set(data.keys()) == set(['coinA coin_source1/info', 'coinB coin_source_from_config/info', 'coinA coin_source1/data', 'coinB coin_source_from_config/data'])
        assert data['coinA coin_source1/data'].values.size == 0
        npt.assert_almost_equal(data['coinB coin_source_from_config/data'].values[0,:], [2.006, 73.104, 72.702, 72.972], decimal=3)
        npt.assert_almost_equal(data['coinB coin_source_from_config/data'].index.values, [1569805200, 1569808800, 1569812400], decimal=3)
        assert data['coinA coin_source1/info'] == {'bloomberg_code': 'bbg_code_coinA', 'csv_name': 'ASSET_A', 'list_exchange': 'ALL_TRADABLE', 'source': 'coin_source1', 'symbol': 'coinA', 'symboltype': 'crypto'.upper(), 'timezone': 'Asia/Hong_Kong', 'trade_time': ['00:00-00:00'], 'trading_currency': 'USD'}
        assert data['coinB coin_source_from_config/info'] == {'csv_name': 'ASSET_B', 'symbol': 'coinB', 'trading_currency': 'USD', 'timezone': 'UTC', 'trade_time': ['00:00-00:00'], 'list_exchange': 'ALL_TRADABLE'}

    def test_csv_no_field(self):
        localcsv = LocalCSV()

        proxies = {}
        data = download_data_bundle(sample_data_bundle_csv_no_field, proxies)

        assert set(data.keys()) == set(['AUDCAD BT150/info', 'AUDCAD BT150/data'])
        npt.assert_almost_equal(data['AUDCAD BT150/data'].values[0,:], [-1.326e-04, -3.262e-05, -3.262e-05], decimal=3)
        npt.assert_almost_equal(data['AUDCAD BT150/data'].index.values, [1569801600, 1569888000, 1569974400], decimal=3)

    def test_csv_no_info_dir(self):
        localcsv = LocalCSV()

        proxies = {}
        data = download_data_bundle(sample_data_bundle_csv_no_info, proxies)

        assert set(data.keys()) == set(['AUDCAD coin_source_from_config/info', 'AUDCAD coin_source_from_config/data'])
        npt.assert_almost_equal(data['AUDCAD coin_source_from_config/data'].values[0,:], [-1.326e-04], decimal=3)
        npt.assert_almost_equal(data['AUDCAD coin_source_from_config/data'].index.values, [1569801600, 1569888000, 1569974400], decimal=3)

    def _test_csv_from_fxrate_to_symbols(self):
        localcsv = LocalCSV()

        proxies = {}
        data = download_data_bundle(sample_data_bundle_fx_rate_csv_to_symbol, proxies)

        # assert set(data.keys()) == set(['coinA coin_source1/info', 'coinB coin_source_from_config/info', 'coinA coin_source1/data', 'coinB coin_source_from_config/data'])
        # assert data['coinA coin_source1/data'].values.size == 0
        # npt.assert_almost_equal(data['coinB coin_source_from_config/data'].values[0,:], [2.006, 73.104, 72.702, 72.972], decimal=3)
        # npt.assert_almost_equal(data['coinB coin_source_from_config/data'].index.values, [1569805200, 1569808800, 1569812400], decimal=3)
        # assert data['coinA coin_source1/info'] == {'bbg_code': 'bbg_code_coinA', 'csv_name': 'ASSET_A', 'instrument_type': 'crypto', 'source': 'coin_source1', 'symbol': 'coinA', 'timezone': 'Asia/Hong_Kong', 'trading_ccy': 'USD'}
        # assert data['coinB coin_source_from_config/info'] == {'csv_name': 'ASSET_B', 'symbol': 'coinB', 'trading_ccy': 'USD'}