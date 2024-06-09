from unittest import TestCase, mock

import pandas as pd
import pytest

from quantcycle.app.data_manager.utils.samples import *
from quantcycle.utils.production_constant import Symbol

from quantcycle.app.data_manager.data_loader.data_center.data_master.proxy import (DataMaster, _parse_fields, _parse_symbols,
                                 _replace_dm_symbol_by_mnemonic)


class ParseSymbolsTestCase(TestCase):
    def test_with_FX_Spot_BT150(self):
        data_bundle = {"Symbol": ["AUDCAD"],
                       "SymbolArgs": {"DataSource": "BT150"}}
        res = _parse_symbols(data_bundle)
        # BT150 Spot, return value is instruments id
        assert res[0][Symbol.DM_SYMBOL.value] == "AUDCAD BT150 Curncy"

    def test_with_FX_NDF_BT150(self):
        data_bundle = {"Symbol": ["USDINR"],
                       "SymbolArgs": {"DataSource": "BT150"}}
        res = _parse_symbols(data_bundle)
        # BT150 NDF, return value is instruments id
        assert res[0][Symbol.DM_SYMBOL.value] == "IRN+1M BT150 Curncy"

    def test_with_FX_Spot_BGNL(self):
        data_bundle = {"Symbol": ["AUDCAD"],
                       "SymbolArgs": {"DataSource": "BGNL"}}
        res = _parse_symbols(data_bundle)
        # BGNL Spot, return value is instrument mnemonic
        assert res[0][Symbol.DM_SYMBOL.value] == "AUDCAD BGNL Curncy"

    def test_with_FX_NDF_BGNL(self):
        data_bundle = {"Symbol": ["USDINR"],
                       "SymbolArgs": {"DataSource": "BGNL"}}
        res = _parse_symbols(data_bundle)
        # BGNL NDF, return value is instrument id
        assert res[0][Symbol.DM_SYMBOL.value] == "IRN+1M BGNL Curncy"

    def test_with_FX_Spot_INT(self):
        data_bundle = {"Symbol": ["AUD"],
                       "SymbolArgs": {"DataSource": "TN"}}
        res = _parse_symbols(data_bundle)
        # interest Spot -> TN, return value is instrument id
        assert res[0][Symbol.DM_SYMBOL.value] == "AUDTN BGNL Curncy"

    def test_with_FX_NDF_INT(self):
        data_bundle = {"Symbol": ["INR"],
                       "SymbolArgs": {"DataSource": "TN"}}
        res = _parse_symbols(data_bundle)
        # interest NDF -> BGNL NDF FP, return value is instrument id
        assert res[0][Symbol.DM_SYMBOL.value] == "IRN1M BGNL Curncy"

    def test_with_SHSE_list_stock(self):
        data_bundle = {"Symbol": ["600001.SH"]}
        res = _parse_symbols(data_bundle)
        # SHSE Stock, return value is instrument mnemonic
        assert res[0][Symbol.DM_SYMBOL.value] == "600001.SH"

    def test_with_SZSE_list_stock(self):
        data_bundle = {"Symbol": ["000001.SZ"]}
        res = _parse_symbols(data_bundle)
        # SZSE Stock, return value is instrument mnemonic
        assert res[0][Symbol.DM_SYMBOL.value] == "000001.SZ"

    def test_with_more_than_one_symbol(self):
        data_bundle = {"Symbol": ["600001.SH", "000001.SZ"]}
        res = _parse_symbols(data_bundle)
        assert res[0][Symbol.DM_SYMBOL.value] == "600001.SH"
        assert res[1][Symbol.DM_SYMBOL.value] == "000001.SZ"

    def test_with_frequency(self):
        data_bundle = {"Symbol": ["000001.SZ"], "Frequency": "DAILY"}
        res = _parse_symbols(data_bundle)
        assert res[0][Symbol.FREQUENCY.value] == 0
        data_bundle = {"Symbol": ["000001.SZ"], "Frequency": "HOURLY"}
        res = _parse_symbols(data_bundle)
        assert res[0][Symbol.FREQUENCY.value] == 1
        data_bundle = {"Symbol": ["000001.SZ"], "Frequency": "MINUTE"}
        res = _parse_symbols(data_bundle)
        assert res[0][Symbol.FREQUENCY.value] == 2

    def test_with_lack_frequency(self):
        data_bundle = {"Symbol": ["000001.SZ"]}
        res = _parse_symbols(data_bundle)
        assert res[0][Symbol.FREQUENCY.value] == 0

    def test_with_unrecognized_frequency(self):
        data_bundle = {"Symbol": ["000001.SZ"], "Frequency": "unknown"}
        res = _parse_symbols(data_bundle)
        assert res[0][Symbol.FREQUENCY.value] == 0


class ParseFieldsTestCase(TestCase):
    def test_OHLC(self):
        data_bundle = {"Fields": "OHLC"}
        res = _parse_fields(data_bundle)
        assert "open" in res['historical']
        assert "high" in res['historical']
        assert "low" in res['historical']
        assert "close" in res['historical']

    def test_close(self):
        data_bundle = {"Fields": "close"}
        res = _parse_fields(data_bundle)
        assert "close" in res['unknown']

    def test_INFO(self):
        data_bundle = {"Fields": "INFO"}
        res = _parse_fields(data_bundle)
        assert "subtype" in res['info']
        # assert "trading_currency" in res['info']
        assert "bloomberg_code" in res['info']
        assert "list_exchange" in res['info']

    def test_other(self):
        data_bundle = {"Fields": "other"}
        res = _parse_fields(data_bundle)
        assert "other" in res['unknown']

    def test_mix(self):
        data_bundle = {"Fields": ["INFO", "CLOSE"]}
        res = _parse_fields(data_bundle)
        assert "info" in res.keys()
        assert "unknown" in res.keys()


class DataMasterProxyTestCase(TestCase):

    def test_INFO(self):
        dm = DataMaster()
        rslt = dm.on_data_bundle(sample_data_bundle_INFO)
        assert len(rslt.keys()) > 0

    def test_DAILY(self):
        dm = DataMaster()
        rslt = dm.on_data_bundle(sample_data_bundle_MAIN)
        assert len(rslt.keys()) == 6
        assert "AUDCAD BT150/data" in rslt.keys()
        assert "USDTWD BT150/data" in rslt.keys()
        assert "USDINR BT150/data" in rslt.keys()
        assert rslt["AUDCAD BT150/data"].shape == (11, 4)
        assert rslt["AUDCAD BT150/data"].index[0] == 1577836800 # hardcode FX day close to UTC 0000
        # assert rslt["AUDCAD BT150/data"].index[0] == 1577901600 # after hardcode FX day close to UTC 1800

    def test_download_one_day(self):
        dm = DataMaster()
        rslt = dm.on_data_bundle({
            "Label": "FX/MAIN",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["AUDCAD", "USDTWD", "USDINR"],
            "SymbolArgs": {
                "DataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartTS": 1325462400,
            "EndTS": 1325462400,
            "Frequency": "DAILY"
        })
        assert "USDTWD BGNL/data" in rslt.keys()
        assert rslt["USDTWD BGNL/data"].shape == (1, 4)
        assert rslt["USDTWD BGNL/data"].index[0] == 1325462400 # hardcode FX day close to UTC 0000
        # assert rslt["AUDCAD BGNL/data"].index[0] == 1325527200 # after hardcode FX day close to UTC 1800


    def test_local_data(self):
        dm = DataMaster()
        rslt = dm.on_data_bundle({
            "Label": "FX/MAIN",
            'Type': "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["AUDCAD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "LOCAL_DATA": True,
                "LOCAL_DATA_DIR": 'tests/app/data_manager/data_loader/data_center/data_center_tests'
            },
            "Fields": "OHLC",
            "StartTS": 1325548800,
            "EndTS": 1325548800,
            "Frequency": "DAILY"
        })
        assert rslt["AUDCAD BT150/data"].shape == (1, 4)
        assert rslt['AUDCAD BT150/data'].iloc[0, 0] == 200

    def test_local_data_with_dmdata(self):
        dm = DataMaster()
        rslt = dm.on_data_bundle({
            "Label": "FX/MAIN",
            'Type': "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["AUDCAD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "LOCAL_DATA": True,
                "LOCAL_DATA_DIR": 'tests/app/data_manager/data_loader/data_center/data_center_tests'
            },
            "Fields": "OHLC",
            "StartTS": 1325548800,
            "EndTS": 1325635200,
            "Frequency": "DAILY"
        })
        assert rslt["AUDCAD BT150/data"].shape == (2, 4)
        assert rslt['AUDCAD BT150/data'].iloc[0, 0] == 200


class ReplaceSymbolTestCase(TestCase):

    def test_correctly_replace(self):
        symbols = [('AUDCAD BT150', 'AUDCAD', '1404000014', 0)]
        res = {'1404000014': pd.DataFrame(data=[1, 2, 3])}
        res = _replace_dm_symbol_by_mnemonic(symbols, res, suffix="")
        assert 'AUDCAD BT150' in res.keys()
        assert res['AUDCAD BT150'].iloc[2, 0] == 3

    def test_add_suffix(self):
        symbols = [('AUDCAD BT150', 'AUDCAD', '1404000014', 0)]
        res = {'1404000014': pd.DataFrame(data=[1, 2, 3])}
        res = _replace_dm_symbol_by_mnemonic(
            symbols, res, suffix="SOMESUFFIX")
        assert 'AUDCAD BT150/SOMESUFFIX' in res.keys()
        assert res['AUDCAD BT150/SOMESUFFIX'].iloc[2, 0] == 3
