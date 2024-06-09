import json
import os
import unittest
from datetime import datetime

import numpy as np
import pandas as pd

from engine_manager import EngineManager
from quantcycle.app.order_crosser.order_router import \
    TestOrderRouter as OrderRouter
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.engine.quant_engine import QuantEngine
from quantcycle.utils import get_logger
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator

#"main_dir": "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_CN/",
#"info": "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_stock_CN/info.csv" 

class STOCK(unittest.TestCase):

    def test_China_stock_EW(self):
        quant_engine = QuantEngine()
        pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH', '002024.SZ', '000100.SZ',
                       '600000.SH', '600519.SH', '601318.SH', '601988.SH', '002415.SZ', '000917.SZ']},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
                        
        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_CN/"
        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_CN.json'))
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_CN/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_stock_CN/info.csv"

        

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_CN_STOCK_lv1.hdf5")
        result_reader = ResultReader(path_name)

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 1560222.40)

    def test_China_stock_EW_usd(self):

        pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH', '002024.SZ', '000100.SZ',
                       '600000.SH', '600519.SH', '601318.SH', '601988.SH', '002415.SZ', '000917.SZ']},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        quant_engine = QuantEngine()

        #json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_CN_usd.json'))
        #quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))

        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_CN_usd/"
        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_CN_usd.json'))
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_CN_usd/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_stock_CN_usd/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))




        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_CN_STOCK_lv1_usd.hdf5")
        result_reader = ResultReader(path_name)

        #result_reader.to_csv(export_folder = 'data/cn_stock_production_usd', id_list = [0])

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 1397129.72)

    def test_HK_stock_EW(self):
        quant_engine = QuantEngine()
        pool_setting = {"symbol": {"STOCKS": ["2388.HK", "0005.HK", "1299.HK"]},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "save_path": "tests/engine/production/asset_class/test_EW_lv1/strategy_pool/EW_lv1_strategy_pool.csv",
                        "params": {
                            # No parameters
                        }}

        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_HK.json'))

        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_HK/"
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_HK/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_stock_HK/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))



        """ quant_engine.load_config(json.load(open(
            'tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_HK.json')),
            strategy_pool_generator(pool_setting,save=False)) """

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_HK_STOCK_lv1.hdf5")
        result_reader = ResultReader(path_name)

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        print(pnl[0][0][-1:].values)
        assert np.isclose(pnl[0][0].iloc[-1,0],1935398.53)

    def test_HK_stock_EW_usd(self):
        pool_setting = {"symbol": {"STOCKS": ["0005.HK", "1299.HK", "2388.HK"]},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        
        quant_engine = QuantEngine()

        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_HK_usd.json'))

        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_HK_usd/"
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_HK_usd/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_stock_HK_usd/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))


        """ quant_engine.load_config(json.load(open(
            'tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_HK_usd.json')),
                                strategy_pool_generator(pool_setting,save=False)) """

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_HK_STOCK_lv1_usd.hdf5")
        result_reader = ResultReader(path_name)

        #result_reader.to_csv(export_folder = 'data/hk_stock_production_usd', id_list = [0])

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 1905674.48)

    def test_mixed_stock_precision(self):
        quant_engine = QuantEngine()
        pool_setting = {"symbol": {"STOCKS": ["600016.SH","2388.HK","601318.SH"]},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
       

        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_mixed.json'))

        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_mixed/"
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_stock_mixed/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_stock_mixed/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))



        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_mixed.hdf5")
        result_reader = ResultReader(path_name)

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 2302430.081)
    
    def test_mixed_stock_precision_usd(self):

        pool_setting = {"symbol": {"STOCKS": ["600016.SH", "2388.HK", "1299.HK", "601318.SH"]},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        quant_engine = QuantEngine()

        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_mixed_CN_HK_usd.json'))

        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_mixed_CN_HK_usd/"
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_mixed_CN_HK_usd/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_mixed_CN_HK_usd/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))


        """ quant_engine.load_config(json.load(open(
            'tests/engine/production/asset_class/test_EW_lv1/config/EW_mixed_CN_HK_usd.json')),
                                strategy_pool_generator(pool_setting,save=False)) """

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_mixed_CN_HK_lv1_usd.hdf5")
        result_reader = ResultReader(path_name)

        #result_reader.to_csv(export_folder = 'data/mixed_stock_production_usd', id_list = [0])

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 3343306.21)

    # def test_large_sample_size_stock_lv1_precision(self):
    #     my_file = open("tests/engine/production/asset_class/cn_stock_symbols.txt", "r")
    #     content_list = my_file.read().split('\n')
    #     # content_list[:300]
    #     pool_setting = {"symbol": {"STOCKS": content_list[:300]},
    #                     "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
    #                     "strategy_name": "EW_strategy",
    #
    #                     "params": {
    #                         # No parameters
    #                     }}
    #     quant_engine = QuantEngine()
    #     quant_engine.load_config(json.load(open(
    #         'tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_large.json')),
    #         strategy_pool_generator(pool_setting, save=False))
    #
    #     quant_engine.load_app(OrderRouter())
    #     quant_engine.load_data()
    #
    #     quant_engine.start_production_engine()
    #     quant_engine.start_production_engine_other()
    #
    #     quant_engine.end_production_engine()
    #     quant_engine.wait_production_engine()
    #     path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
    #                              f"EW_large_lv1.hdf5")
    #     result_reader = ResultReader(path_name)
    #
    #     pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
    #     assert np.isclose(pnl[0][0][-1:].values,625370.9551)
    #
    # def test_super_large_sample_size_stock_lv1_precision(self):
    #     my_file = open("tests/engine/backtest/asset_class/cn_stock_symbols.txt", "r")
    #     content_list = my_file.read().split('\n')
    #     # content_list[:300]
    #     pool_setting = {"symbol": {"STOCKS": content_list[:300]},
    #                     "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
    #                     "strategy_name": "EW_strategy",
    #
    #                     "params": {
    #                         # No parameters
    #                     }}
    #     quant_engine = QuantEngine()
    #     quant_engine.load_config(json.load(open(
    #         'tests/engine/production/asset_class/test_EW_lv1/config/EW_stock_super_large.json')),
    #         strategy_pool_generator(pool_setting, save=False))
    #
    #     quant_engine.load_app(OrderRouter())
    #     quant_engine.load_data()
    #
    #     quant_engine.start_production_engine()
    #     quant_engine.start_production_engine_other()
    #
    #     quant_engine.end_production_engine()
    #     quant_engine.wait_production_engine()
    #     path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
    #                              f"EW_super_large_lv1.hdf5")
    #     result_reader = ResultReader(path_name)
    #
    #     pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
    #     assert np.isclose(pnl[0][0][-1:].values, 979831.94)

    def test_us_etf_precision(self):
        pool_setting = {"symbol": {
            "STOCKS": ['VTI.US', 'SCHF.US', 'VWO.US', 'VB.US', 'AGG.US', 'EMB.US', 'HYG.US', 'FLOT.US', 'VTIP.US',
                       'GLD.US', 'XLE.US', 'QQQ.US', 'ASHR.US', 'INDA.US']},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",

                        "params": {
                            # No parameters
                        }}
        quant_engine = QuantEngine()


        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_US_ETF.json'))

        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_US_ETF/"
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_US_ETF/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_US_ETF/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))


        """ quant_engine.load_config(json.load(open(
            'tests/engine/production/asset_class/test_EW_lv1/config/EW_US_ETF.json')),
            strategy_pool_generator(pool_setting, save=False)) """

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_US_ETF_lv1.hdf5")
        result_reader = ResultReader(path_name)

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 1644550702)
    
    def test_us_etf_precision_hkd(self):

        pool_setting = {"symbol": {"STOCKS": ['VTI.US', 'SCHF.US', 'VWO.US', 'VB.US', 'AGG.US', 'EMB.US',
                                              'HYG.US', 'FLOT.US', 'VTIP.US', 'GLD.US', 'XLE.US', 'QQQ.US',
                                              'ASHR.US', 'INDA.US']},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",

                        "params": {
                            # No parameters
                        }}

        quant_engine = QuantEngine()

        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_US_ETF_hkd.json'))
        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_US_ETF_hkd/"
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_US_ETF_hkd/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_US_ETF_hkd/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))



        """ quant_engine.load_config(json.load(open(
            'tests/engine/production/asset_class/test_EW_lv1/config/EW_US_ETF_hkd.json')),
            strategy_pool_generator(pool_setting, save=False)) """

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_US_ETF_lv1_hkd.hdf5")
        result_reader = ResultReader(path_name)

        #result_reader.to_csv(export_folder = 'data/us_etf_production_usd', id_list = [0])

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 1644621074.28)

    def test_hk_etf_precision(self):
        pool_setting = {"symbol": {
            "STOCKS": ['2813.HK', '3010.HK', '3077.HK', '3081.HK', '3101.HK', '3140.HK', '3141.HK', '3169.HK']},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        quant_engine = QuantEngine()

        json_msg = json.load(open('tests/engine/production/asset_class/test_EW_lv1/config/EW_HK_ETF.json'))

        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_HK_ETF/"
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_HK_ETF/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_HK_ETF/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))

        """ quant_engine.load_config(json.load(open(
            'tests/engine/production/asset_class/test_EW_lv1/config/EW_HK_ETF.json')),
            strategy_pool_generator(pool_setting, save=False)) """

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_HK_ETF_lv1.hdf5")
        result_reader = ResultReader(path_name)

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 456420)
    #     625370.95

    def test_hk_etf_precision_usd(self):

        pool_setting = {"symbol": {"STOCKS": ['2813.HK', '3010.HK', '3077.HK', '3081.HK',
                                              '3101.HK', '3140.HK', '3141.HK', '3169.HK']},
                        "strategy_module": "tests.engine.production.asset_class.test_EW_lv1.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        #"save_path":"tests/HK_ETF.csv",
                        "params": {
                            # No parameters
                        }}

        quant_engine = QuantEngine()


        json_msg = json.load(open( 'tests/engine/production/asset_class/test_EW_lv1/config/EW_HK_ETF_usd.json'))
        oss_dir = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_HK_ETF_usd/"
        if os.path.isdir(oss_dir):
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["main_dir"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/stock_data/EW_HK_ETF_usd/"
            json_msg["data"]["STOCKS"]["DataCenterArgs"]["info"] = "//192.168.9.170/share/alioss/QuantCycle/data/production/precision_stock/info_data/EW_HK_ETF_usd/info.csv"

        quant_engine.load_config(json_msg,strategy_pool_generator(pool_setting,save=False))


        """ quant_engine.load_config(json.load(open(
            'tests/engine/production/asset_class/test_EW_lv1/config/EW_HK_ETF_usd.json')),
            strategy_pool_generator(pool_setting, save=False)) """

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/asset_class/test_EW_lv1/result",
                                 f"EW_HK_ETF_lv1_usd.hdf5")
        result_reader = ResultReader(path_name)

        #result_reader.to_csv(export_folder = 'data/hk_etf_production_usd', id_list = [0])

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(pnl[0][0].iloc[-1,0], 436909.03)

    # def test_lv1_2_3_precision(self):
    #     rsi_pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH',
    #                                               '002024.SZ', '000100.SZ']},
    #                         "strategy_module": "tests.engine.backtest.asset_class.test_stock.algorithm.RSI_lv1",
    #                         "strategy_name": "RSI_strategy",
    #                         "params": {
    #                             "length": [10],
    #                             "break_threshold": [10, 20, 30],
    #                             "stop_profit": [0.01],
    #                             "stop_loss": [0.005],
    #                             "max_hold_days": [10]
    #                         }}
    #
    #     kd_pool_setting = {"symbol": {"STOCKS": ['600000.SH', '000005.SZ', '600519.SH', '601318.SH', '601988.SH',
    #                                              '002415.SZ', '000917.SZ']},
    #                        "strategy_module": "tests.engine.backtest.asset_class.test_stock.algorithm.KD_lv1",
    #                        "strategy_name": "KD_strategy",
    #                        "params": {
    #                            "length1": [10, 20],
    #                            "length2": [3],
    #                            "length3": [3],
    #                            "break_threshold": [10, 20],
    #                            "stop_profit": [0.01],
    #                            "stop_loss": [0.005],
    #                            "max_hold_days": [10]
    #                        }}
    #
    #     rsi_lv2_pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH',
    #                                                   '601628.SH', '002024.SZ', '000100.SZ'],
    #                                        "RSI": list(np.arange(3 * 3).astype(np.float64))},  # 不转成float json会报错
    #                             "strategy_module": "tests.engine.backtest.asset_class.test_stock.algorithm.RSI_lv2",
    #                             "strategy_name": "Allocation_strategy",
    #                             "params": {
    #
    #                             }}
    #
    #     kd_lv2_pool_setting = {"symbol": {"STOCKS": ['600000.SH', '000005.SZ', '600519.SH', '601318.SH',
    #                                                  '601988.SH', '002415.SZ', '000917.SZ'],
    #                                       "KD": list(np.arange(4 * 3).astype(np.float64))},  # 不转成float json会报错
    #                            "strategy_module": "tests.engine.backtest.asset_class.test_stock.algorithm.KD_lv2",
    #                            "strategy_name": "Allocation_strategy",
    #                            "params": {
    #
    #                            }}
    #

