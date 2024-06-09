"""
[ Future tests ]
    This test group aims to test future related work.

"""

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
from quantcycle.utils.run_once import run_once
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator


class ProdFutureTest(unittest.TestCase):
    def test_constant_holding(self):
        pool_settings = {'symbol': {'FUTURES': ['1603000558', '1603000563', '1603000568',
                                                '1603000557', '1603000562', '1603000567']},
                        'strategy_module': 'tests.engine.production.test_futures.algorithm.constant_holding_lv1',
                        'strategy_name': 'ConstantHolding',
                        'params': {
                            # No parameters
                        }}
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_futures/config/constant_holding_lv1.json')),
            strategy_pool_generator(pool_settings, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/test_futures/result",
                                    f"constant_holding_lv1.hdf5")
        reader = ResultReader(path_name)
        pnl = reader.get_strategy(id_list=[0], fields=["pnl"])
        position = reader.get_strategy(id_list=[0], fields=["position"])
        #assert np.isclose(pnl[0][0].iloc[-1, 0], 975.75)
        assert np.isclose(pnl[0][0].iloc[-1, 0], 1175.25)
        #assert np.isclose(pnl[0][0][-1:].values, 1560222.40)


    def test_random_weighting(self):
        pool_settings = {'symbol': {'FUTURES': ['1603000558', '1603000563',
                                                '1603000557', '1603000562']},
                        'strategy_module': 'tests.engine.production.test_futures.algorithm.random_weighting_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_futures/config/random_weighting_lv1.json')),
            strategy_pool_generator(pool_settings, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/test_futures/result",
                                    f"random_weighting_lv1.hdf5")
        reader = ResultReader(path_name)
        pnl = reader.get_strategy(id_list=[0], fields=["pnl"])
        position = reader.get_strategy(id_list=[0], fields=["position"])
        #assert np.isclose(pnl[0][0].iloc[-1, 0], 240486.45)
        assert np.isclose(pnl[0][0].iloc[-1, 0], 267462.55)

    def test_refer_adjusted_price(self):
        pool_settings = {'symbol': {'FUTURES': ['1603000558', '1603000563', '1603000568',
                                                '1603000557', '1603000562', '1603000567']},
                        'strategy_module': 'tests.engine.production.test_futures.algorithm.constant_holding_lv1_refer_adjusted',
                        'strategy_name': 'ConstantHolding',
                        'params': {
                            # No parameters
                        }}

        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            r'tests/engine/production/test_futures/config/constant_holding_lv1_refer_adjusted.json')),
            strategy_pool_generator(pool_settings, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()


    def test_many_commodities_local(self):
        pool_settings = {'symbol': {'FUTURES': ['CC_MAIN1', 'CC_MAIN2', 'CL_MAIN1', 'CL_MAIN2', 'CO_MAIN1', 'CO_MAIN2',
                                                'CT_MAIN1', 'CT_MAIN2', 'C_MAIN1', 'C_MAIN2', 'FC_MAIN1', 'FC_MAIN2',
                                                'GC_MAIN1', 'GC_MAIN2', 'HG_MAIN1', 'HG_MAIN2', 'HO_MAIN1', 'HO_MAIN2',
                                                'KC_MAIN1', 'KC_MAIN2', 'KO_MAIN1', 'KO_MAIN2', 'LA_MAIN1', 'LA_MAIN2',
                                                'LC_MAIN1', 'LC_MAIN2', 'LH_MAIN1', 'LH_MAIN2', 'LL_MAIN1', 'LL_MAIN2',
                                                'LN_MAIN1', 'LN_MAIN2', 'LT_MAIN1', 'LT_MAIN2', 'LX_MAIN1', 'LX_MAIN2',
                                                'NG_MAIN1', 'NG_MAIN2', 'PL_MAIN1', 'PL_MAIN2', 'QS_MAIN1', 'QS_MAIN2',
                                                'SAI_MAIN1', 'SAI_MAIN2', 'SB_MAIN1', 'SB_MAIN2', 'S_MAIN1', 'S_MAIN2',
                                                'W_MAIN1', 'W_MAIN2', 'XB_MAIN1', 'XB_MAIN2']},
                        'strategy_module': 'tests.engine.production.test_futures.algorithm.random_weighting_lv1_less_cash',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        
        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_futures/config/many_commodities_local.json')),
            strategy_pool_generator(pool_settings, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()
        
        result_reader = ResultReader(r'tests/engine/production/test_futures/result/many_commodities_local')
        output = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(output[0][0].iloc[-1, 0], 9292.780)

    def test_large_weights(self):
        pool_settings = {'symbol': {'FUTURES': ['C_MAIN1', 'C_MAIN2',
                                                'S_MAIN1', 'S_MAIN2']},
                        'strategy_module': 'tests.engine.production.test_futures.algorithm.random_weighting_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}

        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            'tests/engine/production/test_futures/config/large_weights.json')),
            strategy_pool_generator(pool_settings, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()
        
        result_reader = ResultReader(r'tests/engine/production/test_futures/result/large_weights')
        output = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(output[0][0].iloc[-1, 0], 4018575398.406)


    def test_price_has_zero(self):
        pool_settings = {'symbol': {'FUTURES': ['C_MAIN1', 'C_MAIN2',
                                                'S_MAIN1', 'S_MAIN2']},
                         'strategy_module': 'tests.engine.production.test_futures.algorithm.random_position_lv1',
                         'strategy_name': 'RandomWeighting',
                         'params': {
                             # No parameters
                         }}

        quant_engine = QuantEngine()
        quant_engine.load_config(json.load(open(
            r'tests/engine/production/test_futures/config/price_has_zero.json')),
            strategy_pool_generator(pool_settings, save=False))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        result_reader = ResultReader(r'tests/engine/production/test_futures/result/price_has_zero')
        output = result_reader.get_strategy(id_list=[0], fields=['pnl'])
        assert np.isclose(output[0][0].iloc[-1, 0], -243937.918)

