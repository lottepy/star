import numpy as np
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.utils.run_once import run_once


def test_constant_holding():
    pool_settings = {'symbol': {'FUTURES': ['1603000558', '1603000563', '1603000568',
                                            '1603000557', '1603000562', '1603000567']},
                     'strategy_module': 'tests.engine.backtest.test_futures.algorithm.constant_holding_lv1',
                     'strategy_name': 'ConstantHolding',
                     'params': {
                         # No parameters
                     }}
    run_once(r'tests/engine/backtest/test_futures/config/constant_holding_lv1.json',
             strategy_pool_generator(pool_settings, save=False))
    reader = ResultReader(r'tests/engine/backtest/test_futures/result/constant_holding_lv1')
    output = reader.get_strategy(id_list=[0], fields=["pnl"])
    # assert np.isclose(output[0][0].iloc[-1, 0], 975.75)
    # DataMaster 2020-12-03 update affects the above pnl, strictly controlled
    assert np.isclose(output[0][0].iloc[-1, 0], 1175.25)


def test_constant_holding_datamaster():
    pool_settings = {'symbol': {'FUTURES': ['1603000558', '1603000563', '1603000568',
                                            '1603000557', '1603000562', '1603000567']},
                     'strategy_module': 'tests.engine.backtest.test_futures.algorithm.constant_holding_lv1',
                     'strategy_name': 'ConstantHolding',
                     'params': {
                         # No parameters
                     }}
    run_once(r'tests/engine/backtest/test_futures/config/constant_holding_lv1_dm.json',
             strategy_pool_generator(pool_settings, save=False))
    reader = ResultReader(r'tests/engine/backtest/test_futures/result/constant_holding_lv1_dm')
    output = reader.get_strategy(id_list=[0], fields=["pnl"])
    # assert np.isclose(output[0][0].iloc[-1, 0], 975.75)
    # DataMaster 2020-12-03 update affects the above pnl, strictly controlled
    assert np.isclose(output[0][0].iloc[-1, 0], 1175.25)


def test_random_weighting():
    pool_settings = {'symbol': {'FUTURES': ['1603000558', '1603000563',
                                            '1603000557', '1603000562']},
                     'strategy_module': 'tests.engine.backtest.test_futures.algorithm.random_weighting_lv1',
                     'strategy_name': 'RandomWeighting',
                     'params': {
                         # No parameters
                     }}
    run_once(r'tests/engine/backtest/test_futures/config/random_weighting_lv1.json',
             strategy_pool_generator(pool_settings, save=False))
    reader = ResultReader(r'tests/engine/backtest/test_futures/result/random_weighting_lv1')
    output = reader.get_strategy(id_list=[0], fields=["pnl"])
    # assert np.isclose(output[0][0].iloc[-1, 0], 240486.45)
    # DataMaster 2020-12-03 update affects the above pnl, strictly controlled
    assert np.isclose(output[0][0].iloc[-1, 0], 267462.55)


def test_refer_adjusted_price():
    pool_settings = {'symbol': {'FUTURES': ['1603000558', '1603000563', '1603000568',
                                            '1603000557', '1603000562', '1603000567']},
                     'strategy_module': 'tests.engine.backtest.test_futures.algorithm.constant_holding_lv1_refer_adjusted',
                     'strategy_name': 'ConstantHolding',
                     'params': {
                         # No parameters
                     }}
    run_once(r'tests/engine/backtest/test_futures/config/constant_holding_lv1_refer_adjusted.json',
             strategy_pool_generator(pool_settings, save=False))


def test_many_commodities_local():
    pool_settings = {'symbol': {'FUTURES': ['CC_MAIN1', 'CC_MAIN2', 'CL_MAIN1', 'CL_MAIN2', 'CO_MAIN1', 'CO_MAIN2',
                                            'CT_MAIN1', 'CT_MAIN2', 'C_MAIN1', 'C_MAIN2', 'FC_MAIN1', 'FC_MAIN2',
                                            'GC_MAIN1', 'GC_MAIN2', 'HG_MAIN1', 'HG_MAIN2', 'HO_MAIN1', 'HO_MAIN2',
                                            'KC_MAIN1', 'KC_MAIN2', 'KO_MAIN1', 'KO_MAIN2', 'LA_MAIN1', 'LA_MAIN2',
                                            'LC_MAIN1', 'LC_MAIN2', 'LH_MAIN1', 'LH_MAIN2', 'LL_MAIN1', 'LL_MAIN2',
                                            'LN_MAIN1', 'LN_MAIN2', 'LT_MAIN1', 'LT_MAIN2', 'LX_MAIN1', 'LX_MAIN2',
                                            'NG_MAIN1', 'NG_MAIN2', 'PL_MAIN1', 'PL_MAIN2', 'QS_MAIN1', 'QS_MAIN2',
                                            'SAI_MAIN1', 'SAI_MAIN2', 'SB_MAIN1', 'SB_MAIN2', 'S_MAIN1', 'S_MAIN2',
                                            'W_MAIN1', 'W_MAIN2', 'XB_MAIN1', 'XB_MAIN2']},
                     'strategy_module': 'tests.engine.backtest.test_futures.algorithm.random_weighting_lv1_less_cash',
                     'strategy_name': 'RandomWeighting',
                     'params': {
                         # No parameters
                     }}
    run_once(r'tests/engine/backtest/test_futures/config/many_commodities_local.json',
             strategy_pool_generator(pool_settings, save=False))
    reader = ResultReader(r'tests/engine/backtest/test_futures/result/many_commodities_local')
    output = reader.get_strategy(id_list=[0], fields=["pnl"])
    assert np.isclose(output[0][0].iloc[-1, 0], 9292.780)   #! weakly controlled


def test_large_weights():
    pool_settings = {'symbol': {'FUTURES': ['C_MAIN1', 'C_MAIN2',
                                            'S_MAIN1', 'S_MAIN2']},
                     'strategy_module': 'tests.engine.backtest.test_futures.algorithm.random_weighting_lv1',
                     'strategy_name': 'RandomWeighting',
                     'params': {
                         # No parameters
                     }}
    run_once(r'tests/engine/backtest/test_futures/config/large_weights.json',
             strategy_pool_generator(pool_settings, save=False))
    reader = ResultReader(r'tests/engine/backtest/test_futures/result/large_weights')
    output = reader.get_strategy(id_list=[0], fields=["pnl"])
    assert np.isclose(output[0][0].iloc[-1, 0], 4018575398.406)   #! weakly controlled


def test_price_has_zero():
    pool_settings = {'symbol': {'FUTURES': ['C_MAIN1', 'C_MAIN2',
                                            'S_MAIN1', 'S_MAIN2']},
                     'strategy_module': 'tests.engine.backtest.test_futures.algorithm.random_position_lv1',
                     'strategy_name': 'RandomWeighting',
                     'params': {
                         # No parameters
                     }}
    run_once(r'tests/engine/backtest/test_futures/config/price_has_zero.json',
             strategy_pool_generator(pool_settings, save=False))
    reader = ResultReader(r'tests/engine/backtest/test_futures/result/price_has_zero')
    output = reader.get_strategy(id_list=[0], fields=['pnl'])
    assert np.isclose(output[0][0].iloc[-1, 0], -243937.918)

