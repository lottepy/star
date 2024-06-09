import numpy as np
import unittest
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.utils.run_once import run_once


class MultiAsset(unittest.TestCase):
    def test_pipeline(self):
        self.__run_lv1_pipeline()
        self.__run_lv2_pipeline()
        return True

    def __run_lv1_pipeline(self):
        pool_settings = {'symbol': {'FX': ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY'],
                                    'STOCKS': ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH'],
                                    'FUTURES': ['1603000558', '1603000563', '1603000568',
                                                '1603000557', '1603000562', '1603000567']},
                         'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RSI_lv1',
                         'strategy_name': 'RSI_strategy',
                         'params': {
                             "length": [10],
                             "break_threshold": [5, 30],
                             "stop_profit": [0.02],
                             "stop_loss": [0.01, 0.02],
                             "max_hold_days": [10]
                         }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RSI_lv1.json',
                 strategy_pool_generator(pool_settings, save=False))

    def __run_lv2_pipeline(self):
        pool_settings = {'symbol': {'FX': ['EURUSD', 'USDJPY', 'NZDJPY', 'NOKSEK', 'GBPJPY'],
                                    'STOCKS': ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH'],
                                    'FUTURES': ['1603000558', '1603000563', '1603000568',
                                                '1603000557', '1603000562', '1603000567'],
                                    "RSI": list(np.arange(4 * 16).astype(np.float64))},  # 不转成float json会报错
                         'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RSI_lv2',
                         'strategy_name': 'Allocation_strategy',
                         "params": {}}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RSI_lv2.json',
                 strategy_pool_generator(pool_settings, save=False))


    def test_RW_FX_CN_stock(self):
        pool_settings = {'symbol': {'FX': ['EURUSD', 'NOKSEK', 'NZDJPY', 'USDJPY'],
                                    'STOCKS': ['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_FX_CN_stock.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_FX_CN_stock")
        output = reader.get_strategy(id_list=list(range(9)), fields=["pnl"])
        #FX_pnl_57 = output[0][0].iloc[-1,0]+ output[1][0].iloc[-1,0] + output[2][0].iloc[-1,0] + output[3][0].iloc[-1,0]
        FX_pnl_00 = output[0][0].iloc[-2,0]+ output[1][0].iloc[-2,0] + output[2][0].iloc[-2,0] + output[3][0].iloc[-2,0] #不计息
        Stock_pnl = output[4][0].iloc[-1,0]+ output[5][0].iloc[-1,0] + output[6][0].iloc[-1,0] + output[7][0].iloc[-1,0] + output[8][0].iloc[-1,0]
        assert np.isclose(FX_pnl_00 + Stock_pnl, -132866.678896)
        
    @unittest.skip("test_RW_FX_CN_stock & test_RW_FX_HK_stock & test_RW_FX_HK_etfs % US_stock US_etfs test result prep")
    def test_RW_FX_(self):
        pool_settings = {'symbol': {'FX': ['EURUSD', 'NOKSEK', 'NZDJPY', 'USDJPY']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_FX_.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_FX_")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        print(output[0][-1][-1:].values)
        #reader.to_csv(export_folder = 'results/new/RW_FX', id_list = [0])
        assert np.isclose(output[0][0].iloc[-1,0], -131974.59271)
    
    @unittest.skip("test_RW_FX_CN_stock test result prep")
    def test_RW_CN_stock_(self):
        pool_settings = {'symbol': {'STOCKS': ['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_CN_stock_.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_CN_stock_")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        print(output[0][-1][-1:].values)
        #reader.to_csv(export_folder = 'results/new/RW_CN_stock', id_list = [0])
        assert np.isclose(output[0][0].iloc[-1,0], -892.08619)
    

    # def test_RW_FX_US_etfs(self):
    #     pool_settings = {'symbol': {'FX': ['EURUSD', 'NOKSEK', 'NZDJPY', 'USDJPY'],
    #                                 'STOCKS':['AGG.US', 'ASHR.US', 'EMB.US', 'FLOT.US', 'GLD.US', 'HYG.US', 
    #                                           'INDA.US', 'QQQ.US', 'SCHF.US', 'VB.US', 'VTI.US', 'VTIP.US', 'VWO.US', 'XLE.US']},
    #                     'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
    #                     'strategy_name': 'RandomWeighting',
    #                     'params': {
    #                         # No parameters
    #                     }}
    #     run_once(r'tests/engine/backtest/test_precision_multi/config/RW_FX_US_etfs.json',
    #             strategy_pool_generator(pool_settings, save=False))
    #     reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_FX_US_etfs")
    #     output = reader.get_strategy(id_list=[0], fields=["pnl"])
    #     print(output[0][-1][-1:].values)
    #     #reader.to_csv(export_folder = 'results/RW_FX_US_etfs', id_list = [0])
    #     assert np.isclose(output[0][0].iloc[-1,0], 537881.452774 - 131974.59271) #TODO
    
    # #@unittest.skip("test_RW_FX_US_stock test result prep")
    # def test_RW_US_etfs_(self):
    #     pool_settings = {'symbol': {'STOCKS':['AGG.US', 'ASHR.US', 'EMB.US', 'FLOT.US', 'GLD.US', 'HYG.US', 
    #                                           'INDA.US', 'QQQ.US', 'SCHF.US', 'VB.US', 'VTI.US', 'VTIP.US', 'VWO.US', 'XLE.US']},
    #                     'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
    #                     'strategy_name': 'RandomWeighting',
    #                     'params': {
    #                         # No parameters
    #                     }}
    #     run_once(r'tests/engine/backtest/test_precision_multi/config/RW_US_etfs_.json',
    #             strategy_pool_generator(pool_settings, save=False))
    #     reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_US_etfs_")
    #     output = reader.get_strategy(id_list=[0], fields=["pnl"])
    #     print(output[0][-1][-1:].values)
    #     #reader.to_csv(export_folder = 'results/RW_US_etfs_', id_list = [0])
    #     assert np.isclose(output[0][0].iloc[-1,0], 537881.45277) 
    
    def test_RW_FX_mixed_stocks(self):
        pool_settings = {'symbol': {'FX': ['EURUSD', 'NOKSEK', 'NZDJPY', 'USDJPY'],
                                    'STOCKS':['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH',
                                                "0005.HK", "1299.HK", "2388.HK",
                                                '2813.HK', '3010.HK', '3077.HK', '3081.HK']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_FX_mixed_stocks.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_FX_mixed_stocks")

        output = reader.get_strategy(id_list=list(range(16)), fields=["pnl"])
        FX_pnl_00 = output[0][0].iloc[-3,0]+ output[1][0].iloc[-3,0] + output[2][0].iloc[-3,0] + output[3][0].iloc[-3,0] #不计息
        Stock_pnl = output[4][0].iloc[-1,0]+ output[5][0].iloc[-1,0] + output[6][0].iloc[-1,0] + output[7][0].iloc[-1,0] + output[8][0].iloc[-1,0] \
                + output[9][0].iloc[-1,0]+ output[10][0].iloc[-1,0] + output[11][0].iloc[-1,0] + output[12][0].iloc[-1,0] + output[13][0].iloc[-1,0] \
                + output[14][0].iloc[-1,0]+ output[15][0].iloc[-1,0]
        assert np.isclose(FX_pnl_00 + Stock_pnl, 39990.96265-131974.59271) # 39990.96265-131974.59271
    
    @unittest.skip("test cases with mixed stock based on local currency result prep")
    def test_RW_mixed_stocks_(self):
        pool_setting = {"symbol": { 'STOCKS': ['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH',
                                                "0005.HK", "1299.HK", "2388.HK",
                                                '2813.HK', '3010.HK', '3077.HK', '3081.HK']},
                        "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
                        "strategy_name": "RandomWeighting",
                        "params": {
                        }}

        run_once('tests/engine/backtest/test_precision_multi/config/RW_mixed_stocks_.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_mixed_stocks_.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        print(final_pnl) 
        assert np.isclose(final_pnl, 39990.96265) 
    
    @unittest.skip("test cases with futures result prep")
    def test_RW_Futures_(self):
        pool_settings = {'symbol': {'FUTURES': ['1603000557', '1603000558', '1603000562', '1603000563', '1603000567', '1603000568']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_Futures_.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_Futures_")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        print(output[0][-1][-1:].values)
        #reader.to_csv(export_folder = 'results/1116_RW_Futures')
        assert np.isclose(output[0][0].iloc[-1,0], 64885.12982)
    
    @unittest.skip("test_RW_FX_mixed_stocks_Futures result prep")
    def test_RW_FX_mixed_stocks_prepFutures(self):
        pool_settings = {'symbol': {'FX': ['EURUSD', 'NOKSEK', 'NZDJPY', 'USDJPY'],
                                    'STOCKS':['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH',
                                                "0005.HK", "1299.HK", "2388.HK",
                                                '2813.HK', '3010.HK', '3077.HK', '3081.HK']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_FX_mixed_stocks_prepFutures.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_FX_mixed_stocks_prepFutures")
        #reader.to_csv(export_folder = 'results/1116_FX_mixed_stocks_prepFutures')
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        assert np.isclose(output[0][0].iloc[-1,0], -38700.63464) # 39990.96265-131974.59271
    
    def test_RW_FX_mixed_stocks_Futures(self):
        pool_settings = {'symbol': {'FX': ['EURUSD', 'NOKSEK', 'NZDJPY', 'USDJPY'],
                                    'STOCKS':['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH',
                                                "0005.HK", "1299.HK", "2388.HK",
                                                '2813.HK', '3010.HK', '3077.HK', '3081.HK'],
                                    'FUTURES': ['1603000557', '1603000558', '1603000562', '1603000563', '1603000567', '1603000568']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_FX_mixed_stocks_Futures.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_FX_mixed_stocks_Futures")
        #reader.to_csv(export_folder = 'results/1116_FX_mixed_stocks_Futures')
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        # assert np.isclose(output[0][0].iloc[-1,0], 64885.12982 - 38700.63464) # -35088.365177 + 67828.48027
        # DataMaster 2020-12-03 update affects this pnl, strictly controlled
        assert np.isclose(output[0][0].iloc[-1,0], 49520.165)

    @unittest.skip("test cases with FX csv result prep")
    def test_RW_FX_csv_(self):
        pool_setting = {"symbol": {"FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW']},
                        "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
                        "strategy_name": "RandomWeighting",
                        "params": {
                            # EW has no parameters
                        }}

        run_once('tests/engine/backtest/test_precision_multi/config/RW_FX_csv_.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_FX_csv_.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        # reader.to_csv(export_folder = 'results/RW_FX_csv_', id_list = [0])
        print(final_pnl) 
        # -4534.770516156812 sit result
        assert np.isclose(final_pnl, -3878.27769)
    
    @unittest.skip("test cases with mixed stock based on local currency result prep")
    def test_RW_mixed_stocks_local_(self):
        pool_setting = {"symbol": { 'STOCKS': ['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH',
                                                "0005.HK", "1299.HK", "2388.HK",
                                                '2813.HK', '3010.HK', '3077.HK', '3081.HK']},
                        "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
                        "strategy_name": "RandomWeighting",
                        "params": {
                            # EW has no parameters
                        }}
                        # 'AGG.US', 'ASHR.US'

        run_once('tests/engine/backtest/test_precision_multi/config/RW_mixed_stocks_local_.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_mixed_stocks_local_.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        #reader.to_csv(export_folder = 'results/RW_mixed_stocks_local_', id_list = [0])
        print(final_pnl) 
        assert np.isclose(final_pnl, -5596.357)

    def test_RW_FX_csv_mixed_stocks(self):
        pool_setting = {"symbol": { "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW'],
                                    'STOCKS': ['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH',
                                                "0005.HK", "1299.HK", "2388.HK",
                                                '2813.HK', '3010.HK', '3077.HK', '3081.HK']},
                        "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
                        "strategy_name": "RandomWeighting",
                        "params": {
                            # EW has no parameters
                        }}
                    
        run_once('tests/engine/backtest/test_precision_multi/config/RW_FX_csv_mixed_stocks.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_FX_csv_mixed_stocks.hdf5')
        #reader.to_csv(export_folder = 'results/RW_FX_csv_mixed_stocks', id_list = [0])
        result = reader.get_strategy(id_list=list(range(17)), fields=["pnl"])
        fx_pnl = sum(result[i][0].iloc[-3, 0] for i in range(5))        # -3878.27771
        stocks_pnl = sum(result[i][0].iloc[-1, 0] for i in range(5, 17))
        assert np.isclose(fx_pnl + stocks_pnl, -3878.27769 - 5596.357)
    
    # #@unittest.skip("test cases with futures based on local currency result prep")
    # def test_RW_Futures_local_(self):
    #     # Day cut off at 16:00 UTC. No skipping timestamp at this time.
    #     pool_setting = {"symbol": { 'FUTURES': ['1603000557', '1603000558', '1603000562', '1603000563', '1603000567', '1603000568']},
    #                     "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
    #                     "strategy_name": "RandomWeighting",
    #                     "params": {
    #                         # EW has no parameters
    #                     }}

    #     run_once('tests/engine/backtest/test_precision_multi/config/RW_Futures_local_.json',
    #              strategy_pool_generator(pool_setting, save=False))
    #     reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_Futures_local_.hdf5')
    #     final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
    #     #reader.to_csv(export_folder = 'results/RW_Futures_local_', id_list = [0])
    #     print(final_pnl)
    #     # 2590.466335850353 sit result 
    #     assert np.isclose(final_pnl, 0)

    # def test_RW_FX_csv_mixed_stocks_Futures(self):
    #     # Day cut off at 16:00 UTC. No skipping timestamp at this time.
    #     pool_setting = {"symbol": { "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW'],
    #                                 'STOCKS': ['000005.SZ', '600016.SH', '600028.SH', '601398.SH', '601628.SH'
    #                                             "0005.HK", "1299.HK", "2388.HK",
    #                                             '2813.HK', '3010.HK', '3077.HK', '3081.HK', '3101.HK', '3140.HK', '3141.HK', '3169.HK'
    #                                             'AGG.US', 'ASHR.US'],
    #                                 'FUTURES': ['1603000557', '1603000558', '1603000562', '1603000563', '1603000567', '1603000568']},
    #                     "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
    #                     "strategy_name": "RandomWeighting",
    #                     "params": {
    #                         # EW has no parameters
    #                     }}

    #     run_once('tests/engine/backtest/test_precision_multi/config/RW_FX_csv_mixed_stocks_Futures.json',
    #              strategy_pool_generator(pool_setting, save=False))
    #     reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_FX_csv_mixed_stocks_Futures.hdf5')
    #     final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
    #     #reader.to_csv(export_folder = 'results/RW_FX_csv_mixed_stocks_Futures', id_list = [0])
    #     print(final_pnl) 
    #     assert np.isclose(final_pnl, 0) #TODO

    @unittest.skip("test cases with local csv stock data")
    def test_RW_stocks_csv_(self):
        # Day cut off at 16:00 UTC. No skipping timestamp at this time.
        # HK stocks are the same as US stocks
        pool_setting = {"symbol": {"STOCKS": ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
                        "strategy_name": "RandomWeighting",
                        "params": {
                            # EW has no parameters
                        }}

        run_once('tests/engine/backtest/test_precision_multi/config/RW_stocks_csv_.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_stocks_csv_.hdf5')
        final_pnl = reader.get_strategy(id_list=[0], fields=["pnl"])[0][0].iloc[-1, 0]
        assert np.isclose(final_pnl, -6002.98290)

    def test_RW_stocks_csv_FX(self):
        pool_setting = {"symbol": { "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW'],
                                    'STOCKS': ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5']},
                        "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
                        "strategy_name": "RandomWeighting",
                        "params": {
                            # EW has no parameters
                        }}
                    
        run_once('tests/engine/backtest/test_precision_multi/config/RW_stocks_csv_FX.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_stocks_csv_FX.hdf5')
        #reader.to_csv(export_folder = 'results/RW_FX_csv_mixed_stocks', id_list = [0])
        result = reader.get_strategy(id_list=list(range(10)), fields=["pnl"])
        fx_pnl = sum(result[i][0].iloc[-1, 0] for i in range(5))  
        stocks_pnl = sum(result[i][0].iloc[-1, 0] for i in range(5, 10))
        assert np.isclose(fx_pnl + stocks_pnl, -1906.02472) # = 4096.95818 -6002.98290
    
    @unittest.skip("FX result prep for stock_csv test")
    def test_RW_FX_stock_csv_prep(self):
        pool_settings = {'symbol': { "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_FX_stock_csv_prep.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_FX_stock_csv_prep")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        #print(output[0][-1][-1:].values)
        assert np.isclose(output[0][0].iloc[-1,0], 4096.95818)
    
    @unittest.skip("futures result prep for stock_csv test")
    def test_RW_Futures_stock_csv_prep(self):
        pool_settings = {'symbol': {'FUTURES': ['1603000557', '1603000558', '1603000562', '1603000563', '1603000567', '1603000568']},
                        'strategy_module': 'tests.engine.backtest.test_precision_multi.algorithm.RW_lv1',
                        'strategy_name': 'RandomWeighting',
                        'params': {
                            # No parameters
                        }}
        run_once(r'tests/engine/backtest/test_precision_multi/config/RW_Futures_stock_csv_prep.json',
                strategy_pool_generator(pool_settings, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_multi/result/RW_Futures_stock_csv_prep")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        print(output[0][-1][-1:].values)
        assert np.isclose(output[0][0].iloc[-1,0], -38139.53212)
    
    def test_RW_stocks_csv_FX_Futures(self):
        pool_setting = {"symbol": { "FX": ['AUDCAD', 'AUDJPY', 'EURUSD', 'USDJPY', 'USDKRW'],
                                    'STOCKS': ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5'],
                                    'FUTURES': ['1603000557', '1603000558', '1603000562', '1603000563', '1603000567', '1603000568']},
                        "strategy_module": "tests.engine.backtest.test_precision_multi.algorithm.RW_lv1",
                        "strategy_name": "RandomWeighting",
                        "params": {
                            # EW has no parameters
                        }}
                    
        run_once('tests/engine/backtest/test_precision_multi/config/RW_stocks_csv_FX_Futures.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader(r'tests/engine/backtest/test_precision_multi/result/RW_stocks_csv_FX_Futures.hdf5')
        result = reader.get_strategy(id_list=list(range(16)), fields=["pnl"])
        fx_pnl = sum(result[i][0].iloc[-1, 0] for i in range(5))        
        stocks_pnl = sum(result[i][0].iloc[-1, 0] for i in range(5, 10))
        futures_pnl = sum(result[i][0].iloc[-1, 0] for i in range(10, 16))
        assert np.isclose(fx_pnl + stocks_pnl + futures_pnl, -40045.55684) # = 4096.95818 -6002.98290 -38139.53212
