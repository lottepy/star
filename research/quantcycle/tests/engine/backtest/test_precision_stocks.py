import unittest
import numpy as np
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator
from quantcycle.utils.run_once import run_once
from quantcycle.app.result_exporter.result_reader import ResultReader


class MyTestCase(unittest.TestCase):
    def test_cn_stock_lv1_precision(self):
        pool_setting = {"symbol": {
            "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH', '002024.SZ', '000100.SZ',
                       '600000.SH', '600519.SH', '601318.SH', '601988.SH', '002415.SZ', '000917.SZ']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_stock_CN.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_CN_STOCK_lv1")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])

        assert np.isclose(output[0][0].iloc[-1,0], 1560222.40)  # TODO: ffill needs investigation
        # Forward-fill pnl: 1560222.40; weekly-controlled non-fill pnl: 2136659.07
    
    def test_cn_stock_lv1_precision_datamaster(self):
        pool_setting = {"symbol": {
            "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH', '002024.SZ', '000100.SZ',
                       '600000.SH', '600519.SH', '601318.SH', '601988.SH', '002415.SZ', '000917.SZ']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_stock_CN_dm.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/asset_class/test_EW_lv1/result/EW_CN_STOCK_lv1_dm")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])

        assert np.isclose(output[0][0].iloc[-1,0], 1560222.40)  # TODO: ffill needs investigation
        # Forward-fill pnl: 1560222.40; weekly-controlled non-fill pnl: 2136659.07

    def test_cn_stock_lv1_precision_usd(self):
        pool_setting = {"symbol": {
            "STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH', '002024.SZ', '000100.SZ',
                       '600000.SH', '600519.SH', '601318.SH', '601988.SH', '002415.SZ', '000917.SZ']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_stock_CN_usd.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_CN_STOCK_lv1_usd")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        # output_position = reader.get_strategy(id_list=[0], fields=["pnl"])

        assert np.isclose(output[0][0].iloc[-1,0], 1397129.72398)  # TODO: ffill needs investigation
        # Forward-fill pnl: 1397129.72398; weekly-controlled non-fill pnl: 1868928.485


    def test_hk_stock_lv1_precision(self):
        pool_setting = {"symbol": {"STOCKS": ["0005.HK", "1299.HK", "2388.HK"]},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_stock_HK.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_HK_STOCK_lv1")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])

        assert np.isclose(output[0][0].iloc[-1,0], 1935398.5304418395)


    def test_hk_stock_lv1_precision_usd(self):
        pool_setting = {"symbol": {"STOCKS": ["0005.HK", "1299.HK", "2388.HK"]},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_stock_HK_usd.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_HK_STOCK_lv1_usd")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        # output_position = reader.get_strategy(id_list=[0], fields=["pnl"])

        assert np.isclose(output[0][0].iloc[-1,0], 1905699.5490676302)


    # def test_large_sample_size_stock_lv1_precision(self):
    #     my_file = open("tests/engine/backtest/test_precision_stocks_ETFs/cn_stock_symbols.txt", "r")
    #     content_list = my_file.read().split('\n')
    #     # content_list[:300]
    #     pool_setting = {"symbol": {"STOCKS": content_list[:300]},
    #                     "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
    #                     "strategy_name": "EW_strategy",
    #                     "save_path":"tests/large_sample_size_pool.csv",
    #                     "params": {
    #                         # No parameters
    #                     }}
    #     run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_stock_large.json',
    #              strategy_pool_generator(pool_setting, save=False))
    #     reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_large_lv1")
    #     output = reader.get_strategy(id_list=[0], fields=["pnl"])
    #     print(output[0][-1][-1:].values)
    #     assert np.isclose(output[0][0].iloc[-1,0], 625370.9551)

    # def test_super_large_sample_size_stock_lv1_precision(self):
    #
    #     my_file = open("tests/engine/backtest/test_precision_stocks_ETFs/cn_stock_symbols.txt", "r")
    #     content_list = my_file.read().split('\n')
    #     # content_list[:300]
    #     pool_setting = {"symbol": {"STOCKS": content_list[:1000]},
    #                     "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
    #                     "strategy_name": "EW_strategy",
    #                     "save_path":"tests/large_sample_size_pool.csv",
    #                     "params": {
    #                         # No parameters
    #                     }}
    #     run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_stock_super_large.json',
    #              strategy_pool_generator(pool_setting, save=False))
    #     reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_super_large_lv1")
    #     output = reader.get_strategy(id_list=[0], fields=["pnl"])
    #     print(output[0][-1][-1:].values)
    #     assert np.isclose(output[0][0].iloc[-1,0], 979831.94)


    def test_us_etf_precision(self):

        pool_setting = {"symbol": {"STOCKS": ['VTI.US', 'SCHF.US', 'VWO.US', 'VB.US', 'AGG.US', 'EMB.US',
                                              'HYG.US', 'FLOT.US', 'VTIP.US', 'GLD.US', 'XLE.US', 'QQQ.US',
                                              'ASHR.US', 'INDA.US']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_US_ETF.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_US_ETF_lv1")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        #reader.to_csv(export_folder = 'results/us_etf_master', id_list = [0])
        
        assert np.isclose(output[0][0].iloc[-1,0], 1535700.7580639513)


    def test_us_etf_precision_datamaster(self):
        pool_setting = {"symbol": {"STOCKS": ['VTI.US', 'SCHF.US', 'VWO.US', 'VB.US', 'AGG.US', 'EMB.US',
                                              'HYG.US', 'FLOT.US', 'VTIP.US', 'GLD.US', 'XLE.US', 'QQQ.US',
                                              'ASHR.US', 'INDA.US']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_US_ETF_dm.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_US_ETF_lv1_dm")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        # reader.to_csv(export_folder = 'results/us_etf_master', id_list = [0])

        assert np.isclose(output[0][0].iloc[-1, 0], 1535700.7580639513)


    def test_us_etf_precision_hkd(self):

        pool_setting = {"symbol": {"STOCKS": ['VTI.US', 'SCHF.US', 'VWO.US', 'VB.US', 'AGG.US', 'EMB.US',
                                              'HYG.US', 'FLOT.US', 'VTIP.US', 'GLD.US', 'XLE.US', 'QQQ.US',
                                              'ASHR.US', 'INDA.US']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_US_ETF_hkd.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_US_ETF_lv1_hkd")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        #reader.to_csv(export_folder = 'results/us_etf_hkd_master', id_list = [0])
        
        assert np.isclose(output[0][0].iloc[-1,0], 1567973.8443036904)


    def test_hk_etf_precision(self):

        pool_setting = {"symbol": {"STOCKS": ['2813.HK', '3010.HK', '3077.HK', '3081.HK',
                                              '3101.HK', '3140.HK', '3141.HK', '3169.HK']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_HK_ETF.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_HK_ETF_lv1")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        print(output[0][-1][-1:].values)
        #reader.to_csv(export_folder = 'results/hk_etf_master', id_list = [0])

        assert np.isclose(output[0][0].iloc[-1,0], 456208.4550106079)        # 353584.40 matches V21 when check_order is disabled


    def test_hk_etf_precision_usd(self):

        pool_setting = {"symbol": {"STOCKS": ['2813.HK', '3010.HK', '3077.HK', '3081.HK',
                                              '3101.HK', '3140.HK', '3141.HK', '3169.HK']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_HK_ETF_usd.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_HK_ETF_lv1_usd")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        #reader.to_csv(export_folder = 'results/hk_etf_usd_master', id_list = [0])

        assert np.isclose(output[0][0].iloc[-1,0], 436725.984619592)


    def test_hk_etf_precision_usd_datamaster(self):

        pool_setting = {"symbol": {"STOCKS": ['2813.HK', '3010.HK', '3077.HK', '3081.HK',
                                              '3101.HK', '3140.HK', '3141.HK', '3169.HK']},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_HK_ETF_usd_dm.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_HK_ETF_lv1_usd_dm")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        #reader.to_csv(export_folder = 'results/hk_etf_usd_master', id_list = [0])

        assert np.isclose(output[0][0].iloc[-1,0], 436725.984619592)


    def test_mixed_stock_precision(self):

        pool_setting = {"symbol": {"STOCKS": ["600016.SH", "2388.HK", "1299.HK", "601318.SH"]},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1_adj",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_mixed_CN_HK.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_mixed_CN_HK_lv1")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        print(output[0][-1][-1:].values)
        #reader.to_csv(export_folder = 'results/hk_mixed_master', id_list = [0])

        assert np.isclose(output[0][0].iloc[-1,0],353481.466)


    def test_mixed_stock_precision_usd(self):

        pool_setting = {"symbol": {"STOCKS": ["600016.SH", "2388.HK", "1299.HK", "601318.SH"]},
                        "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.EW_lv1_adj",
                        "strategy_name": "EW_strategy",
                        "params": {
                            # No parameters
                        }}
        run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/EW_mixed_CN_HK_usd.json',
                 strategy_pool_generator(pool_setting, save=False))
        reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/EW_mixed_CN_HK_lv1_usd")
        output = reader.get_strategy(id_list=[0], fields=["pnl"])
        #reader.to_csv(export_folder = 'results/hk_mixed_usd_master', id_list = [0])

        assert np.isclose(output[0][0].iloc[-1,0], 344935.427)


    # def test_lv1_2_3_precision(self):
    #     rsi_pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH', '601628.SH',
    #                                               '002024.SZ', '000100.SZ']},
    #                         "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.RSI_lv1",
    #                         "strategy_name": "RSI_strategy",
    #                         "params": {
    #                             "length": [10, 15, 20, 40],
    #                             "break_threshold": [5, 10, 15, 20, 25, 30],
    #                             "stop_profit": [0.01, 0.02],
    #                             "stop_loss": [0.005, 0.01, 0.02],
    #                             "max_hold_days": [10, 20]
    #                         }}
    #
    #     # kd_pool_setting = {"symbol": {"STOCKS": ['600000.SH', '000005.SZ', '600519.SH', '601318.SH', '601988.SH',
    #     #                                       '002415.SZ', '000917.SZ']},
    #     #                 "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.KD_lv1",
    #     #                 "strategy_name": "KD_strategy",
    #     #                 "params": {
    #     #                     "length1": [10, 20],
    #     #                     "length2": [3],
    #     #                     "length3": [3],
    #     #                     "break_threshold": [10, 20],
    #     #                     "stop_profit": [0.01],
    #     #                     "stop_loss": [0.005],
    #     #                     "max_hold_days": [10]
    #     #                 }}
    #
    #     rsi_lv2_pool_setting = {"symbol": {"STOCKS": ['600016.SH', '000005.SZ', '601398.SH', '600028.SH',
    #                                                   '601628.SH', '002024.SZ', '000100.SZ'],
    #                                        "RSI": list(np.arange(288 * 7).astype(np.float64))},  # 不转成float json会报错
    #                             "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.RSI_lv2",
    #                             "strategy_name": "Allocation_strategy",
    #                             "params": {
    #
    #                             }}
    #
    #     # kd_lv2_pool_setting = {"symbol": {"STOCKS": ['600000.SH', '000005.SZ', '600519.SH', '601318.SH',
    #     #                                       '601988.SH', '002415.SZ', '000917.SZ'],
    #     #                            "KD": list(np.arange(7 * 288).astype(np.float64))},  # 不转成float json会报错
    #     #                 "strategy_module": "tests.engine.backtest.test_precision_stocks_ETFs.algorithm.KD_lv2",
    #     #                 "strategy_name": "Allocation_strategy",
    #     #                 "params": {
    #     #
    #     #                 }}
    #     run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/RSI_stock_lv1.json',
    #              strategy_pool_generator(rsi_pool_setting, save=False))
    #     # run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/KD_stock_lv1.json',
    #     #          strategy_pool_generator(kd_pool_setting, save=False))
    #     run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/RSI_stock_lv2.json',
    #              strategy_pool_generator(rsi_lv2_pool_setting, save=False))
    #     # run_once('tests/engine/backtest/test_precision_stocks_ETFs/config/KD_stock_lv2.json',
    #     #          strategy_pool_generator(kd_lv2_pool_setting, save=False))
    #
    #     reader = ResultReader("tests/engine/backtest/test_precision_stocks_ETFs/result/RSI_lv2")
    #     output = reader.get_strategy(id_list=[0], fields=["pnl"])
    #     print(output)
    #
    #     # assert np.isclose(output[0][0].iloc[-1,0], 12.8293)     #! this will fail



if __name__ == '__main__':
    unittest.main()
