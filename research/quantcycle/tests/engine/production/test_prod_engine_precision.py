import os
import unittest
from datetime import datetime
import json
import numpy as np
import pandas as pd
from quantcycle.utils import get_logger
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.app.order_crosser.order_router import \
    TestOrderRouter as OrderRouter
from quantcycle.engine.quant_engine import QuantEngine
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator

from engine_manager import EngineManager
import json

class FX(unittest.TestCase):

    def test_fx_daily_01(self):
        # if os.path.exists("tests/engine/production/production_engine_test_case/fx_daily/results"):
        #     os.remove("tests/engine/production/production_engine_test_case/fx_daily/results")

        quant_engine = QuantEngine()
        risk_config = {"active": False,
                       "Order_flow_limit": None,
                       "Order_size_limit": 200000000,
                       "Trade_limit": 2000000000,
                       "Order_cancel_limit": 100,
                       "time_interval": None,
                       "security_weight_limit": [1, 1, 1],
                       "industry_limit": [1],
                       "black_list": [0]}
        quant_engine.load_config(
        {
            "start_year": 2019,
            "start_month": 10,
            "start_day": 15,
            "end_year": 2019,
            "end_month": 11,
            "end_day": 15,
          
            "data": {
                "FX": {"DataCenter": "DataMaster",
                       "SymbolArgs": {
                           "DataSource": "BGNL",
                           "BackupDataSource": "BGNL"},
                       "Fields": "OHLC",
                       "Frequency": "DAILY"}
            },
            "secondary_data": {
            },
            "account": {
                "cash": 4000000,
            },
            "algo": {
                "base_ccy": "USD",
                "window_size": {
                    "main": 1
                },
                "monitor_open": False
            },
            "result_output": {
                "flatten": False,
                "save_dir": "tests/engine/production/production_engine_test_case/fx_daily/results",
                "save_name": "real_result_0",
            },
            "ref_data": {
            },
            "optimization": {
                "numba_parallel": False
            },
            "engine": {
                "engine_name": "test_fx_daily_01",
            }
        },pd.read_csv("tests/engine/production/production_engine_test_case/fx_daily/strategy_pool.csv"))
        quant_engine.load_app()

        path = "tests/engine/production/production_engine_test_case/fx_daily"

        ##
        name = "current_data"
        symbols = ["AUDCAD", "EURUSD", "USDJPY"]
        data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%Y-%m-%d', utc=True)
        data_df.drop(columns=["open_AUDCAD.FX", "high_AUDCAD.FX", "low_AUDCAD.FX", "open_EURUSD.FX", "high_EURUSD.FX",
                              "low_EURUSD.FX", "open_USDJPY.FX", "high_USDJPY.FX", "low_USDJPY.FX"], inplace=True)
        data_df = data_df.loc['2019-10-15':'2019-11-15']

        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data_df_array = data_df.values
        temp_symbols = list(map(lambda x: f"{x}", symbols))

        ##
        ccys = ["AUDCAD", "USDJPY", "EURUSD"]
        name = "current_fx_data"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%Y-%m-%d', utc=True)
        fx_data_df = fx_data_df.loc['2019-10-15':'2019-11-15']
        fx_data_df_array = fx_data_df.values

        ##
        name = "current_rate_data"
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%Y-%m-%d', utc=True)
        rate_data_df = rate_data_df.loc['2019-10-15':'2019-11-15']

        rate_data_df.drop(columns=["AUDCAD.FX_BID", "AUDCAD.FX_ASK", "EURUSD.FX_BID", "EURUSD.FX_ASK", "USDJPY.FX_BID",
                                   "USDJPY.FX_ASK"], inplace=True)
        rate_data_df_array = rate_data_df.values / 10000

        for i in range(len(data_df_array)):
            current_data = np.array(data_df_array[i][:len(temp_symbols)])
            current_data = np.array(list(map(lambda x: np.array([0,0,0,x]),current_data)))
            current_data = dict(zip(temp_symbols, current_data))

            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]),current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))

            date = datetime.utcfromtimestamp(data_df_array[i][-1]).date()
            time_array = np.array( [data_df_array[i][-1],data_df_array[i][-1],date.weekday(),date.year,date.month,date.day,0,0,0] ).astype(np.int64)
            ts = np.array(time_array)
            #ts = np.array(list(map(lambda x: np.array([x]),time_array)))

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts[3:6]))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts[3:6]))

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/fx_daily/results", f"real_result_0.hdf5")
        result_reader = ResultReader(path_name)
        result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/fx_daily/results', id_list=[0])
        pnl = pd.read_csv(os.path.join(path, "results/0/pnl.csv"))
        result = np.sum(pnl.iloc[-1:, -1:].values)
        assert np.isclose(result, 15785.02189)

    def test_fx_daily_ndf_01(self):

        quant_engine = QuantEngine()
        quant_engine.load_config(
            {
                "start_year": 2019,
                "start_month": 10,
                "start_day": 15,
                "end_year": 2019,
                "end_month": 11,
                "end_day": 15,
              
                "data": {
                    "FX": {"DataCenter": "DataMaster",
                           "SymbolArgs": {
                               "DataSource": "BGNL",
                               "BackupDataSource": "BGNL"},
                           "Fields": "OHLC",
                           "Frequency": "DAILY"}
                },
                "secondary_data": {
                },
                "account": {
                    "cash": 4000000,
                },
                "algo": {
                    "base_ccy": "USD",
                    "window_size": {
                        "main": 1
                    },
                    "monitor_open": False
                },
                "result_output": {
                    "flatten": False,
                    "save_dir": "tests/engine/production/production_engine_test_case/fx_daily_ndf/results",
                    "save_name": "real_result_0",
                },
                "ref_data": {
                },
                "optimization": {
                    "numba_parallel": False
                },
                "engine": {
                    "engine_name": "test_fx_daily_ndf_01",
                }
            }, pd.read_csv("tests/engine/production/production_engine_test_case/fx_daily_ndf/strategy_pool.csv"))


        quant_engine.load_app()



        path = "tests/engine/production/production_engine_test_case/fx_daily_ndf"

        ##
        name = "current_data"
        symbols = ["EURUSD.FX", "USDIDR.FX", "USDKRW.FX"]
        data_df = pd.read_csv(os.path.join(path, name + ".csv"),index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%Y-%m-%d', utc=True)
        data_df.drop(columns=["open_EURUSD.FX", "high_EURUSD.FX", "low_EURUSD.FX", "open_USDIDR.NDF", "high_USDIDR.NDF",
                              "low_USDIDR.NDF", "open_USDKRW.NDF", "high_USDKRW.NDF", "low_USDKRW.NDF"], inplace=True)
        data_df = data_df.loc['2019-10-15':'2019-11-15']

        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data_df_array = data_df.values
        temp_symbols = list(map(lambda x: x.split(".")[0], symbols))

        ##
        ccys = ["USDIDR", "USDKRW", "EURUSD"]
        name = "current_fx_data"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%Y-%m-%d', utc=True)
        fx_data_df = fx_data_df.loc['2019-10-15':'2019-11-15']
        fx_data_df_array = fx_data_df.values

        ##
        name = "current_rate_data"
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%Y-%m-%d', utc=True)
        rate_data_df = rate_data_df.loc['2019-10-15':'2019-11-15']

        rate_data_df.drop(
            columns=["EURUSD.FX_BID", "EURUSD.FX_ASK", "USDIDR.NDF_BID", "USDIDR.NDF_ASK", "USDKRW.NDF_BID",
                     "USDKRW.NDF_ASK"], inplace=True)
        rate_data_df_array = rate_data_df.values / 10000

        for i in range(len(data_df_array)):
            current_data = np.array(data_df_array[i][:len(temp_symbols)])
            current_data = np.array(list(map(lambda x: np.array([0,0,0,x]),current_data)))
            current_data = dict(zip(temp_symbols, current_data))

            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]),current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))

            date = datetime.utcfromtimestamp(data_df_array[i][-1]).date()
            time_array = np.array( [data_df_array[i][-1],data_df_array[i][-1],date.weekday(),date.year,date.month,date.day,0,0,0] ).astype(np.int64)
            ts = np.array(time_array)
            #ts = np.array(list(map(lambda x: np.array([x]),time_array)))

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts))
            
            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts))

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()


        path_name = os.path.join("tests/engine/production/production_engine_test_case/fx_daily_ndf/results", f"real_result_0.hdf5")
        result_reader = ResultReader(path_name)
        result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/fx_daily_ndf/results', id_list=[0])
        pnl = pd.read_csv(os.path.join(path, "results/0/pnl.csv"))


        assert np.isclose(np.sum(pnl.iloc[-1:, -1:].values), -37136.4380190477)

    def test_fx_daily_with_commission_01(self):

        quant_engine = QuantEngine()
        risk_config = {"active": False,
                       "Order_flow_limit": None,
                       "Order_size_limit": 200000000,
                       "Trade_limit": 2000000000,
                       "Order_cancel_limit": 100,
                       "time_interval": None,
                       "security_weight_limit": [1, 1, 1],
                       "industry_limit": [1],
                       "black_list": [0]}


        quant_engine.load_config(
            {
                "start_year": 2019,
                "start_month": 10,
                "start_day": 14,
                "end_year": 2019,
                "end_month": 10,
                "end_day": 14,
              
                "data": {
                    "FX": {"DataCenter": "DataMaster",
                           "SymbolArgs": {
                               "DataSource": "BGNL",
                               "BackupDataSource": "BGNL"},
                           "Fields": "OHLC",
                           "Frequency": "DAILY"}
                },
                "secondary_data": {
                },
                "account": {
                    "cash": 4000000,
                },
                "algo": {
                    "base_ccy": "USD",
                    "window_size": {
                        "main": 1
                    },
                    "monitor_open": False
                },
                "result_output": {
                    "flatten": False,
                    "save_dir": "tests/engine/production/production_engine_test_case/fx_daily_with_com/results",
                    "save_name": "real_result_0",
                },
                "ref_data": {
                },
                "optimization": {
                    "numba_parallel": False
                },
                "engine": {
                    "engine_name": "test_fx_daily_with_commission_01",
                }
            }, pd.read_csv("tests/engine/production/production_engine_test_case/fx_daily_with_com/strategy_pool.csv"))

        quant_engine.load_app(OrderRouter(commission_fee=0.1))


        path = "tests/engine/production/production_engine_test_case/fx_daily_with_com"

        ##
        name = "current_data"
        symbols = ["AUDCAD", "EURUSD", "USDJPY"]
        data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%Y-%m-%d', utc=True)
        data_df.drop(columns=["open_AUDCAD.FX", "high_AUDCAD.FX", "low_AUDCAD.FX", "open_EURUSD.FX", "high_EURUSD.FX",
                              "low_EURUSD.FX", "open_USDJPY.FX", "high_USDJPY.FX", "low_USDJPY.FX"], inplace=True)
        data_df = data_df.loc['2019-10-15':'2019-11-15']

        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data_df_array = data_df.values
        temp_symbols = list(map(lambda x: f"{x}", symbols))

        ##
        ccys = ["AUDCAD", "USDJPY", "EURUSD"]
        name = "current_fx_data"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%Y-%m-%d', utc=True)
        fx_data_df = fx_data_df.loc['2019-10-15':'2019-11-15']
        fx_data_df_array = fx_data_df.values

        ##
        name = "current_rate_data"
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%Y-%m-%d', utc=True)
        rate_data_df = rate_data_df.loc['2019-10-15':'2019-11-15']

        rate_data_df.drop(columns=["AUDCAD.FX_BID", "AUDCAD.FX_ASK", "EURUSD.FX_BID", "EURUSD.FX_ASK", "USDJPY.FX_BID",
                                   "USDJPY.FX_ASK"], inplace=True)
        rate_data_df_array = rate_data_df.values / 10000

        for i in range(len(data_df_array)):
            current_data = np.array(data_df_array[i][:len(temp_symbols)])
            current_data = np.array(list(map(lambda x: np.array([0,0,0,x]),current_data)))
            current_data = dict(zip(temp_symbols, current_data))

            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]),current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))

            date = datetime.utcfromtimestamp(data_df_array[i][-1]).date()
            time_array = np.array( [data_df_array[i][-1],data_df_array[i][-1],date.weekday(),date.year,date.month,date.day,0,0,0] ).astype(np.int64)
            ts = np.array(time_array)

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts))

            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts))

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/fx_daily_with_com/results", f"real_result_0.hdf5")
        result_reader = ResultReader(path_name)
        pnl=result_reader.get_strategy(id_list=[0],fields=['pnl'])
        # result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/fx_daily_with_com/results', id_list=[0])
        result = pnl[0][0][-1:].values
        assert np.isclose(result, -400436)

    @unittest.skip("fx data change from ccy to symbol")
    def test_fx_daily_weight_01(self):

        quant_engine = QuantEngine()
        quant_engine.load_config(
            {
                "start_year": 2020,
                "start_month": 1,
                "start_day": 1,
                "end_year": 2020,
                "end_month": 1,
                "end_day": 1,
              
                "data": {
                    "FX": {"DataCenter": "DataMaster",
                           "SymbolArgs": {
                               "DataSource": "BGNL",
                               "BackupDataSource": "BGNL"},
                           "Fields": "OHLC",
                           "Frequency": "DAILY"}
                },
                "secondary_data": {
                },
                "account": {
                    "cash": 2900000,
                },
                "algo": {
                    "base_ccy": "USD",
                    "window_size": {
                        "main": 1
                    },
                    "monitor_open": False
                },
                "result_output": {
                    "flatten": False,
                    "save_dir": "tests/engine/production/production_engine_test_case/fx_daily_weight_strategy/results",
                    "save_name": "real_result_0",
                },
                "ref_data": {
                },
                "optimization": {
                    "numba_parallel": False
                },
                "engine": {
                    "engine_name": "tests/engine/production/production_engine_test_case/fx_daily_weight_strategy/results",
                }
            }, pd.read_csv("tests/engine/production/production_engine_test_case/fx_daily_weight_strategy/strategy_pool.csv"))

        quant_engine.load_app(OrderRouter(commission_pool_path='tests/engine/production/production_engine_test_case/commission_pool/aqm_turnover_union_fx_weight_strategy.csv'))

        path = "tests/engine/production/production_engine_test_case/fx_daily_weight_strategy"

        #
        ##
        name = "current"
        symbols1 = ["AUDCAD","AUDJPY","AUDNZD","AUDUSD","CADCHF","CADJPY","EURCAD","EURCHF","EURGBP","EURJPY","EURUSD","GBPCAD","GBPCHF","GBPJPY","GBPUSD","NOKSEK","NZDJPY","NZDUSD","USDCAD","USDCHF","USDIDR","USDINR","USDJPY","USDKRW","USDNOK","USDSEK","USDSGD","USDTHB","USDTWD"]

        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols=list(map(lambda x: f"{x}", symbols1))+['date']

        data_df = pd.read_csv(os.path.join(path, name + ".csv"),usecols= df_symbols,index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2020-02-28']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data_df_array = data_df.values


        ##
        ccys = ["CAD", "JPY", "NZD", "USD", "CHF", "SGD", "THB", "TWD", "GBP", "IDR", "INR", "KRW", "NOK", "SEK"]

        df_symbols = ccys + ['date']
        name = "fx"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%m/%d/%Y', utc=True)
        fx_data_df = fx_data_df.loc['2012-01-01':'2020-02-28']
        fx_data_df_array = fx_data_df.values

        ##
        name = "rate"
        df_symbols = list(map(lambda x: f"{x}", symbols1))+ ['date']
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%m/%d/%Y', utc=True)
        rate_data_df = rate_data_df.loc['2012-01-01':'2020-02-28']


        rate_data_df_array = rate_data_df.values

        for i in range(len(data_df_array)):
            current_data = np.array(data_df_array[i][:len(temp_symbols)])
            current_data = np.array(list(map(lambda x: np.array([0, 0, 0, x]), current_data)))
            current_data = dict(zip(temp_symbols, current_data))

            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]), current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))

            date = datetime.utcfromtimestamp(data_df_array[i][-1]).date()
            time_array = np.array(
                [data_df_array[i][-1], data_df_array[i][-1], date.weekday(), date.year, date.month, date.day, 0, 0,
                 0]).astype(np.int64)
            ts = np.array(time_array)

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts))

            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts))

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/fx_daily_weight_strategy/results", f"real_result_0")
        result_reader = ResultReader(path_name)
        result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/fx_daily_weight_strategy/results', id_list=[0])
        pnl = pd.read_csv(os.path.join("tests/engine/production/production_engine_test_case/fx_daily_weight_strategy/results", "0/pnl.csv"))
        assert np.isclose(np.sum(pnl.iloc[-2:-1, 1:].values), -342646.01)

    def test_RSI_lv1_data_loader(self):

        ####lv1
        # 'EURGBP', 'EURJPY',
        """ RSI_lv1_pool_setting = {"symbol":{"FX":['EURGBP','EURJPY','AUDNZD']},
                                "strategy_module": "tests.test_case.test_RSI_lv1_data_loader.algorithm.RSI_lv1",
                                "strategy_name": "RSI_strategy",
                                "save_path": "tests/engine/production/production_engine_test_case/test_RSI_lv1_data_loader/strategy_pool/RSI_lv1_strategy_pool.csv",
                                "params": {
                                    "length": [10],
                                    "break_threshold": [10,20,30],
                                    "stop_profit": [0.01],
                                    "stop_loss": [0.005],
                                    "max_hold_days": [10]
                                }}
        strategy_pool_generator(RSI_lv1_pool_setting) """

        quant_engine = QuantEngine()

        quant_engine.load_config(json.load(open('tests/engine/production/production_engine_test_case/test_RSI_lv1_data_loader/config/combination.json')),
            pd.read_csv("tests/engine/production/production_engine_test_case/test_RSI_lv1_data_loader/strategy_pool/RSI_lv1_strategy_pool.csv"))

        quant_engine.load_app(OrderRouter())
        quant_engine.load_data()
        #quant_engine.streaming_data_queue.put(("END", 0, 0))

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/test_RSI_lv1_data_loader/results", f"rsi_lv1.hdf5")
        result_reader = ResultReader(path_name)

        # result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/test_RSI_lv1_data_loader/results',
        #                       id_list=[0,1,2])

        pnl=result_reader.get_strategy(id_list=[0,1,2],fields=["pnl"])
        total=0
        for id,pv in pnl.items():
            total+=np.sum(pnl[id][0][-1:].values)
        get_logger.get_logger().info(f"total:{total}")
        assert np.isclose(total,-133576.95)

    def test_RSI_lv1(self):

        ####lv1
        # 'EURGBP', 'EURJPY',


        quant_engine = QuantEngine()

        quant_engine.load_config(json.load(open('tests/engine/production/production_engine_test_case/test_RSI_lv1/config/combination.json')),
            pd.read_csv("tests/engine/production/production_engine_test_case/test_RSI_lv1/strategy_pool/RSI_lv1_strategy_pool.csv"))

        quant_engine.load_app(OrderRouter())

        path = "tests/engine/production/production_engine_test_case/test_RSI_lv1/data"
        ##
        name = "current_lv1"
        symbols1 = ['AUDNZD','EURGBP','EURJPY']
        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']

        data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2014-02-28']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data_df_array = data_df.values


        ##
        ccys = ['AUDNZD','EURGBP','EURJPY']

        df_symbols = ccys + ['date']
        name = "fx_lv1"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%m/%d/%Y', utc=True)
        fx_data_df = fx_data_df.loc['2012-01-01':'2014-02-28']
        fx_data_df_array = fx_data_df.values

        ##
        name = "rate_lv1"
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%m/%d/%Y', utc=True)
        rate_data_df = rate_data_df.loc['2012-01-01':'2014-02-28']
        rate_data_df_array = rate_data_df.values
        ##

        for i in range(len(data_df_array) - 1):
            current_data = np.array(data_df_array[i][:len(temp_symbols)])
            current_data = np.array(list(map(lambda x: np.array([0, 0, 0, x]), current_data)))
            current_data = dict(zip(temp_symbols, current_data))

            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]), current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))

            date = datetime.utcfromtimestamp(data_df_array[i][-1]).date()
            time_array = np.array(
                [data_df_array[i][-1], data_df_array[i][-1], date.weekday(), date.year, date.month, date.day, 0, 0,
                 0]).astype(np.int64)
            ts = np.array(time_array)

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts[3:6]))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts[3:6]))

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()


        path_name = os.path.join("tests/engine/production/production_engine_test_case/test_RSI_lv1/results", f"rsi_lv1.hdf5")
        result_reader = ResultReader(path_name)

        # result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/test_RSI_lv1/results',
        #                      id_list=[0,1,2])

        pnl=result_reader.get_strategy(id_list=[0,1,2],fields=["pnl"])
        total=0
        for id,pv in pnl.items():
            total+=np.sum(pnl[id][0][-1:].values)
        get_logger.get_logger().info(f"total:{total}")
        print(total)
        assert np.isclose(total,-133577)

    def test_RSI_lv2(self):

        ####lv1
        # 2012-01-01  2014-02-28  3params  flatten=true   9 strategy in total



        quant_engine = QuantEngine()

        quant_engine.load_config(json.load(open('tests/engine/production/production_engine_test_case/test_RSI_lv2/config/combination.json')),
                                 pd.read_csv("tests/engine/production/production_engine_test_case/test_RSI_lv2/strategy_pool/RSI_lv2_strategy_pool.csv"))
        quant_engine.load_app(OrderRouter())

        path = "tests/engine/production/production_engine_test_case/test_RSI_lv2/data"

        #
        ##
        name = "current_lv1"
        symbols1 = ['AUDNZD','EURGBP','EURJPY']
        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols=list(map(lambda x: f"{x}", symbols1))+['date']

        data_df = pd.read_csv(os.path.join(path, name + ".csv"),usecols= df_symbols,index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2014-02-28']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data_df_array = data_df.values


        ##
        ccys = ['AUDNZD','EURGBP','EURJPY']

        df_symbols = ccys + ['date']
        name = "fx_lv1"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%m/%d/%Y', utc=True)
        fx_data_df = fx_data_df.loc['2012-01-01':'2014-02-28']
        fx_data_df_array = fx_data_df.values

        ##
        name = "rate_lv1"
        df_symbols = list(map(lambda x: f"{x}", symbols1))+ ['date']
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%m/%d/%Y', utc=True)
        rate_data_df = rate_data_df.loc['2012-01-01':'2014-02-28']
        rate_data_df_array = rate_data_df.values
        ##
        name = "pnl_lv1"
        ids=["0","1","2","3","4","5","6","7","8"]

        df_symbols = ids + ['date']
        pnl_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        pnl_data_df.index = pd.to_datetime(pnl_data_df.index, format='%m/%d/%Y', utc=True)
        pnl_data_df = pnl_data_df.loc['2012-01-02':'2014-02-28']
        pnl_data_df["ts"] = list(map(lambda x: x.timestamp(), pnl_data_df.index))
        pnl_data_df_array = pnl_data_df.values
        ##

        ##
        name = "position_lv1"
        position_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        position_data_df.index = pd.to_datetime(position_data_df.index, format='%m/%d/%Y', utc=True)
        position_data_df = position_data_df.loc['2012-01-02':'2014-02-28']
        position_data_df_array = position_data_df.values
        ##

        ##
        name = "metrics_252_lv1"
        metrics_252_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        metrics_252_data_df.index = pd.to_datetime(metrics_252_data_df.index, format='%m/%d/%Y', utc=True)
        metrics_252_data_df["ts"] = list(map(lambda x: x.timestamp(), metrics_252_data_df.index))
        metrics_252_data_df_array = metrics_252_data_df.values
        ##
        name = "metrics_61_lv1"
        metrics_61_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        metrics_61_data_df.index = pd.to_datetime(metrics_61_data_df.index, format='%m/%d/%Y', utc=True)
        metrics_61_data_df["ts"] = list(map(lambda x: x.timestamp(), metrics_61_data_df.index))
        metrics_61_data_df_array = metrics_61_data_df.values

        

        for i in range(len(data_df_array)-1):
            current_data = np.array(data_df_array[i][:len(temp_symbols)])
            current_data = np.array(list(map(lambda x: np.array([0, 0, 0, x]), current_data)))
            current_data = dict(zip(temp_symbols, current_data))

            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]), current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))

            date = datetime.utcfromtimestamp(data_df_array[i][-1]).date()
            time_array = np.array(
                [data_df_array[i][-1], data_df_array[i][-1], date.weekday(), date.year, date.month, date.day, 0, 0,
                 0]).astype(np.int64)
            ts = np.array(time_array)




            temp_secondary_data={}
            #temp_secondary_data["RSI_lv1"+ '_' + "ID_symbol_map"]
            temp_secondary_data["ID_symbol_map"] = {}
            temp_secondary_data["ID_symbol_map"]["RSI_lv1"] = np.array([['AUDNZD'],
                                                                        ['EURGBP'],
                                                                        ['EURJPY'],
                                                                        ['AUDNZD'],
                                                                        ['EURGBP'],
                                                                        ['EURJPY'],
                                                                        ['AUDNZD'],
                                                                        ['EURGBP'],
                                                                        ['EURJPY']], dtype = object)
            #temp_secondary_data["ID_symbol_map"]["RSI_lv1"]
            temp_secondary_data["strategy_id"]=0

            temp_secondary_data["RSI_lv1"+ '_' + "pnl"]={}
            temp_secondary_data["RSI_lv1" + '_' + "pnl"]["data"]=np.array(pnl_data_df_array[i][:len(temp_symbols)*3])[None,:][:,:,None]

            temp_secondary_data["RSI_lv1" + '_' + "pnl"]["time"] = ts.reshape(-1,9)

            temp_secondary_data["RSI_lv1" + '_' + "position"] = {}
            temp_secondary_data["RSI_lv1" + '_' + "position"]["data"] = np.array(position_data_df_array[i][:len(temp_symbols)*3])[None,:][:,:,None]
            temp_secondary_data["RSI_lv1" + '_' + "position"]["time"] = ts.reshape(-1,9)

            k=-1
            while metrics_61_data_df_array[k+1][-1]<= pnl_data_df_array[i][-1]:
                  k+=1

            if k != -1:
                date1 = datetime.utcfromtimestamp(metrics_61_data_df_array[k][-1]).date()
                time_array1 = np.array(
                    [metrics_61_data_df_array[k][-1], metrics_61_data_df_array[k][-1], date1.weekday(), date1.year, date1.month, date1.day, 0, 0,
                     0]).astype(np.int64)
                ts1 = np.array(list(map(lambda x: np.array([x]), time_array1)))
                temp_secondary_data["RSI_lv1" + '_' + "metrics_61"] = {}
                temp_secondary_data["RSI_lv1" + '_' + "metrics_61"]["data"] = np.array(
                    metrics_61_data_df_array[k][:len(temp_symbols)*3])[None,:][:,:,None]

                temp_secondary_data["RSI_lv1" + '_' + "metrics_61"]["time"] = ts1.reshape(-1,9)
            else:
                temp_secondary_data["RSI_lv1" + '_' + "metrics_61"] = {}

            k=-1
            while metrics_252_data_df_array[k+1][-1]<= pnl_data_df_array[i][-1]:
                  k+=1

            if k != -1:
                date2 = datetime.utcfromtimestamp(metrics_252_data_df_array[k][-1]).date()
                time_array2 = np.array(
                    [metrics_252_data_df_array[k][-1], metrics_252_data_df_array[k][-1], date2.weekday(), date2.year, date2.month, date2.day, 0, 0,
                     0]).astype(np.int64)
                ts2 = np.array(list(map(lambda x: np.array([x]), time_array2)))
                temp_secondary_data["RSI_lv1" + '_' + "metrics_252"] = {}
                temp_secondary_data["RSI_lv1" + '_' + "metrics_252"]["data"] = np.array(
                    metrics_252_data_df_array[k][:len(temp_symbols)*3])[None,:][:,:,None]

                temp_secondary_data["RSI_lv1" + '_' + "metrics_252"]["time"] = ts2.reshape(-1,9)
            else:
                temp_secondary_data["RSI_lv1" + '_' + "metrics_252"] = {}

            #
            id_map = {0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1, 6: 2, 7: 2, 8: 2}
            temp_secondary_data["id_map"] = {}
            temp_secondary_data["id_map"]['RSI_lv1'] = id_map

            quant_engine.streaming_data_queue.put(("secondary_data", temp_secondary_data, ts))
            get_logger.get_logger().info(("secondary_data", temp_secondary_data, ts[3:6]))

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts[3:6]))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts[3:6]))


        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/test_RSI_lv2/results", f"rsi_lv2.hdf5")

        result_reader = ResultReader(path_name)

        result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/test_RSI_lv2/results' ,id_list=[0])

        pnl=result_reader.get_strategy(id_list=[0],fields=["pnl"])
        total=0
        for id,pv in pnl.items():
            total+=np.sum(pnl[id][0][-1:].values)
        get_logger.get_logger().info(f"total:{total}")
        assert np.isclose(total, 4916.44)

    def test_engine_manager_RSI(self):

        engine_manager = EngineManager()

        engine_manager.add_engine(json.load(open(r'tests/engine/production/production_engine_test_case/test_RSI/config/lv1.json')),pd.read_csv(r"tests/engine/production/production_engine_test_case/test_RSI/strategy_pool/RSI_lv1_strategy_pool.csv"))
        engine_manager.add_engine(json.load(open(r'tests/engine/production/production_engine_test_case/test_RSI/config/lv2.json')),pd.read_csv(r"tests/engine/production/production_engine_test_case/test_RSI/strategy_pool/RSI_lv2_strategy_pool.csv"))

        load_data_event = engine_manager.load_engine_data()
        engine_manager.start_engine()
        engine_manager.run()
        load_data_event.wait()

        timepoint = "20121231"
        engine_manager.handle_current_fx_data(timepoint)
        handle_current_data_event,_ = engine_manager.handle_current_data(timepoint)
        handle_current_data_event.wait()
        engine_manager.handle_current_rate_data(timepoint)

        get_logger.get_logger().info("finish handle data")

        engine_manager.kill_engine()
        engine_manager.wait_engine()
        engine_manager.kill()
        engine_manager.wait()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/test_RSI/results", f"RSI_lv2.hdf5")
        result_reader = ResultReader(path_name)
        # result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/test_RSI/results_level2' ,id_list=[0])
        position=result_reader.get_strategy(id_list=[0],fields=["position"])

        get_logger.get_logger().info(f"position:{position[0]}")
        assert np.isclose(np.sum(position[0][-1][-1:].values), -931057.4885)

    # originally commented
    def test_KD_lv1(self):
    
        ##lv1
        # 2012-01-01  2020-01-01  4params  flatten=true   12 strategy in total
        quant_engine = QuantEngine()
    
        quant_engine.load_config(json.load(open('tests/engine/production/production_engine_test_case/test_KD/config/KD_lv1.json')),
            pd.read_csv("tests/engine/production/production_engine_test_case/test_KD/strategy_pool/KD_lv1_strategy_pool.csv"))
        
        quant_engine.load_app(OrderRouter(commission_pool_path = 'tests/engine/production/production_engine_test_case/commission_pool/aqm_turnover_union_fx_weight_strategy.csv'))
        
        # commission_pool_path = 'tests/engine/production/production_engine_test_case/commission_pool/aqm_turnover_union_fx.csv'))
        path = "tests/engine/production/production_engine_test_case/test_KD/data"
    
        #
        ##
        name = "current_0"
        symbols1 = ["GBPUSD", "NOKSEK", "NZDJPY"]
        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']
    
        data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2020-01-01']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data0_df_array = data_df.values
    
        name = "current_1"
        symbols1 = ["GBPUSD", "NOKSEK", "NZDJPY"]
        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']
    
        data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2020-01-01']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data1_df_array = data_df.values
    
        name = "current_2"
        symbols1 = ["GBPUSD", "NOKSEK", "NZDJPY"]
        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']
    
        data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2020-01-01']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data2_df_array = data_df.values
    
        name = "current_3"
        symbols1 = ["GBPUSD", "NOKSEK", "NZDJPY"]
        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']
    
        data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2020-01-01']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data3_df_array = data_df.values
    
        ##
        ccys = ["GBPUSD", "NOKSEK", "NZDJPY"]
        
        df_symbols = ccys + ['date']
        name = "fx_lv1"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%m/%d/%Y', utc=True)
        fx_data_df = fx_data_df.loc['2012-01-01':'2020-01-01']
        fx_data_df_array = fx_data_df.values
    
        ##
        name = "rate_lv1"
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%m/%d/%Y', utc=True)
        rate_data_df = rate_data_df.loc['2012-01-01':'2020-01-01']
        rate_data_df_array = rate_data_df.values
        ##
    
    
    
        for i in range(len(data0_df_array) - 1):
            current_data0 = np.array(data0_df_array[i][:len(temp_symbols)])
            current_data1 = np.array(data1_df_array[i][:len(temp_symbols)])
            current_data2 = np.array(data2_df_array[i][:len(temp_symbols)])
            current_data3 = np.array(data3_df_array[i][:len(temp_symbols)])
            c0 = np.hstack((current_data0[0],current_data1[0],current_data2[0],current_data3[0]))
            c1 = np.hstack((current_data0[1], current_data1[1], current_data2[1], current_data3[1]))
            c2 = np.hstack((current_data0[2], current_data1[2], current_data2[2], current_data3[2]))
    
    
            current_data=np.array([c0,c1,c2])
    
    
            current_data = dict(zip(temp_symbols, current_data))
    
            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]), current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))
    
            date = datetime.utcfromtimestamp(data0_df_array[i][-1]).date()
            time_array = np.array(
                [data0_df_array[i][-1], data0_df_array[i][-1], date.weekday(), date.year, date.month, date.day, 0, 0,
                 0]).astype(np.int64)
            ts = np.array(time_array)

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))
    
            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts[3:6]))
    
            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts[3:6]))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))
    
            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts[3:6]))
    
        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()
    
        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()
    
        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()
    
        path_name = os.path.join("tests/engine/production/production_engine_test_case/test_KD/results", f"KD_lv1.hdf5")
    
        result_reader = ResultReader(path_name)
    
        # result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/test_KD/results', id_list=[0,1,2,3])
    
        pnl = result_reader.get_strategy(id_list=[0,1,2,3], fields=["pnl"])
        total = 0
        for id, pv in pnl.items():
            total += np.sum(pnl[id][0][-2:-1].values)
        get_logger.get_logger().info(f"total:{total}")
        assert np.isclose(total, 101215.82)

        id0:-106594.934912735

    def test_KD_lv2(self):

        ####KD1
        # 2012-01-01  2020-01-01  4params  flatten=true   12 strategy in total

        quant_engine = QuantEngine()

        quant_engine.load_config(json.load(open('tests/engine/production/production_engine_test_case/test_KD/config/KD_lv2.json')),
            pd.read_csv("tests/engine/production/production_engine_test_case/test_KD/strategy_pool/KD_lv2_strategy_pool.csv"))
        quant_engine.load_app(OrderRouter(commission_pool_path='tests/engine/production/production_engine_test_case/commission_pool/aqm_turnover_union_fx_weight_strategy.csv'))

        path = "tests/engine/production/production_engine_test_case/test_KD/data"

        #
        ##
        name = "current_lv1"
        symbols1 = ['GBPUSD','NOKSEK','NZDJPY']
        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']

        data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2020-01-01']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data_df_array = data_df.values

        ##
        ccys = ['GBPUSD','NOKSEK','NZDJPY']

        df_symbols = ccys + ['date']
        name = "fx_lv1"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%m/%d/%Y', utc=True)
        fx_data_df = fx_data_df.loc['2012-01-01':'2020-01-01']
        fx_data_df_array = fx_data_df.values

        ##
        name = "rate_lv1"
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%m/%d/%Y', utc=True)
        rate_data_df = rate_data_df.loc['2012-01-01':'2020-01-01']
        rate_data_df_array = rate_data_df.values
        ##
        name = "pnl_lv1"
        ids = ["0", "1", "2", "3", "4", "5", "6", "7", "8","9","10","11"]

        df_symbols = ids + ['date']
        pnl_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        pnl_data_df.index = pd.to_datetime(pnl_data_df.index, format='%m/%d/%Y', utc=True)
        pnl_data_df = pnl_data_df.loc['2012-01-02':'2020-01-01']
        pnl_data_df["ts"] = list(map(lambda x: x.timestamp(), pnl_data_df.index))
        pnl_data_df_array = pnl_data_df.values
        ##

        ##
        name = "position_lv1"
        position_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        position_data_df.index = pd.to_datetime(position_data_df.index, format='%m/%d/%Y', utc=True)
        position_data_df = position_data_df.loc['2012-01-02':'2020-01-01']
        position_data_df_array = position_data_df.values
        ##

        ##
        name = "metrics_252"
        metrics_252_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        metrics_252_data_df.index = pd.to_datetime(metrics_252_data_df.index, format='%m/%d/%Y', utc=True)
        metrics_252_data_df["ts"] = list(map(lambda x: x.timestamp(), metrics_252_data_df.index))
        metrics_252_data_df_array = metrics_252_data_df.values
        ##
        name = "metrics_61"
        metrics_61_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        metrics_61_data_df.index = pd.to_datetime(metrics_61_data_df.index, format='%m/%d/%Y', utc=True)
        metrics_61_data_df["ts"] = list(map(lambda x: x.timestamp(), metrics_61_data_df.index))
        metrics_61_data_df_array = metrics_61_data_df.values

        for i in range(len(data_df_array) - 1):
            current_data = np.array(data_df_array[i][:len(temp_symbols)])
            current_data = np.array(list(map(lambda x: np.array([0, 0, 0, x]), current_data)))
            current_data = dict(zip(temp_symbols, current_data))

            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]), current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))

            date = datetime.utcfromtimestamp(data_df_array[i][-1]).date()
            time_array = np.array(
                [data_df_array[i][-1], data_df_array[i][-1], date.weekday(), date.year, date.month, date.day, 0, 0,
                 0]).astype(np.int64)
            ts = np.array(time_array)

            temp_secondary_data = {}
            temp_secondary_data["ID_symbol_map"] = {}
            #temp_secondary_data["KD_lv1" + '_' + "ID_symbol_map"]
            temp_secondary_data["ID_symbol_map"]["KD_lv1"] = np.array([['GBPUSD'],
                                                                        ['NOKSEK'],
                                                                        ['NZDJPY'],
                                                                        ['GBPUSD'],
                                                                        ['NOKSEK'],
                                                                        ['NZDJPY'],
                                                                        ['GBPUSD'],
                                                                        ['NOKSEK'],
                                                                        ['NZDJPY'],
                                                                        ['GBPUSD'],
                                                                        ['NOKSEK'],
                                                                        ['NZDJPY']
                                                                        ], dtype=object)

            temp_secondary_data["strategy_id"] = 0

            temp_secondary_data["KD_lv1" + '_' + "pnl"] = {}
            temp_secondary_data["KD_lv1" + '_' + "pnl"]["data"] = np.array(
                pnl_data_df_array[i][:len(temp_symbols) * 4])[None, :][:, :, None]

            temp_secondary_data["KD_lv1" + '_' + "pnl"]["time"] = ts.reshape(-1, 9)

            temp_secondary_data["KD_lv1" + '_' + "position"] = {}
            temp_secondary_data["KD_lv1" + '_' + "position"]["data"] = np.array(
                position_data_df_array[i][:len(temp_symbols) * 4])[None, :][:, :, None]
            temp_secondary_data["KD_lv1" + '_' + "position"]["time"] = ts.reshape(-1, 9)

            k = -1
            while metrics_61_data_df_array[k + 1][-1] <= pnl_data_df_array[i][-1]:
                k += 1

            if k != -1:
                date1 = datetime.utcfromtimestamp(metrics_61_data_df_array[k][-1]).date()
                time_array1 = np.array(
                    [metrics_61_data_df_array[k][-1], metrics_61_data_df_array[k][-1], date1.weekday(), date1.year,
                     date1.month, date1.day, 0, 0,
                     0]).astype(np.int64)
                ts1 = np.array(list(map(lambda x: np.array([x]), time_array1)))
                temp_secondary_data["KD_lv1" + '_' + "metrics_61"] = {}
                temp_secondary_data["KD_lv1" + '_' + "metrics_61"]["data"] = np.array(
                    metrics_61_data_df_array[k][:len(temp_symbols) * 4])[None, :][:, :, None]

                temp_secondary_data["KD_lv1" + '_' + "metrics_61"]["time"] = ts1.reshape(-1, 9)
            else:
                temp_secondary_data["KD_lv1" + '_' + "metrics_61"] = {}

            k = -1
            while metrics_252_data_df_array[k + 1][-1] <= pnl_data_df_array[i][-1]:
                k += 1

            if k != -1:
                date2 = datetime.utcfromtimestamp(metrics_252_data_df_array[k][-1]).date()
                time_array2 = np.array(
                    [metrics_252_data_df_array[k][-1], metrics_252_data_df_array[k][-1], date2.weekday(), date2.year,
                     date2.month, date2.day, 0, 0,
                     0]).astype(np.int64)
                ts2 = np.array(list(map(lambda x: np.array([x]), time_array2)))
                temp_secondary_data["KD_lv1" + '_' + "metrics_252"] = {}
                temp_secondary_data["KD_lv1" + '_' + "metrics_252"]["data"] = np.array(
                    metrics_252_data_df_array[k][:len(temp_symbols) * 4])[None, :][:, :, None]

                temp_secondary_data["KD_lv1" + '_' + "metrics_252"]["time"] = ts2.reshape(-1, 9)
            else:
                temp_secondary_data["KD_lv1" + '_' + "metrics_252"] = {}

            #
            id_map = {0: 0, 1: 0, 2: 0, 3: 1, 4: 1, 5: 1, 6: 2, 7: 2, 8: 2, 9: 3, 10: 3, 11: 3}
            temp_secondary_data["id_map"] = {}
            temp_secondary_data["id_map"]['KD_lv1'] = id_map

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("secondary_data", temp_secondary_data, ts))
            get_logger.get_logger().info(("secondary_data", temp_secondary_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts[3:6]))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts[3:6]))

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/test_KD/results", f"KD_lv2.hdf5")

        result_reader = ResultReader(path_name)

        result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/test_KD/results', id_list=[0])

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        total = 0
        for id, pv in pnl.items():
            total += np.sum(pnl[id][0][-7:-6].values)
        get_logger.get_logger().info(f"total:{total}")
        assert np.isclose(total, 79922.26)

    def test_lv3(self):

        ####KD2,rsi2

        quant_engine = QuantEngine()

        quant_engine.load_config(json.load(open('tests/engine/production/production_engine_test_case/test_combination_strategy/config/combination.json')),
            pd.read_csv("tests/engine/production/production_engine_test_case/test_combination_strategy/strategy_pool/combination_strategy_pool.csv"))
        quant_engine.load_app(OrderRouter(commission_pool_path='tests/engine/production/production_engine_test_case/commission_pool/aqm_turnover_union_fx_weight_strategy.csv'))

        path = "tests/engine/production/production_engine_test_case/test_combination_strategy/data"

        #
        ##
        name = "current"
        symbols1 = ['AUDCAD','AUDJPY','AUDNZD','GBPUSD','NOKSEK','NZDJPY']
        temp_symbols = list(map(lambda x: f"{x}", symbols1))
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']

        data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        data_df.index = pd.to_datetime(data_df.index, format='%m/%d/%Y', utc=True)
        data_df = data_df.loc['2012-01-01':'2020-01-01']
        data_df["ts"] = list(map(lambda x: x.timestamp(), data_df.index))
        data_df_array = data_df.values

        ##
        ccys = ['AUDCAD','AUDJPY','AUDNZD','GBPUSD','NOKSEK','NZDJPY']

        df_symbols = ccys + ['date']
        name = "fx"
        fx_data_df = pd.read_csv(os.path.join(path, name + ".csv"), usecols=df_symbols, index_col='date')
        fx_data_df.index = pd.to_datetime(fx_data_df.index, format='%m/%d/%Y', utc=True)
        fx_data_df = fx_data_df.loc['2012-01-01':'2020-01-01']
        fx_data_df_array = fx_data_df.values

        ##
        name = "rate"
        df_symbols = list(map(lambda x: f"{x}", symbols1)) + ['date']
        rate_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        rate_data_df.index = pd.to_datetime(rate_data_df.index, format='%m/%d/%Y', utc=True)
        rate_data_df = rate_data_df.loc['2012-01-01':'2020-01-01']
        rate_data_df_array = rate_data_df.values
        ##

        ##

        ##
        name = "position_KD"
        position_KD_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        position_KD_data_df.index = pd.to_datetime(position_KD_data_df.index, format='%m/%d/%Y', utc=True)
        position_KD_data_df = position_KD_data_df.loc['2012-01-01':'2020-01-01']
        position_KD_data_df_array = position_KD_data_df.values
        ##

        name = "position_RSI"
        position_RSI_data_df = pd.read_csv(os.path.join(path, name + ".csv"), index_col='date')
        position_RSI_data_df.index = pd.to_datetime(position_RSI_data_df.index, format='%m/%d/%Y', utc=True)
        position_RSI_data_df = position_RSI_data_df.loc['2012-01-01':'2020-01-01']
        position_RSI_data_df_array = position_RSI_data_df.values


        for i in range(len(data_df_array) - 1):
            current_data = np.array(data_df_array[i][:len(temp_symbols)])
            current_data = np.array(list(map(lambda x: np.array([0, 0, 0, x]), current_data)))
            current_data = dict(zip(temp_symbols, current_data))

            current_fx_data = np.array(fx_data_df_array[i][:len(ccys)])
            current_fx_data = np.array(list(map(lambda x: np.array([x]), current_fx_data)))
            current_fx_data = dict(zip(ccys, current_fx_data))
            #
            current_rate_data = np.array(rate_data_df_array[i][:len(temp_symbols)])
            current_rate_data = dict(zip(temp_symbols, current_rate_data))

            date = datetime.utcfromtimestamp(data_df_array[i][-1]).date()
            time_array = np.array(
                [data_df_array[i][-1], data_df_array[i][-1], date.weekday(), date.year, date.month, date.day, 0, 0,
                 0]).astype(np.int64)
            ts = np.array(time_array)

            temp_secondary_data = {}
            temp_secondary_data["ID_symbol_map"] = {}
            temp_secondary_data["ID_symbol_map"]["KD"] = np.array([['GBPUSD','NOKSEK','NZDJPY'],
                                                                               ], dtype=object)
            temp_secondary_data["ID_symbol_map"]["RSI"] = np.array([['AUDCAD','AUDJPY','AUDNZD'],
                                                                         ], dtype=object)


            temp_secondary_data["strategy_id"] = 0

            temp_secondary_data["KD" + '_' + "position"] = {}
            temp_secondary_data["KD" + '_' + "position"]["data"] = np.array(
                position_KD_data_df_array[i][:3])[None, :][None,:, :]
            temp_secondary_data["KD" + '_' + "position"]["time"] = ts.reshape(-1, 9)

            temp_secondary_data["RSI" + '_' + "position"] = {}
            temp_secondary_data["RSI" + '_' + "position"]["data"] = np.array(
                position_RSI_data_df_array[i][:3])[None, :][None,:, :]
            temp_secondary_data["RSI" + '_' + "position"]["time"] = ts.reshape(-1, 9)

            #
            id_map = {0: 0}
            temp_secondary_data["id_map"] = {}
            temp_secondary_data["id_map"]['KD'] = id_map
            temp_secondary_data["id_map"]['RSI'] = id_map

            #traable table pack all True/1
            current_tradable_data_array = np.ones(len(temp_symbols))
            current_tradable_data = dict(zip(temp_symbols, current_tradable_data_array))            
            quant_engine.streaming_data_queue.put(("current_tradable_data", current_tradable_data, ts))
            get_logger.get_logger().info(("current_tradable_data", current_tradable_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("secondary_data", temp_secondary_data, ts))
            get_logger.get_logger().info(("secondary_data", temp_secondary_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_fx_data", current_fx_data, ts))
            get_logger.get_logger().info(("current_fx_data", current_fx_data, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_data", current_data, ts))
            get_logger.get_logger().info(("current_data", current_data, ts[3:6]))
            quant_engine.streaming_data_queue.put(("fire_order", None, ts))
            get_logger.get_logger().info(("fire_order", None, ts[3:6]))

            quant_engine.streaming_data_queue.put(("current_rate_data", current_rate_data, ts))
            get_logger.get_logger().info(("current_rate_data", current_rate_data, ts[3:6]))

        quant_engine.start_production_engine()
        quant_engine.start_production_engine_other()

        quant_engine.end_production_engine()
        quant_engine.wait_production_engine()

        quant_engine.end_production_engine_other()
        quant_engine.wait_production_engine_other()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/test_combination_strategy/results", f"lv3.hdf5")

        result_reader = ResultReader(path_name)

        result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/test_combination_strategy/results', id_list=[0])

        pnl = result_reader.get_strategy(id_list=[0], fields=["pnl"])
        get_logger.get_logger().info(f"pnl:{pnl}")
        assert np.isclose(pnl[0][0][-2:-1].values, 44217.015)

    def test_engine_manager_lv3(self):

        engine_manager = EngineManager()

        engine_manager.add_engine(json.load(open(r'tests/engine/production/production_engine_test_case/test_engine_manager_lv3/config/RSI_lv1.json')),pd.read_csv(r"tests/engine/production/production_engine_test_case/test_engine_manager_lv3/strategy_pool/RSI_lv1_strategy_pool.csv"))
        engine_manager.add_engine(json.load(open(r'tests/engine/production/production_engine_test_case/test_engine_manager_lv3/config/RSI_lv2.json')),pd.read_csv(r"tests/engine/production/production_engine_test_case/test_engine_manager_lv3/strategy_pool/RSI_lv2_strategy_pool.csv"))
        engine_manager.add_engine(json.load(open(r'tests/engine/production/production_engine_test_case/test_engine_manager_lv3/config/KD_lv1.json')),pd.read_csv(r"tests/engine/production/production_engine_test_case/test_engine_manager_lv3/strategy_pool/KD_lv1_strategy_pool.csv"))
        engine_manager.add_engine(json.load(open(r'tests/engine/production/production_engine_test_case/test_engine_manager_lv3/config/KD_lv2.json')),pd.read_csv(r"tests/engine/production/production_engine_test_case/test_engine_manager_lv3/strategy_pool/KD_lv2_strategy_pool.csv"))
        engine_manager.add_engine(json.load(open(r'tests/engine/production/production_engine_test_case/test_engine_manager_lv3/config/combination.json')),
                                  pd.read_csv(
                                      r"tests/engine/production/production_engine_test_case/test_engine_manager_lv3/strategy_pool/combination_strategy_pool.csv"))

        load_data_event = engine_manager.load_engine_data()
        engine_manager.start_engine()
        engine_manager.run()
        load_data_event.wait()

        timepoint = "20121231"
        engine_manager.handle_current_fx_data(timepoint)
        handle_current_data_event,_ = engine_manager.handle_current_data(timepoint)
        handle_current_data_event.wait()
        engine_manager.handle_current_rate_data(timepoint)

        timepoint = "20130101"
        engine_manager.handle_current_fx_data(timepoint)
        handle_current_data_event,_ = engine_manager.handle_current_data(timepoint)
        handle_current_data_event.wait()
        engine_manager.handle_current_rate_data(timepoint)


        get_logger.get_logger().info("finish handle data")

        engine_manager.kill_engine()
        engine_manager.wait_engine()
        engine_manager.kill()
        engine_manager.wait()

        path_name = os.path.join("tests/engine/production/production_engine_test_case/test_engine_manager_lv3/results", f"lv3.hdf5")
        result_reader = ResultReader(path_name)
        # result_reader.to_csv(export_folder='tests/engine/production/production_engine_test_case/test_RSI/results_level2' ,id_list=[0])
        position=result_reader.get_strategy(id_list=[0],fields=["position"])

        get_logger.get_logger().info(f"position:{np.sum(position[0][-1][-1:].values)}")
        assert np.isclose(np.sum(position[0][-1][-1:].values), 432890.7721)