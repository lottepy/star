import json
import math
import os
import pickle
import re
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce
from multiprocessing import Lock, Process

import pandas as pd

from .backtest_engine_base import BacktestEngineBase
from .core.backtester import BackTester
from .utils.constants import InstrumentType, TimeFreq,SAVE_DATA_TIME,WINDOW_TIME,REF_WINDOW_TIME,StockDataType
from .utils.data_loader import DataLoader, fx_init
from .utils.helper import (array_to_raw_array, find_csv_filenames,
                           get_ccy2ticker_matrix, get_symbol2ccy_matrix,
                           get_symbol_type, raw_array_to_array)

class MagnumBacktestEngine(BacktestEngineBase):

    def __init__(self):
        super().__init__()
        self.local_data_dir = ''
        self.local_data_symbols = []
        self.local_rate_data_dir = ''
        self.local_rate_data_symbols = []

        self.local_data_symbol_info = pd.DataFrame()
        self.local_ref_data_dir = ''
        self.local_ref_data_symbols = []

    def find_local_data(self, path_to_dir,path_to_rate_dir, path_to_info):
        """ read self declare csv as trading data"""
        local_data_symbols = []
        if path_to_dir is not None and os.path.isdir(path_to_dir):
            self.local_data_dir = path_to_dir
            local_data_symbols = find_csv_filenames(path_to_dir)
        if path_to_rate_dir is not None and os.path.isdir(path_to_rate_dir):
            self.local_rate_data_dir = path_to_rate_dir
            #self.local_rate_data_symbols = find_csv_filenames(path_to_rate_dir)
        if  path_to_info is not None and  os.path.isfile(path_to_info):
            self.local_data_symbol_info = pd.read_csv(path_to_info, index_col="symbol")
            self.local_data_symbol_info["source"] = ["self" if x in local_data_symbols else "data_master" for x in self.local_data_symbol_info.index]
        

    def find_local_ref_data(self, path_to_dir):
        """ read self declare csv as ref data"""
        if os.path.isdir(path_to_dir):
            self.local_ref_data_dir = path_to_dir
            self.local_ref_data_symbols = find_csv_filenames(path_to_dir)

    def load_parameters(self, item):
        if isinstance(item, str):
            json_item = json.load(open(item))
        else:
            json_item = item
        self.load_parameters_by_json(json_item)


    
    
    def load_parameters_by_json(self, json_item:dict):
        """ parameter主要与strategy运行的参数有关；path可以是相对或绝对路径，指向一个.json文件 """
        fx_init()
        self.params = json_item.copy()
        self.logger.debug(self.params)

        self.params['data'] = self.params.get("data",{})
        self.params['time'] = self.params.get("time",{})

        self.start = datetime(self.params['start_year'], self.params['start_month'], self.params['start_day'])
        self.end = datetime(self.params['end_year'], self.params['end_month'], self.params['end_day'])


        self.params['algo']['save_data_time'] = self.params['algo'].get('save_data_time', SAVE_DATA_TIME)
        self.params['algo']['window_time'] = self.params['algo'].get('window_time', WINDOW_TIME)
        data_buffer = max(self.params['algo']['save_data_time'], self.params['algo']['window_time'])
        self.download_start = self.start - timedelta(seconds = 4 * data_buffer * TimeFreq[self.params['algo']['freq']].value)
        self.download_end = self.end
        self.params['algo']['ref_window_time'] = self.params['algo'].get('ref_window_time', REF_WINDOW_TIME)
        ref_data_buffer = self.params['algo']['ref_window_time']
        self.ref_download_start = self.start - timedelta(seconds=4 * ref_data_buffer * TimeFreq[self.params['algo']['freq']].value)
        self.ref_download_end = self.download_end
        self.base_ccy = self.params['algo'].get("base_ccy", "NA")
        strategy_pool_df = pd.read_csv(self.params['algo']['strategy_pool_path'])
        """ non DAILY data have only BGNL source in dm """
        if self.params['algo']['freq'] != 'DAILY':
            fx_data_source = "BGNL"
            fx_backup_data_source = "BGNL"
        self.is_strategy_allocation = self.params['algo'].get('super_stratetgy_module') is not None and self.params['algo'].get('super_stratetgy') is not None
        self.is_trade_at_open = self.params['algo'].get('is_trade_at_open',False)
        self.is_save_data = self.params['algo'].get('is_save_data',False)

        self.download_ref_data_types = list(set(self.params['data'].get("download_types", [])) - {self.params['algo']['freq']})
        self.download_ref_factor_ticker = self.params['data'].get("download_ref_factor_ticker", [])
        self.params['data']['stock_data_type'] = self.params['data'].get('stock_data_type', StockDataType.UNADJUSTED.value)
        self.stock_data_type = self.params['data']['stock_data_type']
        fx_data_source = self.params['data'].get('fx_data_source','BGNL')
        fx_backup_data_source = self.params['data'].get('fx_backup_data_source', None)
        if fx_backup_data_source == fx_data_source:
            fx_backup_data_source = None


        self.params['time']['order_time'] = self.params['time'].get('order_time', 0)
        self.params['time']['rate_time'] = self.params['time'].get('rate_time', 0)


        if "commission_path" in self.params['account'] and os.path.isfile(self.params['account']['commission_path']):
            self.commission = pd.read_csv(self.params['account']['commission_path'], index_col=0)['COMMISSION'].to_dict()
        else:
            self.commission = {}
        self.params['account']['commission'] = self.params['account'].get('commission',0 )




        """ read strategy pool csv and check instrument field like instrument type, lot size in dm or self declare csv """
        self.strategy_pool = {}
        instrument_type_to_symbols_dict = defaultdict(set)  # {'FX': {..}, 'HK_STOCK': {..}}
        for index in strategy_pool_df.index:
            temp_strategy_row = dict(strategy_pool_df.loc[index])
            temp_strategy_row["params"] = eval(temp_strategy_row["params"])
            temp_symbol_dict = eval(temp_strategy_row["symbol"])
            for sym_type, sym_list in temp_symbol_dict.items():
                instrument_type_to_symbols_dict[sym_type] = instrument_type_to_symbols_dict[sym_type].union(set(sym_list))
            temp_strategy_row["symbol"] = list(reduce(lambda x, y: x + y, temp_symbol_dict.values()))
            self.strategy_pool[index] = temp_strategy_row

        local_data_symbols = find_csv_filenames(self.local_data_dir )
        symbol_feature_df = get_symbol_type(instrument_type_to_symbols_dict, fx_data_source, fx_backup_data_source,
                                            self.local_data_symbol_info,local_data_symbols)
        self.lot_size = dict(symbol_feature_df["lot_size"])
        self.symbol2bbg_dict = dict(symbol_feature_df["bbg_code"])
        self.symbol2back_up_bbg_dict = dict(symbol_feature_df["back_up_bbg_code"])
        self.symbol2instrument_type_dict = dict(
            map(lambda x: (x[0], InstrumentType[x[1]].value), dict(symbol_feature_df["instrument_type"]).items()))
        self.symbol2source_dict = dict(symbol_feature_df["source"])
        self.symbol2exchange_id_dict = dict(symbol_feature_df["exchange_id"])
        """ get symbol ccy matrix for pnl calcualtion and fx convert """
        self.symbol_list = sorted(symbol_feature_df.index, key=lambda x: (self.symbol2instrument_type_dict.get(x, 1), x))
        self.ccy_matrix = get_symbol2ccy_matrix(self.symbol_list, symbol_feature_df["base_ccy"].to_dict())
        self.ccy_list = list(self.ccy_matrix.index)

        fx_backup_data_source = self.params['data'].get('fx_backup_data_source', fx_data_source)
        ccy_feature_df = get_ccy2ticker_matrix(self.ccy_list, fx_data_source, fx_backup_data_source, self.params['algo']['freq'])
        self.ccy2bbg_dict = dict(ccy_feature_df["bbg_code"])
        self.ccy2back_up_bbg_dict = dict(ccy_feature_df["back_up_bbg_code"])
        self.ccy2trading_ccy_dict = dict(ccy_feature_df["trading_ccy"])

        # assert self.download_ref_data_types + [self.params['algo']['freq']], f'download_ref_data_types cannot be empty'
        assert set(self.download_ref_data_types) <= set(
            BacktestEngineBase.DATA_TYPES), f'data types {self.download_ref_data_types} error: only {BacktestEngineBase.DATA_TYPES} are allowed'
        assert len(list(filter(lambda x: set(x["symbol"]) != {"FX"}, self.strategy_pool.values()))) != 0 \
               or self.params['algo']['freq'] == "DAILY", f"only fx symbol have hourly or minly data"
        assert self.base_ccy in ["NA", "USD", "HKD", "CNY", "CNH"], f"base_ccy only support None NA,USD,HKD,CNY,CNH"
        assert self.params['algo']['freq'] == "DAILY" or self.base_ccy in ["NA",
                                                                           "USD"], f"non daily data only support USD as base ccy"
        #assert set(self.local_data_symbol_info.index).issuperset(
        #    set(self.symbol_list).intersection(self.local_data_symbols)), "some self declare ticker are not exist in data_info"

        assert self.stock_data_type in [t.value for t in StockDataType], f'stock data types {self.stock_data_type} error: only {[t.value for t in StockDataType]} are allowed'
        
        self.hdf5_path = os.path.join(self.params['result_output']['save_dir'], self.params['result_output'].get("name","result") + ".hdf5" )
        """ assert not os.path.exists(self.hdf5_path) or self.params['result_output'].get('override', False), f'{self.hdf5_path} already exists! you must delete old result file before running backtest'
        if os.path.exists(self.hdf5_path):
            os.remove(self.hdf5_path) """

    def load_data(self):
        assert not os.path.exists(self.hdf5_path) or self.params['result_output'].get('override', False), f'{self.hdf5_path} already exists! you must delete old result file before running backtest'
        if os.path.exists(self.hdf5_path):
            os.remove(self.hdf5_path)
        self.data_loader = DataLoader(self.symbol_list, self.ccy_list, self.symbol2bbg_dict, self.symbol2back_up_bbg_dict,
                                        self.symbol2instrument_type_dict, self.ccy2bbg_dict, self.ccy2back_up_bbg_dict,
                                        self.ccy2trading_ccy_dict, self.symbol2source_dict, self.download_start,
                                        self.download_end, self.params, self.base_ccy, self.local_data_dir,self.local_rate_data_dir,
                                        self.local_ref_data_dir, self.local_ref_data_symbols,self.stock_data_type,self.symbol2exchange_id_dict)

        self.exchange2calender = self.data_loader.get_exchange_exchange_calendar()
        if self.params.get('download', True):
            # TODO 做成thread  是否一次全部load完不需要pipeline？
            if "DAILY" == self.params['algo']['freq']:
                self.data_loader.start_load_daily()
                self.data_loader.start_load_fx_ccy_daily()
            elif "HOURLY" == self.params['algo']['freq']:
                self.data_loader.start_load_hour()
                self.data_loader.start_load_fx_ccy_hour()
            elif "MINLY" == self.params['algo']['freq']:
                self.data_loader.start_load_minute()
                self.data_loader.start_load_fx_ccy_minute()

            self.data_loader.start_load_dividends_daily()

            # download reference data
            self.data_loader.modify_date(self.ref_download_start, self.ref_download_end)
            if "DAILY" in self.download_ref_data_types:
                self.data_loader.start_load_daily()
            if "HOURLY" in self.download_ref_data_types:
                self.data_loader.start_load_hour()
            if "MINLY" in self.download_ref_data_types:
                self.data_loader.start_load_minute()

    def run(self):
        while True:
            if self.params.get('download', True):
                data_dict = self.get_data()
                if self.params.get('save_download_data'):
                    self.save_data(data_dict)
                    data_dict = self.get_data_from_pickle()
            else:
                data_dict = self.get_data_from_pickle()

            #################################################

            config_dict = {}
            config_dict['observer_save_path'] = self.params['result_output']['save_dir']
            config_dict['observer_file_name'] = "global"
            config_dict['is_export_csv'] = True

            config_dict['save_dir'] = self.params['result_output']['save_dir']
            config_dict["ccy_list"] = self.ccy_list
            config_dict["symbol_batch"] = self.symbol_list
            config_dict["commission"] = self.commission
            config_dict["lot_size"] = self.lot_size
            config_dict["instrument_type"] = self.symbol2instrument_type_dict
            config_dict["is_strategy_allocation"] = self.is_strategy_allocation
            config_dict["hdf5_path"] = self.hdf5_path
            config_dict["is_trade_at_open"] = self.is_trade_at_open
            config_dict["is_save_data"] = self.is_save_data
            config_dict["symbol2exchange_id"] = self.symbol2exchange_id_dict
            config_dict["exchange2calender"] = self.exchange2calender
            # config_dict["strategy_pool"] =  self.strategy_pool
            #################################################
            # 开始回测
            worker_processes = self.params.get("worker_processes", 1) if not self.is_strategy_allocation else 1
            
            t_bt_s = datetime.now()
            hdf5_lock = Lock()
            self.logger.info(f'开始{config_dict["symbol_batch"][0]}~{config_dict["symbol_batch"][-1]}的回测..')

            batch_size = self.params.get('maximum_strategy_quantity_per_process', int(len(self.strategy_pool)/worker_processes+1))
            batch_count = 0
            # 限制每个子进程跑的strategy数量，以防占用的内存过多
            # 假设子进程共2个，batch_size=10，则子进程1运行strategy 0~9，子进程2运行10~19；下一轮，子进程1运行strategy 20~29，以此类推
            while batch_size * batch_count < len(self.strategy_pool):
                processes = []
                for _ in range(worker_processes):
                    config_dict["strategy_pool"] = {strategy_id: strategy for strategy_id, strategy in self.strategy_pool.items()
                                                if batch_size * batch_count <= strategy_id < batch_size * (batch_count+1)}
                    if config_dict['strategy_pool']:  # 在最后的batch 可能有strategy_pool为空的情况 应跳过
                        #processes.append(Process(target=start_backtest, args=(data_dict, config_dict.copy(), self.params, hdf5_lock)))
                        start_backtest(data_dict, config_dict.copy(), self.params, hdf5_lock)
                        batch_count += 1
                for p in processes:
                    p.start()
                for p in processes:
                    p.join()

            t_bt_e = datetime.now()
            self.logger.info(f"本轮回测用时{t_bt_e - t_bt_s}")
            break

    def get_data(self):
        data_dict = {}
        queue_item = None
        if "MINLY" == self.params['algo']['freq'] and not self.data_loader.minute_queue.empty():
            queue_item = self.data_loader.minute_queue.get()
        elif "DAILY" == self.params['algo']['freq'] and not self.data_loader.daily_queue.empty():
            queue_item = self.data_loader.daily_queue.get()
        elif "HOURLY" == self.params['algo']['freq'] and not self.data_loader.hour_queue.empty():
            queue_item = self.data_loader.hour_queue.get()
        if queue_item:
            date, symbol_batch, DataArray, DatatimeArray = queue_item
            data_dict['data_array_raw'] = array_to_raw_array(DataArray)
            data_dict['data_array_shape'] = DataArray.shape
            del DataArray
            data_dict['time_array_raw'] = array_to_raw_array(DatatimeArray)
            data_dict['time_array_shape'] = DatatimeArray.shape
            del DatatimeArray

        if not self.data_loader.dividends_queue.empty():
            RateArray, RatetimeArray = self.data_loader.dividends_queue.get()
            data_dict['rate_array_raw'] = array_to_raw_array(RateArray)
            data_dict['rate_array_shape'] = RateArray.shape
            del RateArray
            data_dict['rate_time_array_raw'] = array_to_raw_array(RatetimeArray)
            data_dict['rate_time_array_shape'] = RatetimeArray.shape
            del RatetimeArray

        data_dict["ref_factor"] = {}
        data_dict["ref_data"] = {}

        for data_type in self.download_ref_data_types:
            queue_item = None
            if "MINLY" == data_type and not self.data_loader.minute_queue.empty():
                queue_item = self.data_loader.minute_queue.get()
            elif "DAILY" == data_type and not self.data_loader.daily_queue.empty():
                queue_item = self.data_loader.daily_queue.get()
            elif "HOURLY" == data_type and not self.data_loader.hour_queue.empty():
                queue_item = self.data_loader.hour_queue.get()
            if queue_item:
                date, symbol_batch, DataArray, DatatimeArray = queue_item
                data_dict["ref_data"][data_type] = {}
                data_dict_temp = data_dict["ref_data"][data_type]
                data_dict_temp['data_array_raw'] = array_to_raw_array(DataArray)
                data_dict_temp['data_array_shape'] = DataArray.shape
                del DataArray
                data_dict_temp['time_array_raw'] = array_to_raw_array(DatatimeArray)
                data_dict_temp['time_array_shape'] = DatatimeArray.shape
                del DatatimeArray

        for factor in self.download_ref_factor_ticker:
            queue_item = self.data_loader.get_ref_factor(factor)
            if queue_item:
                DataArray, DatatimeArray = queue_item
                data_dict["ref_factor"][factor] = {}
                data_dict_temp = data_dict["ref_factor"][factor]
                data_dict_temp['data_array_raw'] = array_to_raw_array(DataArray)
                data_dict_temp['data_array_shape'] = DataArray.shape
                del DataArray
                data_dict_temp['time_array_raw'] = array_to_raw_array(DatatimeArray)
                data_dict_temp['time_array_shape'] = DatatimeArray.shape
                del DatatimeArray

        if not self.data_loader.ccy_queue.empty():
            queue_item = self.data_loader.ccy_queue.get()
            FxDataArray, DatatimeArray = queue_item
            data_dict['fx_data_array_raw'] = array_to_raw_array(FxDataArray)
            data_dict['fx_data_array_shape'] = FxDataArray.shape
            del FxDataArray
            data_dict['fx_data_time_array_raw'] = array_to_raw_array(DatatimeArray)
            data_dict['fx_data_time_array_shape'] = DatatimeArray.shape
            del DatatimeArray
            data_dict['ccy_matrix_array_raw'] = array_to_raw_array(self.ccy_matrix.values)
            data_dict['ccy_matrix_array_shape'] = self.ccy_matrix.values.shape

        return data_dict

    def get_data_from_pickle(self):
        print('将会从本地的 data_dict_pickle.bin 读取数据，不会下载数据')
        try:
            data_dict = pickle.load(open('data_dict_pickle.bin', 'rb'))
        except BaseException as e:
            print(f'读取 data_dict_pickle.bin 出错：{e}')
            raise e
        else:
            keys = data_dict.keys()
            for array_name in list(keys):  # copy
                if array_name.endswith('_array'):
                    data_dict[array_name + '_raw'] = array_to_raw_array(data_dict[array_name])
                    data_dict[array_name + '_shape'] = data_dict[array_name].shape
                    data_dict.pop(array_name)
        return data_dict

    def save_data(self, data_dict):
        keys = data_dict.keys()
        for key in list(keys):  # copy
            result = re.match('(.*)_raw', key)  # 要求命名必须是xxx_array_raw, xxx_array_shape
            if result:
                array_name = result.group(1)
                data_dict[array_name] = raw_array_to_array(data_dict[array_name + '_raw'], data_dict[array_name + '_shape'])
                data_dict.pop(array_name + '_raw')
                data_dict.pop(array_name + '_shape')
        with open('data_dict_pickle.bin', 'wb') as f:
            pickle.dump(data_dict, f, pickle.HIGHEST_PROTOCOL)
        print('下载的数据已经保存到 data_dict_pickle.bin，修改.json中的"download"配置即可从本地读取，程序将返回')
        # exit(0)


def start_backtest(data_dict, config_dict, params, hdf5_lock):
    backtester = BackTester(data_dict, config_dict, params,hdf5_lock)
    backtester.start_backtest()
    return
