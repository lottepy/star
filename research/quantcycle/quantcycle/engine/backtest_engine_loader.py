import importlib
import multiprocessing
import os
import numba as nb
import numpy as np
import pandas as pd
import pytz
from copy import deepcopy
from datetime import datetime, timedelta
from numba.typed import Dict, List
from numba import types
from quantcycle.utils.backtest_constant import InstrumentType


class ConfigLoader:
    def __init__(self, config, strategy_pool_df=None):
        self.config = config
        self.load = True
        self.dump = True

        # 每个strategy相同的config
        self.start_dt = datetime(self.config['start_year'], self.config['start_month'], self.config['start_day'],
                                 tzinfo=pytz.utc)
        self.end_dt = datetime(self.config['end_year'], self.config['end_month'], self.config['end_day'],
                               23, 59, 59, tzinfo=pytz.utc)
        self.window_size_dict = config['algo']['window_size']
        self.base_ccy = config['algo']['base_ccy']
        self.max_order_feedback = config.get('algo', {}).get('max_order_feedback', 0) # 不填就当0
        self.cash = config['account']['cash']
        self.result_dir = config['result_output']['save_dir']
        self.result_filename = config['result_output']['save_name']
        self.remark_save_dir = config.get('signal_remark', {}).get('save_dir', None)
        self.remark_save_name = config.get('signal_remark', {}).get('save_name', None)
        self.remark_load_dir = config.get('signal_remark', {}).get('load_dir', None)
        self.remark_load_name = config.get('signal_remark', {}).get('load_name', None)
        n_logic_cores = multiprocessing.cpu_count()
        self.save_n_workers = config.get('result_output', {}) \
                                    .get('save_n_workers', n_logic_cores + 1 if n_logic_cores < 8 else 9)
        self.flatten = config['result_output']['flatten']
        self.status_dir = config.get('result_output', {}).get('status_dir', None)    # 不填就不存
        self.status_name = config.get('result_output', {}).get('status_name', None)
        self.is_numba_parallel = config.get('optimization', {}).get('numba_parallel', None)
        self.python_parallel_n_workers = config.get('optimization', {}).get('python_parallel_n_workers', None)
        self.commission_path = config.get('account', {}).get('commission_path', None)
        self.commission_fee = config.get('account', {}).get('commission', None)
        self.data_config = config.get('data', {})
        self.tradable_table_config = config.get('tradable_table', {})
        self.ref_data_config = config.get('ref_data', {})
        self.pickle_config = config.get("dm_pickle", {})
        self.timing = config.get('timing', {})
        self.main_asset_list = self.get_main_assets()
        self.data_config.update(config.get('secondary_data', {}))   # update must be AFTER getting main_asset_list

        self.strategy_pool_df = None
        self.n_strategies = 0
        self.n_params = 0
        self.strategy_params = None
        self.strategy_class = None
        self.load_strategy_pool(strategy_pool_df)


    def get_main_assets(self):
        valid_tradable_labels = {'FX', 'STOCKS', 'FUTURES'}
        main_asset_list = []
        if not self.data_config:
            raise ValueError('Error: 没有数据怎么做回测？')

        for key in self.data_config:
            if key not in valid_tradable_labels:
                raise ValueError(f'Error: "{key}"无效，main data资产须是{valid_tradable_labels}之一。')
            main_asset_list.append(key)
        return main_asset_list


    def validate_config(self):
        if self.status_dir is None or self.status_name is None:
            self.load = False
            self.dump = False
            print("info: 未指定status_dir或status_name，默认不会有状态载入或存储。")

        if self.ref_data_config == {}:
            print("info: 未指定reference data, 不装载reference data。")

        if self.tradable_table_config != {}:
            if 'dir' not in self.tradable_table_config:
                raise KeyError("Please provide tradable table dir with key 'dir'.")
            if not os.path.exists(self.tradable_table_config['dir']):
                raise FileNotFoundError("File {} does not exit.".format(self.tradable_table_config['dir']))

        if self.is_numba_parallel is None:
            self.is_numba_parallel = False
            print("info: 未指定numba_parallel, 默认不会开启numba parallel。")

        n_logic_cores = multiprocessing.cpu_count()
        if self.python_parallel_n_workers is None:
            self.python_parallel_n_workers = 0
        elif self.python_parallel_n_workers == 'best':
            self.python_parallel_n_workers = min(n_logic_cores - 1, self.n_strategies)
        if self.python_parallel_n_workers >= 2:   # enabled
            if self.is_numba_parallel:
                raise ValueError("Error: 不可同时开启numba parallel和python parallel。")
            if self.python_parallel_n_workers > self.n_strategies:
                self.python_parallel_n_workers = self.n_strategies
                print(f"info: 配置的进程数大于策略个数，进程数被重置。")
            print(f"info: python多进程(n={self.python_parallel_n_workers})启用，请确保numba jit已禁用。")

        if self.save_n_workers == 'best':
            self.save_n_workers = n_logic_cores + 1 if n_logic_cores < 10 else 11   # maximum 11

        if self.commission_path is None and self.commission_fee is None:
            print("Warning: 未指定commission路径和commission预设值, 默认commission为0。")

        # Other checks
        calc_rate_time = self.timing.get('calc_rate_time_utc', None)
        if calc_rate_time is not None:
            split_time = calc_rate_time.split(':')
            if len(calc_rate_time) != 5 or len(split_time) != 2 or \
                    not split_time[0].isnumeric() or not split_time[1].isnumeric():
                raise ValueError("Error: calc_rate_time_utc格式错误，必须是HH:MM。")
        else:
            print("Warning: 未设置calc_rate_time_utc，intraday策略默认22:00UTC计息。")


    def load_strategy_pool(self, strategy_pool_df=None):
        # 每个strategy不相同的config
        if strategy_pool_df is None:
            strategy_pool_path = self.config['algo']['strategy_pool_path']
            self.strategy_pool_df = pd.read_csv(strategy_pool_path)
        else:
            self.strategy_pool_df = strategy_pool_df
        self.n_strategies = self.strategy_pool_df.shape[0]
        for i in range(self.strategy_pool_df.shape[0]):
            self.strategy_pool_df.iloc[i]['symbol'] = eval(self.strategy_pool_df.iloc[i]['symbol'])
            self.strategy_pool_df.iloc[i]['params'] = eval(self.strategy_pool_df.iloc[i]['params'])
        self.n_params = len(self.strategy_pool_df.iloc[0]['params'])        #! num of params are same for all?
        self.strategy_params = np.zeros((self.n_strategies, self.n_params))
        for i in range(self.strategy_pool_df.shape[0]):
            self.strategy_params[i] = list(self.strategy_pool_df.iloc[i]['params'].values())
        strategy_module = importlib.import_module(self.strategy_pool_df.iloc[0]['strategy_module'])
        self.strategy_class = getattr(strategy_module, self.strategy_pool_df.iloc[0]['strategy_name'])


class DataLoader:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.strategy_pool_df = config_loader.strategy_pool_df
        self.main_asset_list = config_loader.main_asset_list
        self.main_data_label = self.main_asset_list[0] if len(self.main_asset_list) == 1 else 'MIX'
        self.main_data_key = f'{self.main_data_label}_MAIN'    # available: 'FX_MAIN', 'STOCKS_MAIN', 'FUTURES_MAIN', 'MIX_MAIN'
        self.n_strategies = config_loader.n_strategies
        self.concat_symbols = None

        self.type_field_dict = {'FX':      ['MAIN', 'CCPFXRATE', 'INT'],
                                'STOCKS':  ['MAIN', 'CCPFXRATE', 'INT'],
                                'FUTURES': ['MAIN', 'CCPFXRATE', 'INT'],
                                'MIX':     ['MAIN', 'CCPFXRATE', 'INT'],
                                'MONITOR': ['pnl', 'position', 'metrics_61', 'metrics_252']}

    def load_group_symbol_map(self):
        # 整理strategy_pool内的symbol
        # 现在 group有两类，一种是 FX、Futures、Stocks，用于清算
        #                 一种是 category，不用于清算
        group_symbol_map = {}
        for i in range(self.strategy_pool_df.shape[0]):
            strategy_symbols = deepcopy(self.strategy_pool_df.iloc[i]['symbol'])
            for group, symbols in strategy_symbols.items():
                if group in group_symbol_map.keys():
                    group_symbol_map[group] += symbols
                else:
                    group_symbol_map[group] = symbols

        # 对每个group做处理：1.删除重复symbol 2. strategy id: 将float转为int 3.symbol排序
        for group, symbols in group_symbol_map.items():
            group_symbol_map[group] = list(set(group_symbol_map[group]))
            if isinstance(group_symbol_map[group][0], float):
                group_symbol_map[group] = [int(symbol) for symbol in group_symbol_map[group]]
            group_symbol_map[group].sort()
        return group_symbol_map


    def rebuild_maps(self, group_type_map, group_symbol_map):
        if len(self.main_asset_list) == 1:
            return

        for asset in self.main_asset_list:
            group_symbol_map.pop(asset)
            group_type_map.pop(asset)
        group_symbol_map['MIX'] = self.concat_symbols
        group_type_map['MIX'] = 'order'


    def init_data_time_dict(self, group_type_map, data):
        data_dict, time_dict = {}, {}

        # 装载数据模块数据
        for group, type in group_type_map.items():
            if type == 'order':
                for field in self.type_field_dict[group]:
                    data_dict[f'{group}_{field}'] = data[f'{group}-{group}/{field}']['data_arr']
                    time_dict[f'{group}_{field}'] = data[f'{group}-{group}/{field}']['time_arr'].astype(np.int64)
            else:   # type == 'monitor'
                for field in self.type_field_dict['MONITOR']:
                    if f'{group}-Strat/{field}' in data:
                        data_dict[f'{group}_{field}'] = data[f'{group}-Strat/{field}']['data_arr']
                        time_dict[f'{group}_{field}'] = data[f'{group}-Strat/{field}']['time_arr'].astype(np.int64)

        # ------------------------------------
        # STOCKS CCPFXRATE patch
        #    - Patch data_dict['STOCKS_CCPFXRATE']
        #    - Align CCPFXRATE with stock symbols
        #    - No longer needed due to data manager application
        # ------------------------------------
        # if 'STOCKS-STOCKS/CCPFXRATE' in data:
        #     ccy2idx = {fx.replace(self.base_ccy, ''): idx
        #                for idx, fx in enumerate(data['STOCKS-STOCKS/CCPFXRATE']['symbol_arr'])}
        #     idx_array = [ccy2idx[data['STOCKS-STOCKS/INFO'][symbol]['trading_currency']]
        #                  for symbol in data['STOCKS-STOCKS/MAIN']['symbol_arr']]
        #     ccyfx_shape = data_dict['STOCKS_CCPFXRATE'].shape
        #     data_dict['STOCKS_CCPFXRATE'] = np.resize(data_dict['STOCKS_CCPFXRATE'],
        #                                           (ccyfx_shape[0], len(idx_array), ccyfx_shape[2]))
        #     data_dict['STOCKS_CCPFXRATE'] = data_dict['STOCKS_CCPFXRATE'].astype(np.float)
        #     for row_idx, row in enumerate(data_dict['STOCKS_CCPFXRATE']):
        #         fx_array = np.array([data['STOCKS-STOCKS/CCPFXRATE']['data_arr'][row_idx, i, :]
        #                              for i in idx_array], dtype=np.float)
        #         row[...] = fx_array

        main_data = self.main_data_key
        return data_dict, time_dict


    def get_main_data_fields(self, data):
        if List([1, 2, 3]):     #! numba black magic :(
            return List(data[f'{self.main_data_label}-{self.main_data_label}/MAIN']['fields_arr'])
        else:
            return data[f'{self.main_data_label}-{self.main_data_label}/MAIN']['fields_arr']


    def load_reference_data(self, metadata, data_dict, time_dict, data):
        # Load reference data
        ref_data_config = self.config_loader.ref_data_config
        for group_name in ref_data_config:
            ref_name = f'{group_name}-REF'  # key name in "data"
            if ref_name not in data:        # must be downloaded from DataMaster
                ref_name = [key for key in data if key.startswith(ref_name) and key.endswith('/MAIN')][0][:-5]
            data_array = data[f'{ref_name}/MAIN']['data_arr']
            time_array = data[f'{ref_name}/MAIN']['time_arr'].astype(np.int64)
            data_dict[f'REF/{group_name}'] = data_array     # "REF/..." is the internal key name in "data_dict"
            time_dict[f'REF/{group_name}'] = time_array
            symbols = List.empty_list(types.unicode_type)
            fields = List.empty_list(types.unicode_type)
            if List([1, 2, 3]):             #! numba black magic :(
                symbols.extend(List(data[f'{ref_name}/INFO'].keys()))
                fields.extend(List(data[f'{ref_name}/MAIN']['fields_arr']))
            else:
                symbols.extend(data[f'{ref_name}/INFO'].keys())
                fields.extend(data[f'{ref_name}/MAIN']['fields_arr'])
            metadata[group_name] = Dict.empty(types.unicode_type, types.ListType(types.unicode_type))
            metadata[group_name]['symbols'] = symbols
            metadata[group_name]['fields'] = fields

    @staticmethod
    def load_tradable_table(data_dict, time_dict, data):
        data_array = data['TRADABLE-MIX/TRADABLE_TABLE']['data_arr']
        time_array = data['TRADABLE-MIX/TRADABLE_TABLE']['time_arr']
        data_dict['TRADABLE_TABLE'] = data_array
        time_dict['TRADABLE_TABLE'] = time_array

    @staticmethod
    def set_all_tradable(data_dict):
        # For debug only, to force tradable
        data_dict['TRADABLE_TABLE'].fill(1.0)

    @staticmethod
    def load_signal_remarks(data_dict, time_dict, data):
        # Load signal remarks
        remark_field_names = List.empty_list(types.unicode_type)
        if 'SIG-MRK/MAIN' in data:
            data_array = data['SIG-MRK/MAIN']['data_arr']   # "SIG-MRK" is the key name in "data"
            time_array = data['SIG-MRK/MAIN']['time_arr'].astype(np.int64)
            data_dict['SIGNAL_REMARK'] = data_array     # "SIGNAL_REMARK" is the internal key name in "data_dict"
            time_dict['SIGNAL_REMARK'] = time_array
            if List([1, 2, 3]):     #! numba black magic :(
                remark_field_names.extend(List(data['SIG-MRK/MAIN']['fields_arr']))
            else:
                remark_field_names.extend(data['SIG-MRK/MAIN']['fields_arr'])
        return remark_field_names

    @staticmethod
    def load_secondary_symbols(metadata, group_symbol_map, group_type_map, data):
        # Load all symbols of secondary data for on_data()'s reference
        for group, symbols in group_symbol_map.items():
            if group_type_map[group] == 'monitor':      # secondary data
                for idx, symbol_list in enumerate(data[f'{group}-Strat/ID_symbol_map']):
                    if str(idx) not in metadata:
                        metadata[str(idx)] = Dict.empty(types.unicode_type, types.ListType(types.unicode_type))
                    symbol_list_nb = List.empty_list(types.unicode_type)
                    if List([1, 2, 3]):     #! numba black magic :(
                        symbol_list_nb.extend(List(symbol_list))
                    else:
                        symbol_list_nb.extend(symbol_list)
                    metadata[str(idx)][group] = symbol_list_nb

    def get_masked_secondary_ids(self, group_symbol_map, group_type_map):
        # Get masked secondary IDs for all strategies
        #    - Every strategy has a dict like {secondary_group: list of secondary IDs},
        #      e.g. {'RSI': ['0', '1', '3'], 'KD': ['0', '1', '2', '3']}
        #    - Return a dict {strategy_id: the_dict_mentioned_above}
        masked_ids_all = Dict()
        for sid in range(self.n_strategies):
            masked_ids = Dict()
            for group, symbols in group_symbol_map.items():
                if group_type_map[group] == 'monitor':      # secondary data
                    strategy_symbols = set(self.strategy_pool_df.iloc[sid]['symbol'][group])
                    symbol_list = [str(symbol) for symbol in symbols if symbol in strategy_symbols]
                    symbol_list_nb = List.empty_list(types.unicode_type)
                    if List([1, 2, 3]):     #! numba black magic :(
                        symbol_list_nb.extend(List(symbol_list))
                    else:
                        symbol_list_nb.extend(symbol_list)
                    masked_ids[group] = symbol_list_nb
            if len(masked_ids) > 0:
                masked_ids_all[sid] = masked_ids
        return masked_ids_all

    def load_mask_dict(self, group_symbol_map, group_type_map, symbols, data_dict, data):
        # 找到每个strategy class对应的symbol index
        # 生成每个策略对应的symbol index
        # example： self.symbol = ['AUDJPY', 'USDCAD', 'USDKRW']
        # 第i个策略       symbol = ['AUDJPY', 'USDCAD']
        # strategy_symbol_mask 第i行为： [True, True, False]
        mask_dict = Dict.empty(nb.types.unicode_type, nb.boolean[:, :])
        n_strategy = self.n_strategies
        _symbols = symbols
        for group, type in group_type_map.items():
            # Init mask
            if type == 'order':
                symbols = _symbols
                n_securities = len(symbols)
            else:   # type == 'monitor'
                # Secondary data symbols are the union of all secondary IDs in the strategy pool
                symbols = group_symbol_map[group]
                n_securities = len(symbols)
            strategy_symbol_mask = np.zeros((n_strategy, n_securities), dtype=np.bool)

            # Create mask for each strategy
            for i in range(n_strategy):
                if type == 'order':
                    strategy_symbol = set()
                    for asset in self.main_asset_list:
                        strategy_symbol.update(self.strategy_pool_df.iloc[i]['symbol'][asset])
                else:   # type == 'monitor'
                    strategy_symbol = set(self.strategy_pool_df.iloc[i]['symbol'][group])
                strategy_symbol_mask[i] = np.array([True if symbol in strategy_symbol else False for symbol in symbols])

            # Apply mask to data groups that will appear in on_data() >> data_dict
            if type == 'order':
                for field in self.type_field_dict[group]:
                    mask_dict[f'{group}_{field}'] = strategy_symbol_mask
                if 'TRADABLE_TABLE' in data_dict:
                    mask_dict['TRADABLE_TABLE'] = strategy_symbol_mask
            elif type == 'monitor':
                for field in self.type_field_dict['MONITOR']:
                    if f'{group}-Strat/{field}' in data:
                        mask_dict[f'{group}_{field}'] = strategy_symbol_mask

        # No need masking -> all true. These will appear in on_data() >> ref_data_dict
        ref_data_config = self.config_loader.ref_data_config
        for group_name in ref_data_config:
            # ref_name = f'{group_name}-REF/MAIN'
            ref_symbols = ref_data_config[group_name]['Symbol']
            mask_dict[f'REF/{group_name}'] = np.ones((n_strategy, len(ref_symbols))) == \
                np.ones((n_strategy, len(ref_symbols)))

        if 'SIGNAL_REMARK' in data_dict:
            mask_dict['SIGNAL_REMARK'] = np.ones((n_strategy, len(data['SIG-MRK/MAIN']['symbol_arr']))) == \
                                         np.ones((n_strategy, len(data['SIG-MRK/MAIN']['symbol_arr'])))
        return mask_dict

    @staticmethod
    def load_id_mapping(group_type_map, data):
        # ID mapping: after flattening -> before flattening
        id_mapping = Dict.empty(nb.int64, nb.int64)
        for group, type in group_type_map.items():
            if type == 'monitor':
                id_mapping.update(np.array(data[f'{group}-Strat/id_map'], dtype=np.int64))
                break
        return id_mapping

    def set_window_size(self, data_dict):
        # 每种数据类型的回看长度
        #    - 优先顺序：1-数据独有的，例如FX_MAIN，2-如不满足1则按main指定，
        #              3-如不满足1和2，则默认到长度1。
        window_size_dict = Dict()
        market_field_set = set()
        market_field_set.update(self.type_field_dict['FX'])
        market_field_set.update(self.type_field_dict['STOCKS'])
        market_field_set.update(self.type_field_dict['FUTURES'])
        market_field_set.update(self.type_field_dict['MIX'])
        window_size_config = self.config_loader.window_size_dict
        for key in data_dict.keys():
            if key in window_size_config.keys():
                window_size_dict[key] = window_size_config[key]
            elif 'main' in window_size_config.keys() and any([True for name in
                                                             market_field_set if name.upper() in key]):
                window_size_dict[key] = window_size_config['main']
            elif 'ref_data' in window_size_config.keys() and key.startswith('REF/'):
                window_size_dict[key] = window_size_config['ref_data']
            else:
                window_size_dict[key] = 1
                print(f"Warning: 数据{key}没有指定回看窗口长度，默认为1")
        return window_size_dict

    def calc_ccy_matrix(self, symbols, data):
        # ccy_matrix is a binary table
        main_data_label = self.main_data_label
        n_securities = len(symbols)
        symbol_quote_ccy = [data[f'{main_data_label}-{main_data_label}/INFO'][symbol]['trading_currency']
                            for symbol in symbols]
        quote_ccy = list(set(symbol_quote_ccy))
        n_quote_ccy = len(quote_ccy)
        # ccy_matrix 其实可以不要，先留着
        ccy_matrix = np.zeros((n_quote_ccy, n_securities))
        for i in range(n_securities):
            ccy_matrix[quote_ccy.index(symbol_quote_ccy[i]), i] = 1
        return ccy_matrix, symbol_quote_ccy

    def load_instrument_type(self, symbols, data):
        # instrument_type
        main_data_label = self.main_data_label
        symbol_instrument_type = [data[f'{main_data_label}-{main_data_label}/INFO'][symbol]['symboltype']
                                  for symbol in symbols]
        # Redirect unexpected types from data manager
        for idx, instrument_type in enumerate(symbol_instrument_type):
            instrument_type = instrument_type.upper()
            if instrument_type == 'HK_ETF':
                symbol_instrument_type[idx] = 'HK_STOCK'
            elif instrument_type == 'US_ETF':
                symbol_instrument_type[idx] = 'US_STOCK'
            elif instrument_type.endswith('FUTURES'):
                symbol_instrument_type[idx] = 'FUTURES'
        instrument_type_array = np.array(
            [InstrumentType[type.upper()].value for type in symbol_instrument_type]).astype(np.int64)
        return instrument_type_array

    def load_commission(self, symbols):
        # Commission
        # * all commission should be written in one csv
        n_securities = len(symbols)
        commission_path = self.config_loader.commission_path
        if commission_path is not None:
            commission_df = pd.read_csv(commission_path)
            commission_df.set_index('SECUCODE', inplace=True)
            commission = commission_df.loc[symbols, 'COMMISSION'].values
        elif self.config_loader.commission_fee is not None:
            commission = np.ones(n_securities) * self.config_loader.commission_fee
        else:
            commission = np.zeros(n_securities)
        return commission

    def prepare_data_manager_request(self, group_symbol_map):
        # Adjust start and end dates by window size
        frequency_list = [self.config_loader.data_config[main_asset]['Frequency']
                          for main_asset in self.main_asset_list]
        main_frequency = self.__find_highest_frequency(frequency_list)
        main_load_start, main_load_end = self.__get_date_range_by_window(self.config_loader.window_size_dict['main'],
                                                                         self.config_loader.start_dt,
                                                                         self.config_loader.end_dt,
                                                                         main_frequency)

        self.concat_symbols = []
        for group_label in self.main_asset_list:
            self.concat_symbols.extend(group_symbol_map[group_label])

        # 整理user需要的group给到data manager
        # Main data and secondary data
        data_manager_config = {"Data": {}}
        i_group = 0
        n_skip = 0
        tradable_config_args = {}
        for group_label, symbols in group_symbol_map.items():
            symbols = group_symbol_map[group_label]
            group_config = self.config_loader.data_config.get(group_label, {})
            group_config['Label'] = group_label
            group_config['Type'] = group_config.get('Type', 'Download')
            group_config['Symbol'] = symbols        # symbols are configured by strategy pool

            self.__ohlc_patch(group_config, group_label)    # ensure "Fields" have OHLC in front
            self.__group_config_append_dates(group_config, main_load_start, main_load_end)
            if group_config['DataCenter'] != 'LocalCSV':
                if ('SymbolArgs' in group_config and 'AccountCurrency' in group_config['SymbolArgs'] and
                        group_config['SymbolArgs']['AccountCurrency'] != self.config_loader.base_ccy):
                    print('Warning: data配置的AccountCurrency和账户base_ccy有出入，采纳base_ccy。')
                    group_config['SymbolArgs']['AccountCurrency'] = self.config_loader.base_ccy
                # TODO: Dividend from DataMaster is temporarily disabled for incomplete data
                if 'SymbolArgs' in group_config:
                    group_config['SymbolArgs']['DMGetDividend'] = False
                data_manager_config['Data'].update({f'DataGroup{i_group + n_skip}': group_config})
            else:       # LocalCSV
                n_groups = 0
                for group_config in self.__unpack_local_csv_group_config(group_label, symbols):
                    self.__group_config_append_dates(group_config, main_load_start, main_load_end)
                    # TODO: Dividend from DataMaster is temporarily disabled for incomplete data
                    group_config['SymbolArgs']['DMGetDividend'] = False
                    data_manager_config['Data'].update({f'DataGroup{i_group + n_groups}': group_config})
                    n_groups += 1
                n_skip += n_groups - 1
                #! Tradable table needs info.csv path in DataCenterArgs for FX. Here, we attach that
                #  information. This may cause problems if multiple local CSV groups are allowed for
                #  the same asset in the future.
                if group_label == 'FX':
                    tradable_config_args['DataCenterArgs'] = {'info': group_config['DataCenterArgs']['info']}
            i_group += 1

        # Tradable table
        if self.config_loader.tradable_table_config == {}:
            # no config, download tradable table from DataMaster
            group_config = {'Label': 'TRADABLE', 'Type': 'Download', 'DataCenter': 'DataMaster',
                            'Symbol': self.concat_symbols, 'Fields': 'tradable_table', 'Frequency': main_frequency,
                            'SymbolArgs': {'AccountCurrency': self.config_loader.base_ccy}}
        else:
            # has local tradable table
            tradable_table_dir = self.config_loader.tradable_table_config['dir']
            group_config = {'Label': 'TRADABLE', 'Type': 'Download', 'DataCenter': 'LocalCSV',
                            'DataCenterArgs':{'dir': tradable_table_dir , 'key_name':'TRADABLE-MIX/TRADABLE_TABLE'},
                            'Symbol': self.concat_symbols, 'Fields': 'tradable_table', 'Frequency': main_frequency,
                            'SymbolArgs': {'AccountCurrency': self.config_loader.base_ccy}}
        group_config.update(tradable_config_args)
        self.__group_config_append_dates(group_config, main_load_start, main_load_end)
        data_manager_config['Data'].update({f'DataGroup{i_group + n_skip}': group_config})
        i_group += 1

        # Multi-assets
        if len(self.main_asset_list) > 1:
            asset_lables = [f'{asset}-{asset}' for asset in self.main_asset_list]
            group_config = {'Label': 'MIX', 'Type': 'Download', 'DataCenter': 'DataManager', 'DMActions': 'STACK',
                            'Symbol': asset_lables, 'Fields': [], 'Frequency': main_frequency}
            self.__group_config_append_dates(group_config, main_load_start, main_load_end)
            data_manager_config['Data'].update({f'DataGroup{i_group + n_skip}': group_config})
            i_group += 1

        # Requests for reference data
        ref_data_config = self.config_loader.ref_data_config
        for idx, group_label in enumerate(ref_data_config):
            group_config = ref_data_config[group_label]
            group_config['Type'] = 'Download'
            if 'Frequency' not in group_config:
                group_config['Frequency'] = 'DAILY'

            if group_config['DataCenter'] == 'LocalCSV':
                group_config['Label'] = group_label
                group_config['DataCenterArgs']['key_name'] = f'{group_label}-REF/MAIN'
                group_config['SymbolArgs'] = {'DataSource': f'reference_data_{idx}'}
                if 'Fields' not in group_config:
                    group_config['Fields'] = []
            elif group_config['DataCenter'] == 'DataMaster':
                group_config['Label'] = group_label + '-REF'
                if ('SymbolArgs' in group_config and 'AccountCurrency' in group_config['SymbolArgs'] and
                        group_config['SymbolArgs']['AccountCurrency'] != self.config_loader.base_ccy):
                    print('Warning: ref_data配置的AccountCurrency和账户base_ccy有出入，采纳base_ccy。')
                    group_config['SymbolArgs']['AccountCurrency'] = self.config_loader.base_ccy
                if 'Fields' not in group_config:    # any available fields are ok for ref_data
                    group_config['Fields'] = 'OHLC'
                if 'Symbol' not in group_config:
                    print('Warning: ref_data配置有从DataMaster取，但没有定义Symbol，默认取main data的Symbol。')
                    group_config['Symbol'] = self.concat_symbols
                elif isinstance(group_config['Symbol'], str):
                    group_config['Symbol'] = group_symbol_map[group_config['Symbol']]

            if 'StartDate' in group_config and 'EndDate' in group_config:
                start_date = group_config['StartDate']
                end_date = group_config['EndDate']
                dt_start = datetime(start_date['Year'], start_date['Month'], start_date['Day'], tzinfo=pytz.utc)
                dt_end = datetime(end_date['Year'], end_date['Month'], end_date['Day'], tzinfo=pytz.utc)
                load_start, load_end = self.__get_date_range_by_window(self.config_loader.window_size_dict.get('ref_data', 1),
                                                                       dt_start, dt_end, group_config['Frequency'])
            else:
                load_start, load_end = main_load_start, main_load_end
            self.__group_config_append_dates(group_config, load_start, load_end)
            data_manager_config['Data'].update({f'DataGroup{i_group + n_skip}': group_config})
            i_group += 1

        # Requests for signal remark
        remark_load_dir = self.config_loader.remark_load_dir
        remark_load_name = self.config_loader.remark_load_name
        if remark_load_dir is not None and remark_load_name is not None:
            group_config = {'Label': 'signal_remark', 'DataCenter': 'LocalCSV', 'Type': 'Download',
                            'DataCenterArgs': {'key_name': 'SIG-MRK/MAIN'},
                            'SymbolArgs': {'DataSource': 'signal_remark'}, 'Frequency': 'DAILY'}
            group_config['DataCenterArgs']['dir'] = remark_load_dir
            remark_names = [name[:-4] for name in os.listdir(remark_load_dir)
                            if name.startswith(remark_load_name) and name.endswith('.csv')]
            group_config['Symbol'] = remark_names
            group_config['Fields'] = self.__get_all_field_names(remark_names)
            group_config['Fields'].sort()
            self.__group_config_append_dates(group_config, main_load_start, main_load_end)
            data_manager_config['Data'].update({f'DataGroup{i_group + n_skip}': group_config})
            i_group += 1

        # dm_pickle
        pickle_config = self.config_loader.pickle_config
        data_manager_config.update({'dm_pickle': pickle_config})
        if pickle_config and not os.path.exists(pickle_config['save_dir']):
            os.makedirs(pickle_config['save_dir'])

        return data_manager_config

    def __ohlc_patch(self, group_config, group_label):
        # For main data, OHLC is required in front, others are optional
        if group_label in self.main_asset_list:
            if 'Fields' in group_config and group_config['Fields'] != 'OHLC':
                if isinstance(group_config['Fields'], str):
                    group_config['Fields'] = [group_config['Fields']]
                if 'OHLC' in group_config['Fields']:
                    group_config['Fields'].remove('OHLC')
                group_config['Fields'].insert(0, 'OHLC')
            elif 'Fields' not in group_config:
                group_config['Fields'] = ['OHLC']

    @staticmethod
    def __group_config_append_dates(group_config, load_start, load_end):
        group_config['StartDate'] = {'Year': load_start.year,
                                     'Month': load_start.month,
                                     'Day': load_start.day}
        group_config['EndDate'] = {'Year': load_end.year,
                                   'Month': load_end.month,
                                   'Day': load_end.day,
                                   'Hour': 23,
                                   'Minute': 59,
                                   'Second': 59}

    def __unpack_local_csv_group_config(self, group, symbols):
        group_config = self.config_loader.data_config.get(group, {})
        assert group_config['DataCenter'] == 'LocalCSV'

        data_group_loop = ['main_dir', 'fxrate_dir', 'int_dir']

        # Check if all dirs are given
        data_group_missing = set([dir_name for dir_name in data_group_loop if dir_name
                                 not in group_config['DataCenterArgs']])
        symbol_args = {}
        if len(data_group_missing) > 0:
            assert 'main_dir' not in data_group_missing
            # There are rates not given by the user. Append a data group that requests these rates from
            # data master instead.
            symbol_args['DMAdjustType'] = group_config['SymbolArgs'].get('DMAdjustType', 1) \
                if 'SymbolArgs' in group_config else 1
            symbol_args['AccountCurrency'] = self.config_loader.base_ccy
            symbol_args['DatacenterMap'] = group_config['DataCenterArgs']['info']
            unpacked_config = {'Label': f'{group}', 'DataCenter': 'DataMaster',     #! change label => wrong return from data manager
                               'Type': 'Download', 'Symbol': symbols, 'SymbolArgs': symbol_args,
                               'Fields': group_config['Fields'], 'Frequency': group_config['Frequency']}
            yield unpacked_config

        for dir_name in data_group_loop:
            if dir_name in data_group_missing:
                continue

            unpacked_config = {'DataCenter': 'LocalCSV', 'Type': 'Download', 'Symbol': symbols,
                               'Frequency': group_config['Frequency'], 'DataCenterArgs': {}}
            if 'main' in dir_name:
                unpacked_config['Label'] = f'{group}-MAIN'
                self.__ohlc_patch(group_config, group)
                unpacked_config['Fields'] = group_config['Fields']
                unpacked_config['DataCenterArgs']['key_name'] = f'{group}-{group}/MAIN'
            elif 'fxrate' in dir_name:
                unpacked_config['Label'] = f'{group}-FXRATE'
                unpacked_config['Fields'] = ['fx_rate']
                unpacked_config['DataCenterArgs']['key_name'] = f'{group}-{group}/CCPFXRATE'
            else:
                assert 'int' in dir_name
                unpacked_config['Label'] = f'{group}-INT'
                unpacked_config['Fields'] = ['interest_rate']
                unpacked_config['DataCenterArgs']['key_name'] = f'{group}-{group}/INT'
            unpacked_config['DataCenterArgs']['dir'] = group_config['DataCenterArgs'][dir_name]
            unpacked_config['DataCenterArgs']['info'] = group_config['DataCenterArgs']['info']
            unpacked_config['SymbolArgs'] = symbol_args
            yield unpacked_config

    @staticmethod
    def __get_date_range_by_window(window_size, dt_start, dt_end, frequency):
        # 由于策略看到的数据有window，需要往回多拉look_back的数据
        # 由于利率需要用到明天的数据，需要往后多拉look_forward的数据

        # Convert window_size to window measured by day
        if frequency == 'HOURLY':
            # Assuming a day has at least 6 timestamps
            window_size = window_size // 6 + 1
        elif frequency == 'MINUTELY':
            window_size = window_size // (6 * 60) + 1
        elif frequency == 'SECONDLY':
            window_size = window_size // (6 * 60 * 60) + 1

        look_back = 2 * max(window_size, 1) + 11
        look_forward = 11
        load_start = dt_start - timedelta(days=look_back)
        load_end = dt_end + timedelta(days=look_forward)
        return load_start, load_end

    @staticmethod
    def __find_highest_frequency(frequency_list):
        ordered_frequency = ['DAILY', 'HOURLY', 'MINUTELY', 'SECONDLY']
        max_index = 0
        for freq in frequency_list:
            try:
                index = ordered_frequency.index(freq)
            except ValueError:
                raise ValueError(f'Invalid frequency "{freq}" in main data config.')
            if index > max_index:
                max_index = index
        return ordered_frequency[max_index]

    def __get_all_field_names(self, remark_names):
        all_fields = set()
        for name in remark_names:
            name += '.fields'
            f_field = os.path.join(self.config_loader.remark_load_dir, name)
            fields = open(f_field, 'r').readlines()
            all_fields.update(fields)
        return [name[:-1] for name in all_fields]
