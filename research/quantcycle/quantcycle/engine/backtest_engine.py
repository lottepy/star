"""
Backtesting Engine Main Class
"""

from multiprocessing import Process, Manager
from quantcycle.app.data_manager import DataManager
from quantcycle.app.data_manager import DataDistributorSub
from quantcycle.app.order_crosser.order_manager import OrderManager
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.status_recorder import BaseStatusRecorder
from quantcycle.app.data_manager.data_distributor_mock import DataDistributor
from quantcycle.app.result_exporter.result_export import ResultExport
from quantcycle.app.result_exporter.utils import OrderLogger
from quantcycle.app.result_exporter.utils import UserSave
from quantcycle.utils.backtest_helper import is_new_day, transfer_jitclass_attribution, remove_files_matching
from quantcycle.engine.backtest_engine_loader import *


def backtest_core_multiprocess_wrapper(engine, queue, python_parallel_params):
    engine.start_backtest(python_parallel_params)
    pms_batch = {}
    strategy_batch = {}
    for i in range(python_parallel_params['start_idx'], python_parallel_params['end_idx']):
        pms_batch[i] = engine.strategy_id_pms_dict[i]
        strategy_batch[i] = engine.strategy_id_strategy_dict[i]
    results = {'pms_batch': pms_batch,
               'strategy_batch': strategy_batch}
    queue.put(results)


class BacktestEngine:

    def __init__(self):
        self.strategy_id_strategy_dict = Dict()
        self.strategy_id_pms_dict = Dict()
        self.config_loader = None
        self.strategy_pool_df = None
        self.main_data_label = None
        self.main_data_key = None
        self.n_strategies = 0
        self.strategy_params = None
        self.max_order_feedback = 0

    def load_config(self, config, strategy_pool_df=None):
        config_loader = ConfigLoader(config, strategy_pool_df)
        config_loader.validate_config()
        self.config_loader = config_loader
        self.strategy_pool_df = config_loader.strategy_pool_df
        self.n_strategies = config_loader.n_strategies
        self.strategy_params = config_loader.strategy_params
        self.max_order_feedback = config_loader.max_order_feedback

    def prepare(self):
        self.__load_data()
        self.__load_other_app()
        self.__load_status()

    def __load_data(self):
        # init data_manager
        self.data_manager = DataManager()
        self.data_manager.prepare()

        data_loader = DataLoader(self.config_loader)
        self.main_data_label = data_loader.main_data_label
        self.main_data_key = data_loader.main_data_key

        # Mapping group -> symbols, symbols are loaded from strategy pool
        # Symbols are sorted; duplicates are removed
        group_symbol_map = data_loader.load_group_symbol_map()

        # 把group分为order和monitor两类，order用于清算，monitor只用于观察
        self.symbols = []
        group_type_map = {}
        for group, symbols in group_symbol_map.items():
            if isinstance(group_symbol_map[group][0], int):
                group_type_map[group] = 'monitor'
            else:
                group_type_map[group] = 'order'
                self.symbols += symbols

        # ------------------- 开始从数据导入模块获取数据 -----------------------
        data_manager_config = data_loader.prepare_data_manager_request(group_symbol_map)
        self.data_manager.load_config(data_manager_config)
        self.data_manager.run()

        data = self.data_manager.data_distributor_main.data_package

        data_distributor_sub = DataDistributorSub()
        data = data_distributor_sub.unpack_data(data)
        # ------------------- 结束从数据导入模块获取数据 -----------------------

        # Distribute data from data manager to internal data containers
        data_loader.rebuild_maps(group_type_map, group_symbol_map)
        self.symbols = data_loader.concat_symbols.copy()
        data_dict, time_dict = data_loader.init_data_time_dict(group_type_map, data)
        self.metadata = Dict.empty(types.unicode_type,
                                   types.DictType(types.unicode_type, types.ListType(types.unicode_type)))
        # Main data (strategy-independent)
        #    - strategy-dependent part is a few blocks below
        self.metadata['main'] = Dict.empty(types.unicode_type, types.ListType(types.unicode_type))
        self.metadata['main']['fields'] = data_loader.get_main_data_fields(data)
        # Reference data
        data_loader.load_reference_data(self.metadata, data_dict, time_dict, data)
        # Signal remarks
        remark_fields = data_loader.load_signal_remarks(data_dict, time_dict, data)
        if remark_fields:
            self.metadata['remark'] = Dict.empty(types.unicode_type, types.ListType(types.unicode_type))
            self.metadata['remark']['fields'] = remark_fields
        # Secondary data
        data_loader.load_secondary_symbols(self.metadata, group_symbol_map, group_type_map, data)
        self.masked_secondary_ids = data_loader.get_masked_secondary_ids(group_symbol_map, group_type_map)
        # Engine peripherals
        data_loader.load_tradable_table(data_dict, time_dict, data)
        # data_loader.set_all_tradable(data_dict)     #! debug only
        self.mask_dict = data_loader.load_mask_dict(group_symbol_map, group_type_map, self.symbols, data_dict, data)
        self.id_mapping = data_loader.load_id_mapping(group_type_map, data)
        self.instrument_type_array = data_loader.load_instrument_type(self.symbols, data)
        self.commission = data_loader.load_commission(self.symbols)
        window_size_dict = data_loader.set_window_size(data_dict)
        self.ccy_matrix, self.symbol_quote_ccy = data_loader.calc_ccy_matrix(self.symbols, data)

        # 数据装入data_distributor
        self.data_distributor = DataDistributor(window_size_dict=window_size_dict,
                                                start_time=self.config_loader.start_dt.timestamp(),
                                                end_time=self.config_loader.end_dt.timestamp(),
                                                n_strategy=self.n_strategies,
                                                mask_dict=self.mask_dict,
                                                main_field=self.main_data_key)
        # TODO: data_distributor一次load所有数据, load一个字典
        for key, value in data_dict.items():
            data_array = data_dict[key]
            time_array = time_dict[key]
            self.data_distributor.load_data(data_array, time_array, key)
            print(f"info: 成功载入数据{key}")
        self.data_distributor.prepare()
        self.time_dots = self.data_distributor.time_dots

    def __load_other_app(self):

        self.strategy_recorder = BaseStatusRecorder()
        self.order_crosser = OrderManager(commission=self.commission,
                                          ccy_matrix=self.ccy_matrix,
                                          instrument_type_array=self.instrument_type_array)
        strategy_symbol_mask = self.mask_dict[self.main_data_key]

        for id in range(0, self.n_strategies):
            strategy_id_symbol_mask = strategy_symbol_mask[id]
            strategy_id_ccy_matrix = self.ccy_matrix[:, strategy_id_symbol_mask]
            strategy_id_instrument_type = self.instrument_type_array[strategy_id_symbol_mask]
            self.strategy_id_pms_dict[id] = PorfolioManager(CASH=self.config_loader.cash,
                                                            ccy_matrix=strategy_id_ccy_matrix,
                                                            instrument_type_array=strategy_id_instrument_type)
            strategy_id_strategy_module = importlib.import_module(self.strategy_pool_df.iloc[id]['strategy_module'])
            strategy_id_strategy_class = getattr(strategy_id_strategy_module,
                                                 self.strategy_pool_df.iloc[id]["strategy_name"])

            # Load symbols for this strategy.
            # on_data() needs references to these symbols.
            assert len(self.symbols) == len(self.symbol_quote_ccy)      # lengths of global attributes should match
            main_symbols = np.array(self.symbols)[strategy_id_symbol_mask]
            ccy_list = np.array(self.symbol_quote_ccy)[strategy_id_symbol_mask]
            # assert len(main_symbols) == len(strategy_id_instrument_type) \
            #        == len(ccy_list)    # hopefully, all these attributes are local to the strategy

            # Metadata - main data (strategy-dependent)
            #! numba black magic included :(
            numba_probe = List([1, 2, 3])
            self.metadata['main']['symbols'] = List(main_symbols) if numba_probe else main_symbols
            symbol_types = [InstrumentType(sid).name for sid in strategy_id_instrument_type]
            self.metadata['main']['symbol_types'] = List(symbol_types) if numba_probe else symbol_types
            self.metadata['main']['ccy_list'] = List(ccy_list) if numba_probe else ccy_list
            if id in self.masked_secondary_ids:
                self.metadata['masked_secondary_ids'] = self.masked_secondary_ids[id]

            self.strategy_id_strategy_dict[id] = strategy_id_strategy_class(self.strategy_id_pms_dict[id],
                                                                            self.strategy_pool_df.strategy_name[id],
                                                                            self.strategy_params[id],
                                                                            self.metadata,
                                                                            strategy_id_ccy_matrix,
                                                                            self.id_mapping)

            self.strategy_id_strategy_dict[id].init()

    # ----------------- Core Zone -----------------
    #           * Mostly numba enclosed *
    # ---------------------------------------------
    def start_backtest(self, python_parallel_params=None):
        # Fake class generator
        class Name:
            def __init__(self, name):
                self.name = name
        try:
            data_distributor_type = DataDistributor.class_type.instance_type
            order_crosser_type = OrderManager.class_type.instance_type
            pms_type = PorfolioManager.class_type.instance_type
            strategy_type = self.config_loader.strategy_class.class_type.instance_type
            strategy_id_pms_dict_type = types.DictType(nb.int64, pms_type)
            strategy_id_strategy_dict_type = types.DictType(nb.int64, strategy_type)
            string_type = types.unicode_type
            max_order_feedback_type = types.int64
            # datetime64_type = types.NPDatetime('s')
        except:
            data_distributor_type = Name("data_distributor_type")
            order_crosser_type = Name("order_crosser_type")
            # pms_type = Name("pms_type")
            # strategy_type = Name("strategy_type")
            strategy_id_pms_dict_type = Name("strategy_id_pms_dict_type")
            strategy_id_strategy_dict_type = Name("strategy_id_strategy_dict_type")
            string_type = Name('string_type')
            max_order_feedback_type = Name('int_type')
            # datetime64_type = Name('datetime64_type')

        cn_stock_rollover_anchor = np.datetime64('1970-01-01T00:00:00')     # rollover at 00:00 UTC
        calc_rate_anchor = self.config_loader.timing.get('calc_rate_time_utc', None)
        if calc_rate_anchor is None:
            calc_rate_anchor = np.datetime64('1970-01-01T22:00:00')         # calc rate at 22:00 UTC by default
        else:
            assert len(calc_rate_anchor) == 5
            calc_rate_anchor = np.datetime64(f'1970-01-01T{calc_rate_anchor}:00')
            # print(f'anchor_time set to {anchor_time}')      # for debug only

        @nb.njit(nb.void
                 (data_distributor_type, order_crosser_type, strategy_id_pms_dict_type,
                  strategy_id_strategy_dict_type, string_type, max_order_feedback_type))
        def _backtest_core(data_distributor, order_crosser, strategy_id_pms_dict, strategy_id_strategy_dict,
                           main_data_label, max_order_feedback):
            time_dots = data_distributor.time_dots

            strategy_symbol_mask = data_distributor.strategy_mask_dict[data_distributor.main_field]
            n_exchange_symbol = order_crosser.n_security
            start_id = list(strategy_id_pms_dict.keys())[0]
            end_id = list(strategy_id_pms_dict.keys())[-1]
            last_timestamp = Dict.empty(nb.int64, nb.int64)
            last_rate_time = List()
            for _ in range(end_id + 1):
                last_rate_time.append(0)
            ccpfxrate_key = main_data_label + '_CCPFXRATE'
            int_key = main_data_label + '_INT'
            main_key = main_data_label + '_MAIN'
            market_data_names = {'FX_MAIN': 0, 'FX_CCPFXRATE': 1, 'FX_INT': 2, 'STOCKS_MAIN': 3,
                                 'STOCKS_CCPFXRATE': 4, 'STOCKS_INT': 5, 'FUTURES_MAIN': 6,
                                 'FUTURES_CCPFXRATE': 7, 'FUTURES_INT': 8, 'MIX_MAIN': 9,
                                 'MIX_CCPFXRATE': 10, 'MIX_INT': 11}

            for i in range(time_dots):
                window_data_dict, window_time_dict = data_distributor.distribute_exchange_data(start_id)

                current_exchange_data = window_data_dict[main_key][-1, :, 3]
                current_exchange_fx_data = window_data_dict[ccpfxrate_key][-1, :, 0]
                current_exchange_time = window_time_dict[main_key][-1, 0]

                for j in nb.prange(start_id, end_id + 1):
                    # Get data for strategy j
                    symbol_mask = strategy_symbol_mask[j]
                    window_data_dict, window_time_dict = data_distributor.distribute(j)
                    window_data, window_time = window_data_dict[main_key], window_time_dict[main_key]
                    window_fx_data, window_fx_time = window_data_dict[ccpfxrate_key], window_time_dict[ccpfxrate_key]
                    window_rate_data, window_rate_time = window_data_dict[int_key], window_time_dict[int_key]
                    window_tradable_data, window_tradable_time = window_data_dict['TRADABLE_TABLE'], \
                                                                 window_time_dict['TRADABLE_TABLE']

                    # Data may contain nan
                    current_data, current_time = window_data[-1, :, 3], window_time[-1, 0]
                    current_fx_data, current_fx_time = window_fx_data[-1, :, 0], window_fx_time[-1, 0]
                    current_rate_data, current_rate_time = window_rate_data[-1, :, 0], window_rate_time[-1, 0]
                    tradable_status = window_tradable_data[-1, :, 0].astype(np.int64)

                    # Passing 00:00 UTC rollover (CN stocks T+1 rule)
                    if is_new_day(current_time, last_timestamp.get(j, -1), cn_stock_rollover_anchor):
                        strategy_id_pms_dict[j].reset_field_rollover_day()

                    # Backtest for strategy j
                    strategy_id_pms_dict[j].reset_trade_status(tradable_status)
                    strategy_id_pms_dict[j].calculate_spot(current_data, current_fx_data)
                    strategy_id_strategy_dict[j].save_current_data(window_fx_data[-1, :, 0], window_data[-1, :, 3],
                                                                   current_time)

                    # Assemble needed fields for on_data()
                    data_dict = Dict()
                    ref_data_dict = Dict()
                    data_dict['main'] = window_data
                    data_dict['fxrate'] = window_fx_data
                    data_dict['int'] = window_rate_data
                    for field, data in window_data_dict.items():
                        if field not in market_data_names and data.shape[0] > 0:
                            if field.startswith('REF/'):
                                ref_data_dict[field[4:]] = data
                            elif field == 'SIGNAL_REMARK':
                                ref_data_dict['signal_remark'] = data
                            elif field == 'TRADABLE_TABLE':
                                data_dict['ex_tradable'] = np.copy(data)    # exchange tradable status
                            else:
                                data_dict[field] = data
                    # Note these dup blocks can't be extracted as a method because of dimension variation
                    time_dict = Dict()
                    ref_time_dict = Dict()
                    time_dict['main'] = window_time
                    time_dict['fxrate'] = window_fx_time
                    time_dict['int'] = window_rate_time
                    for field, data in window_time_dict.items():
                        if field not in market_data_names and data.shape[0] > 0:
                            if field.startswith('REF/'):
                                ref_time_dict[field[4:]] = data
                            elif field == 'SIGNAL_REMARK':
                                ref_time_dict['signal_remark'] = data
                            elif field == 'TRADABLE_TABLE':
                                time_dict['ex_tradable'] = np.copy(data)    # exchange tradable status
                            else:
                                time_dict[field] = data

                    # TODO: order_array应该有两行，另外一行为目标价格
                    order_array = strategy_id_strategy_dict[j].on_data(data_dict, time_dict,
                                                                       ref_data_dict, ref_time_dict)
                    if order_array is None:
                        order_array = np.zeros((1, data_dict['main'].shape[1]))

                    for num_order_feedback in range(max_order_feedback + 1):
                        # on_data 本身要下一次单 所以 +1
                        order_array[0], pms_status = strategy_id_pms_dict[j].check_order(order_array[0], current_time)

                        # 把order_array扩成总的symbol数目
                        exchange_order_array = np.zeros(n_exchange_symbol)
                        exchange_order_array[symbol_mask] = order_array[0]

                        j, exchange_trade_array = order_crosser.cross_order(exchange_order_array, current_exchange_data,
                                                                            current_exchange_fx_data,
                                                                            j, current_exchange_time, current_exchange_data)
                        # 把返回的撮合数据缩成策略j需要的symbol
                        trade_array = exchange_trade_array[:, symbol_mask]
                        strategy_id_pms_dict[j].receive_order_feedback(trade_array)
                        strategy_id_pms_dict[j].capture(current_time)

                        # --- on_order_feedback ---
                        if max_order_feedback == 0:
                            # no order feedback
                            break
                        if num_order_feedback == max_order_feedback:
                            # or reach maximum feedback times
                            print("Warning: Reach maximum order feedback limit", num_order_feedback, "!")
                            break
                        order_array = strategy_id_strategy_dict[j].on_order_feedback(trade_array, data_dict, time_dict,
                                                                                     ref_data_dict, ref_time_dict)
                        if order_array is None or len(np.nonzero(order_array)[0]) == 0:
                            #  no order feedback or given a all-zero order
                            break
                        # --- on_order_feedback ---


                    # 时间经过22:00UTC（默认）计息
                    if is_new_day(current_time, last_timestamp.get(j, -1), calc_rate_anchor):
                        if current_rate_time > last_rate_time[j]:
                            strategy_id_pms_dict[j].calculate_rate(current_rate_data, current_data, current_fx_data)
                            last_rate_time[j] = current_rate_time
                        # print(f'{j}: Calc rate at {datetime.fromtimestamp(current_time, pytz.utc)}')    # debug only
                    last_timestamp[j] = current_time

        n_strategies = self.n_strategies

        # Numba parallel entry
        @nb.njit(nb.void
                 (data_distributor_type, order_crosser_type,
                  strategy_id_pms_dict_type, strategy_id_strategy_dict_type, string_type, max_order_feedback_type),
                 parallel=self.config_loader.is_numba_parallel)
        def _start_backtest_parallel(data_distributor, order_crosser, strategy_id_pms_dict,
                                     strategy_id_strategy_dict, main_data_label, max_order_feedback):
            n_batches = n_strategies
            batch_size = (n_strategies - 1) // n_batches + 1
            n_batches = (n_strategies - 1) // batch_size + 1
            for i in nb.prange(n_batches):
                start_idx = i * batch_size
                end_idx = min((i + 1) * batch_size, n_strategies)
                strategy_batch = Dict()
                pms_batch = Dict()
                for j in nb.prange(start_idx, end_idx):
                    strategy_batch[j] = strategy_id_strategy_dict[j]
                    pms_batch[j] = strategy_id_pms_dict[j]
                _backtest_core(data_distributor, order_crosser, pms_batch, strategy_batch, main_data_label, max_order_feedback)

        # Python parallel entry
        def _start_backtest_python_parallel(data_distributor, order_crosser, strategy_id_pms_dict,
                                            strategy_id_strategy_dict, main_data_label, max_order_feedback, n_workers):
            assert n_strategies == len(strategy_id_pms_dict) == len(strategy_id_strategy_dict)
            problem_size = n_strategies
            batch_size = problem_size // n_workers
            jobs = []
            process_manager = Manager()
            queue = process_manager.Queue()
            for batch_id in range(n_workers):
                start_idx = batch_id * batch_size
                if batch_id != n_workers - 1:
                    end_idx = (batch_id + 1) * batch_size
                else:
                    end_idx = problem_size
                # Load data
                pms_batch = {}
                strategy_batch = {}
                for i in range(start_idx, end_idx):
                    pms_batch[i] = strategy_id_pms_dict[i]
                    strategy_batch[i] = strategy_id_strategy_dict[i]

                starter_params = {'data_distributor': data_distributor, 'order_crosser': order_crosser,
                                  'pms_batch': pms_batch, 'strategy_batch': strategy_batch, 'max_order_feedback': max_order_feedback,
                                  'main_data_label': main_data_label, 'start_idx': start_idx, 'end_idx': end_idx}
                p = Process(target=backtest_core_multiprocess_wrapper, args=(self, queue, starter_params))
                jobs.append(p)
                p.start()

            # Block until all jobs done
            for job in jobs:
                job.join()
            # Unpack results
            while not queue.empty():
                res = queue.get()
                for k, v in res['pms_batch'].items():
                    self.strategy_id_pms_dict[k] = v
                for k, v in res['strategy_batch'].items():
                    self.strategy_id_strategy_dict[k] = v

        if python_parallel_params is not None:
            _backtest_core(python_parallel_params['data_distributor'],
                           python_parallel_params['order_crosser'],
                           python_parallel_params['pms_batch'],
                           python_parallel_params['strategy_batch'],
                           python_parallel_params['main_data_label'],
                           python_parallel_params['max_order_feedback'])
            return

        # -------- The Real Start Begins Here ---------
        ts = datetime.now()

        # Numba parallel can work when python parallel is disabled. This is the default choice.
        if self.config_loader.python_parallel_n_workers < 2:
            _start_backtest_parallel(self.data_distributor, self.order_crosser,
                                     self.strategy_id_pms_dict, self.strategy_id_strategy_dict,
                                     self.main_data_label, self.max_order_feedback)
        else:   # Go for python parallel, assuming Numba JIT is disabled.
            _start_backtest_python_parallel(self.data_distributor, self.order_crosser, self.strategy_id_pms_dict,
                                            self.strategy_id_strategy_dict, self.main_data_label, self.max_order_feedback,
                                            self.config_loader.python_parallel_n_workers)

        te = datetime.now()
        print("回测用时：", te - ts)

        # temporarily check
        # import matplotlib.pyplot as plt
        # security_pnl = np.array(self.strategy_id_pms_dict[0].historial_pnl)
        # pnl = security_pnl.sum(axis=1)
        # pnl_df = pd.DataFrame(pnl, columns=['pnl'])
        # pnl_df.to_csv("pnl.csv")
        # plt.plot(pnl)
        # plt.show()


    def __load_status(self):

        if self.config_loader.load:
            status_dict = self.strategy_recorder.load(self.config_loader.status_name, self.config_loader.status_dir)
            # 若有载入的status_dict，需要检查status_dict格式是否符合要求
            if status_dict is not None:
                assert isinstance(status_dict, dict), "输入的状态不是dict"
                status_dict_key_type = set(type(key) for key in status_dict.keys())
                assert len(status_dict_key_type) == 1, "输入的状态是dict，但key有多种type"
                assert isinstance(list(status_dict.keys())[0], int), "输入的状态是dict，但key不是int"
                assert len(status_dict) == self.n_strategies, \
                    f"输入的状态数目({len(status_dict)})与策略数目({self.n_strategies})不一致，变更参数请删除之前保存的状态。"
            # 若有载入的status_dict格式是否符合要求，开始载入状态
            for id, strategy in self.strategy_id_strategy_dict.items():
                strategy_status = Dict.empty(types.unicode_type, nb.float64)
                if status_dict is not None:
                    assert isinstance(status_dict[id], dict), f"输入的第{id}状态不是dict"
                    for key, value in status_dict[id].items():
                        assert isinstance(key, str), f"输入的第{id}状态是dict,但key不是string"
                        assert isinstance(value, float), f"输入的第{id}状态是dict,但value不是float"
                        strategy_status[key] = value
                self.strategy_id_strategy_dict[id].load_status(strategy_status)

    def save_status(self):
        """
        把M个strategy的状态以dict形式存储下来
        status_dict -> dict 
        key: strategy_id, value: strategy_status
        strategy_status -> dict 
        key：status_name, value: status_value
        以self.status_name的名字存在self.status_dir相对路径里
        """
        status_dir = self.config_loader.status_dir
        if self.config_loader.dump:
            if not os.path.exists(status_dir):
                print("info: 状态存储路径不存在，创建路径")
                os.makedirs(status_dir)
            status_dict = dict()
            for id, strategy in self.strategy_id_strategy_dict.items():
                strategy.save_status()
                strategy_status = dict()
                for key, value in strategy.status.items():
                    strategy_status[key] = value
                status_dict[id] = strategy_status
            self.strategy_recorder.dump(status_dict, self.config_loader.status_name, status_dir)

    def export_pms_results(self):
        result_dir = self.config_loader.result_dir
        result_filename = self.config_loader.result_filename
        if not os.path.exists(result_dir):  # 如果结果存储路径不存在，创建路径
            print("info: 结果存储路径不存在，创建路径")
            os.makedirs(result_dir)
        else:   # 如果目标文件存在，就先删除, 相当于新结果覆盖原结果
            print("Warning: 结果路径非空，新结果将覆盖原结果")
            filename_main, _ = os.path.splitext(result_filename)
            remove_files_matching(result_dir, f'^{filename_main}.*')

        # 把numba class转成可以pickle的class
        # TODO: Optimize time in pms_fakes creation
        save_n_workers = self.config_loader.save_n_workers
        pms_fakes = [transfer_jitclass_attribution(self.strategy_id_pms_dict[i]) for i in range(self.n_strategies)]
        strategy_symbol_mask = self.data_distributor.strategy_mask_dict[self.data_distributor.main_field]
        n_securities = len(self.symbols)

        # Export to HDF5
        result_export = ResultExport(result_dir)
        if save_n_workers > 1:
            result_export.export_parallel(result_filename,
                                          save_n_workers,
                                          [(pms_fakes[i], [self.symbols[j] for j in range(n_securities)
                                            if strategy_symbol_mask[i][j]], self.strategy_params[i])
                                           for i in range(self.n_strategies)],
                                          flatten=self.config_loader.flatten)
        else:
            result_export.export(result_filename,
                                 *[(pms_fakes[i], [self.symbols[j] for j in range(n_securities)
                                    if strategy_symbol_mask[i][j]], self.strategy_params[i])
                                   for i in range(self.n_strategies)],
                                 flatten=self.config_loader.flatten)

        # Export order checker log
        log_name, _ = os.path.splitext(result_filename)
        logger = OrderLogger(result_dir)
        logger.dump_checker_log(f'{log_name}_order.log',
                                [pms_fakes[i].check_order_log for i in range(self.n_strategies)])

    def save_signal_remarks(self):
        remark_save_dir = self.config_loader.remark_save_dir
        remark_save_name = self.config_loader.remark_save_name
        if remark_save_dir is None or remark_save_name is None:
            return

        fname_main, fname_ext = os.path.splitext(remark_save_name)
        fname_ext = '.csv'
        if not os.path.exists(remark_save_dir):
            print('info: signal remark存储路径不存在，创建路径')
            os.makedirs(remark_save_dir)
        else:
            print("Warning: signal remark路径非空，新结果将覆盖原结果")
            remove_files_matching(remark_save_dir, f'^{fname_main}.*')
        user_save = UserSave(remark_save_dir)
        for sid, strategy in self.strategy_id_strategy_dict.items():
            filename = f'{fname_main}-{sid}{fname_ext}'
            user_save.save_signal_remark(filename, strategy.signal_remark)

    def export_results(self):
        # Save results
        self.export_pms_results()
        # Save signal remarks
        self.save_signal_remarks()
        # Save strategy status
        self.save_status()
