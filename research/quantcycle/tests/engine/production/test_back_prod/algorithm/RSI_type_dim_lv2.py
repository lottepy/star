import numpy as np
import numba as nb
from numba.typed import Dict, List
from numba import types
from numba.experimental import jitclass
from quantcycle.utils.indicator import RSI, Moving_series, moving_series_type
from quantcycle.utils.backtest_constant import Metrics, Rank_metrics
from quantcycle.utils.backtest_helper import second_sort
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager

try:
    pms_type = PorfolioManager.class_type.instance_type
except:
    pms_type = "PMS_type"
##===========================================================================##

int64_2d_array_type = nb.int64[:, :]
int64_1d_array_type = nb.int64[:]

@defaultjitclass({
    'portfolio_manager': pms_type,
    'ccy_matrix': nb.float64[:, :],
    'strategy_name': types.unicode_type,
    'strategy_param': nb.float64[:],
    'symbol_batch': types.ListType(types.unicode_type),
    'secondary_symbol_batch': types.DictType(types.unicode_type, types.ListType(types.ListType(types.unicode_type))),
    'ccy_list': types.ListType(types.unicode_type),
    'symbol_type': types.ListType(types.unicode_type),
    'status': types.DictType(types.unicode_type, nb.float64),
    'current_fx_data': nb.float64[:],
    'current_data': nb.float64[:],
    'is_tradable': nb.boolean[:],
    'ID_symbol_dict': types.DictType(types.unicode_type, int64_2d_array_type),
    'category_names': types.ListType(types.unicode_type),
    'select_strategy_ids': nb.int64[:],
    'signal_remark': types.DictType(nb.int64, types.DictType(types.unicode_type, nb.float64)),
    'timestamp': nb.int64,
    'select_strategy_ids': int64_1d_array_type
})
class Allocation_strategy(BaseStrategy):

    def init(self):
        n_security = self.ccy_matrix.shape[1]
        self.ID_symbol_dict = {}
        self.category_names = []
        self.own_static_init = False
    
    def _load_info_dict(self):
        # 找到position对应的symbol(hardcore: 暂时认为每个ID的symbol数目一致)
        symbols = list(self.metadata['main']['symbols'])
        for strategy_name in self.category_names:
            n_id = len(self.id_mapping)
            n_symbol = len(self.metadata[str(int(0))][strategy_name])
            id_symbol_array = np.zeros((n_id, n_symbol), dtype=np.int64)
            for id in range(len(self.id_mapping)):
                id_symbol_array[id] = [symbols.index(s) for s in self.metadata[str(int(id))][strategy_name]]
            self.ID_symbol_dict[strategy_name] = id_symbol_array.copy()
        self.select_strategy_ids = np.ones(1, dtype=np.int64)

    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        # Remark checkpoints begin
        remark = Dict.empty(types.unicode_type, nb.float64)
        current_time = time_dict['main'][-1, 0]
        #eps = 1e-5
        if current_time == 1547189820: # 2017年11月1日WednesdayAM12点00分 First time point
            # data_dict: main/fxrate/int & RSI_pnl/RSI_position
            # reference data: N/A
            if len(data_dict) == 7 and len(time_dict) == 7 and len(ref_data_dict) == 2 and len(ref_time_dict) == 2:
                pass
            else:
                return
            
            if data_dict['main'].shape == (2, 8, 4) and time_dict['main'].shape == (2, 9) and \
                """ data_dict['fxrate'].shape == (1, 8, 1) and time_dict['fxrate'].shape == (1, 9) """ and \
                """ data_dict['int'].shape == (1, 8, 1) and time_dict['int'].shape == (1, 9) """ and \
                data_dict['RSI_pnl'].shape == (254, 8, 1) and time_dict['RSI_pnl'].shape == (254, 9) and \
                data_dict['RSI_position'].shape == (1, 8, 1) and time_dict['RSI_position'].shape == (1,9):
                pass
            else:
                return
            remark = {"field_1":2,"field_2":2,"field_3":2,"field_4":2,"field_5":2}
        
        if current_time == 1547189820 + 3*24*60*60: # 2018年9月17日MondayAM12点00分 a random time point
            #testing static parameters in the strategy
            if self.strategy_name == 'Allocation_strategy' and \
                len(self.strategy_param) == 0 and \
                'main' in self.metadata.keys() and \
                sum(self.metadata['main']['symbols'] == ['000005.SZ', '000100.SZ', '000917.SZ', '002415.SZ', '600000.SH', '600016.SH', '600028.SH', '601398.SH']) == 8 and \
                len(self.metadata['main']['symbol_types']) == 8 and self.metadata['main']['symbol_types'][3] == 'CN_STOCK' and \
                self.metadata['main']['fields'] == ['open', 'high', 'low', 'close'] and \
                len(self.metadata['main']['ccy_list']) == 8 and self.metadata['main']['ccy_list'][2] == 'CNY' and \
                'ref_daily' in self.metadata.keys() and \
                self.metadata['ref_daily']['symbols'] ==  ['STOCK_1', 'STOCK_2', 'STOCK_3', 'STOCK_4', 'STOCK_5'] and \
                self.metadata['ref_daily']['fields'] ==  ['open', 'high', 'low', 'close'] and \
                'ref_hourly' in self.metadata.keys() and \
                self.metadata['ref_hourly']['symbols'] ==  ['STOCK_2', 'STOCK_3', 'STOCK_5'] and \
                self.metadata['ref_hourly']['fields'] ==  ['close'] and \
                'remark' in self.metadata.keys() and \
                len(self.metadata['remark']['fields']) == 5 and \
                '0' in self.metadata.keys() and 0 not in self.metadata.keys() and \
                self.metadata['0']['RSI'] == ['000005.SZ'] and\
                sum(self.ccy_matrix[0]) == 8:
                pass
            else:
                return
            remark = {"field_1":2,"field_2":2,"field_3":2,"field_4":2,"field_5":2}

        self.save_signal_remark(remark)
        # RSI strategy begins
        # TODO: 1. metrics name 不用constant，用可变形式
        #      2. threshold 用 strategy param传进来

        ###############################
        # RSI Hardcode
        short_pnl_threshold = 0.02
        short_sharp_ratio_threshold = 1.5
        short_hit_ratio_threshold = 0
        short_profit_rate_threshold = 0.51
        short_number_of_trade_threshold = 3
        # short_number_of_trade_threshold = 0         #! weaken
        short_average_drawdown_threshold = 0.004
        # short_average_drawdown_threshold = 0.04     #! weaken
        short_expected_return_threshold = -9999

        long_pnl_threshold = 0.04
        long_sharp_ratio_threshold = 1
        long_hit_ratio_threshold = 0
        long_profit_rate_threshold = 0.5
        long_number_of_trade_threshold = 9
        # long_number_of_trade_threshold = 0          #! weaken
        long_average_drawdown_threshold = 0.014
        # long_average_drawdown_threshold = 0.14      #! weaken
        long_expected_return_threshold = -9999

        rank_metrics = 0
        choose_ratio = 1
        correlation_threshold = 0.2
        ref_aum = 2900000 / 29
        ###############################

        if not self.own_static_init:
            self.category_names = list(self.metadata['0'].keys())
            self._load_info_dict()
            self.own_static_init = True

        category_name = self.category_names[0]
        pnl_name = category_name + '_pnl'
        position_name = category_name + '_position'
        short_metrics_name = category_name + '_metrics_61'
        long_metrics_name = category_name + '_metrics_252'
        n_security = self.ccy_matrix.shape[1]
        ccp_current = self.portfolio_manager.current_holding
        ccp_target = np.zeros(n_security)
        strategy_id_position = data_dict[position_name][-1]

        # 有12个月数据时，可以开始allocation
        if long_metrics_name in time_dict:
            # 更新12个月数据时，重新allocation
            if time_dict[long_metrics_name][-1, 0] == \
                    time_dict['main'][-1, 0]:
                all_strategy_short = data_dict[short_metrics_name][-1]
                all_strategy_long = data_dict[long_metrics_name][-1]

                expect_return_short = (all_strategy_short[:, Metrics.holding_period_return.value] /
                                       all_strategy_short[:, Metrics.number_of_trades.value])

                expect_return_long = (all_strategy_long[:, Metrics.holding_period_return.value] /
                                      all_strategy_long[:, Metrics.number_of_trades.value])

                expect_return_index = all_strategy_short.shape[1]
                id_index = all_strategy_short.shape[1] + 1

                all_strategy_short = np.concatenate((all_strategy_short, expect_return_short.reshape(-1, 1),
                                                     np.arange(all_strategy_short.shape[0]).astype(np.float64).reshape(
                                                         -1, 1)), axis=1)
                all_strategy_long = np.concatenate((all_strategy_long, expect_return_long.reshape(-1, 1),
                                                    np.arange(all_strategy_long.shape[0]).astype(np.float64).reshape(-1,
                                                                                                                     1)),
                                                   axis=1)
                selected_short = (all_strategy_short[:, Metrics.holding_period_return.value] > short_pnl_threshold) * \
                                 (all_strategy_short[:, Metrics.sharpe_ratio.value] > short_sharp_ratio_threshold) * \
                                 (all_strategy_short[:, Metrics.hit_ratio.value] > short_hit_ratio_threshold) * \
                                 (all_strategy_short[:, Metrics.number_of_trades.value] > short_number_of_trade_threshold) * \
                                 (all_strategy_short[:, Metrics.ADD.value] < short_average_drawdown_threshold) * \
                                 (all_strategy_short[:, Metrics.profit_rate.value] > short_profit_rate_threshold) * \
                                 (all_strategy_short[:, expect_return_index] > short_expected_return_threshold)
                selected_long = (all_strategy_long[:, Metrics.holding_period_return.value] > long_pnl_threshold) * \
                                (all_strategy_long[:, Metrics.sharpe_ratio.value] > long_sharp_ratio_threshold) * \
                                (all_strategy_long[:, Metrics.hit_ratio.value] > long_hit_ratio_threshold) * \
                                (all_strategy_long[:, Metrics.number_of_trades.value] > long_number_of_trade_threshold) * \
                                (all_strategy_long[:, Metrics.ADD.value] < long_average_drawdown_threshold) * \
                                (all_strategy_long[:, Metrics.profit_rate.value] > long_profit_rate_threshold) * \
                                (all_strategy_long[:, expect_return_index] > long_expected_return_threshold)

                all_strategy_short = all_strategy_short[selected_short * selected_long]
                if rank_metrics == Rank_metrics.Expected_Return.value:
                    all_strategy_short[:, expect_return_index] = np.round(all_strategy_short[:, expect_return_index], 8,
                                                                          all_strategy_short[:, expect_return_index])
                    all_strategy_short = all_strategy_short[np.argsort(all_strategy_short[:, expect_return_index])]
                    fix_columns = np.array([expect_return_index]).astype(np.int64)
                    all_strategy_short = second_sort(all_strategy_short, fix_columns, id_index)
                else:
                    all_strategy_short[:, Metrics.holding_period_return.value] = np.round(
                        all_strategy_short[:, Metrics.holding_period_return.value], 8,
                        all_strategy_short[:, Metrics.holding_period_return.value])
                    all_strategy_short = all_strategy_short[
                        np.argsort(all_strategy_short[:, Metrics.holding_period_return.value])]
                    fix_columns = np.array([Metrics.holding_period_return.value]).astype(np.int64)
                    all_strategy_short = second_sort(all_strategy_short, fix_columns, id_index)
                remaining_strategy_id = np.flip(all_strategy_short[:, id_index])
                remaining_strategy_id = remaining_strategy_id.astype(np.int64)
                pnl_array = data_dict[pnl_name][-254:-1, remaining_strategy_id, 0]
                rtn_array = ((pnl_array[1:] - pnl_array[:-1]) / ref_aum)
                corr_matrix = np.corrcoef(rtn_array, rowvar=False)

                select_strategy = (remaining_strategy_id != remaining_strategy_id)
                for i in range(int(all_strategy_short.shape[0] * choose_ratio)):
                    if i == 0:
                        select_strategy[i] = True
                    else:
                        corr_check = corr_matrix[i]
                        corr_check = corr_check[select_strategy]
                        if np.all(corr_check < correlation_threshold):
                            select_strategy[i] = True
                self.select_strategy_ids = remaining_strategy_id[select_strategy]
            # 跟据选择的子策略下单
            # equal weight to this strategy
            n_select_id = len(self.select_strategy_ids)
            for strategy_id in self.select_strategy_ids:
                strategy_id_symbols = self.ID_symbol_dict[category_name][strategy_id]
                ccp_target[strategy_id_symbols] += strategy_id_position[strategy_id]
            if n_select_id != 0:
                ccp_target = ccp_target / n_select_id

        ccp_order = ccp_target - ccp_current
        return ccp_order.reshape(1, -1)
