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


@defaultjitclass({
    'ID_symbol_dict': types.DictType(types.unicode_type, int64_2d_array_type),
    'category_names': types.ListType(types.unicode_type),
    'select_strategy_ids': nb.int64[:],
    'own_static_init': nb.boolean
})
class Allocation_strategy(BaseStrategy):

    def init(self):
        n_security = self.ccy_matrix.shape[1]
        self.ID_symbol_dict = Dict.empty(nb.types.unicode_type, int64_2d_array_type)
        self.category_names = List.empty_list(nb.types.unicode_type)
        self.own_static_init = False # indicate the initialization of ID_symbol_dict & category_names

    def _load_info_dict(self):
        # 找到position对应的symbol(hardcore: 暂时认为每个ID的symbol数目一致)
        symbols = list(self.metadata['main']['symbols'])
        for strategy_name in self.category_names:
            n_id = len(self.id_mapping)
            n_symbol = len(self.metadata['0'][strategy_name])
            id_symbol_array = np.zeros((n_id, n_symbol), dtype=np.int64)
            for id in range(len(self.id_mapping)):
                id_str = self.metadata['masked_secondary_ids'][strategy_name][id]
                id_symbol_array[id] = [symbols.index(s) for s in self.metadata[id_str][strategy_name]]
            self.ID_symbol_dict[strategy_name] = id_symbol_array.copy()
    
    def save_status(self):
        '''可以把需要的status存进strategy_status
           不存任何状态会存一个空的'''
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status

    def load_status(self, pickle_dict):
        strategy_status = pickle_dict
        self.status = strategy_status

    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        # Print signal remark
        # print(self.get_remark(ref_data_dict['signal_remark'], 20))
        # print(self.metadata['remark']['fields'])

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
            self.category_names.extend(self.metadata['0'].keys())
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

        # Example: Forge a signal remark and save
        signal_remark = {'column_x': 0.01, 'column_y': 0.1, 'column_z': 1.0}
        self.save_signal_remark(signal_remark)

        ccp_order = ccp_target - ccp_current
        return ccp_order.reshape(1, -1)
