#!/usr/bin/env python

# -*- coding:utf-8 -*-

import argparse
import time
from datetime import datetime

import numpy as np
import pandas as pd

from .base import TradingExecutor
from ..config.path import PathManager
from ..config import loader, constant
from ..log import get_logger
from ..symbol.base import symbol_converter, Symbols
from ..trade.trading import trading
from ..trade.trading_controller import TradingController
from ..util.target_positions import calculate_positions_from_weights
from ..util import trading_utils
from ..util.trading_utils import signals_to_orders


class TWAPExecutor(TradingExecutor):
    def __init__(self, paper: dict, region: str, target, *args,   **kwargs ):
        super().__init__(paper, region, *args,
                         target_holding_iuids = list(target.index), **kwargs)

    def _preprocessing(self):
        pass

    @staticmethod
    def get_current_asset(trading_ctrl, paper, currency, local=False):
        logger = get_logger.logger
        try:
            retry_time = 5
            for i in range(5):
                try:
                    current_asset = trading.get_current_cash(trading_ctrl, paper,
                                                            currency)
                    return current_asset
                except Exception as e:
                    logger.debug(f"Aquiring asset {i}: {e}")
                    time.sleep(1)
                    if i  == retry_time - 1:
                        raise e

        except Exception as e:
            if local:
                test_asset = 500000
                confirm = input(f"Read current asset error:{e}, "
                                f"press Y to set as {test_asset},"
                                f"input number to set value "
                                f"otherwise will terminate")
                if confirm in {'y', 'Y'}:
                    current_asset = 500000
                else:
                    try:
                        current_asset = float(confirm)
                    except ValueError:
                        raise e
            else:
                raise e
        return current_asset

    @staticmethod
    def cal_cash(asset, holdings, price):
        # TODO: test
        for iuid, quantity in holdings.items():
            if Symbols.is_future_iuid(iuid):
                continue
            asset -= price[iuid]*holdings
        return asset

    @staticmethod
    def get_flip_orders(positions):
        """ Assets from long to short or short to long
        """
        current_position, target_position = positions['current_position'], positions['target_position']
        mask = (abs(np.sign(target_position) - np.sign(current_position))) == 2
        if mask.any():
            # 对于多转空或空转多而言的 中间target holding
            target_position_mid = target_position.copy()
            target_position_mid[mask] = 0
            diff1 = target_position_mid - current_position  # 第一次的操作变化量
            diff2 = target_position - target_position_mid  # 第二次的操作变化量
        else:
            diff1, diff2 = target_position, None

        diff1 = diff1[diff1!=0]
        diff2 = diff2[diff2!=0] if not diff2 is None else diff2

        return diff1, diff2

    @staticmethod
    def filter_orders(positions: pd.DataFrame, current_price,
                      trading_direction, threshold):
        logger = get_logger.logger
        diff = np.abs(positions['target_position']-positions['current_position'])
        mask = (trading_direction * diff) < 0

        if mask.any():
            logger.warning(f"{diff.index[mask].values} controdict the previous"
                           "trading directions")
            diff = diff[~mask]
            positions = positions[~mask]

        current_price = current_price[diff.index]
        values = diff * current_price
        positions = positions[values > threshold]
        return positions

    @staticmethod
    def create_orders(quantities, prices):
        orders = {}
        for iuid, quant in quantities.items():
            if quant > 0:
                price = prices.loc[iuid, 'BID_1']
            else:
                price = prices.loc[iuid, 'ASK_1']
            orders[iuid] = Order(iuid, np.abs(quantity), price,
                                 np.sign(quantity))
        return orders

    @staticmethod
    def get_min_price_lot_size_from_ready_info(ready_info):
        multiple_main = pd.Series(ready_info['multiple'].values, index=ready_info['main'].values)
        multiple_main_past = pd.Series(ready_info['multiple'].values, index=ready_info['main_past'].values)
        multiple = multiple_main.append(multiple_main_past)  # 当前主力合约与旧主力合约的和
        multiple = multiple[~multiple.index.duplicated(keep='first')]  # 索引去重
        multiple = multiple.rename(index=getattr(symbol_converter, f'from_{symbol_prefix}symbol_to_iuid'))  # 把索引转为iuid

        ################### TODO minprice 重写
        minprice_main = pd.Series(ready_info['minprice'].values, index=ready_info['main'].values)
        minprice_main_past = pd.Series(ready_info['minprice'].values, index=ready_info['main_past'].values)
        minprice = minprice_main.append(minprice_main_past)  # 当前主力合约与旧主力合约的和
        minprice = minprice[~minprice.index.duplicated(keep='first')]  # 索引去重
        minprice = minprice.rename(index=getattr(symbol_converter, f'from_{symbol_prefix}symbol_to_iuid'))  # 把索引转为iuid
        return minprice, multiple


    @classmethod
    def execute(cls, local=True):
        paths = PathManager(local)
        paths.init_paths()
        ####################################
        # 1. 从本地/远端读取配置
        ####################################
        conf, target = loader.load_conf_target(paths)
        logger = get_logger.logger

        min_order_value = int(conf['args']['min_order_value'])
        leverage_limit = float(conf['args']['leverage_limit'])
        region = conf['info']['region']
        activate_rq = (conf['order']['rq'] in constant.true_str)
        # symbol_prefix_mapping = {'CN': 'choice', 'HK': 'choice', 'US': ''}  # 不同区域用到的symbol类型 HK/US为普通symbol
        symbol_prefix = constant.symbol_prefix_mapping[region]  # 会影响到iuid与不同symbol互转时用到的函数
        data_src_mapping = constant.region_data_src_map
        data_src = data_src_mapping[region]  # 会影响到获取实时数据用到的数据源

        # ready info
        # ready_info = pd.read_csv(paths.path['ready'], index_col=0)
        paper = {
            'broker': conf['info']['broker'],
            'account': conf['info']['account']
        }

        trading_ctrl = TradingController(endpoint=conf['network']['endpoint'])
        # 从weight转换为holding需要先获取当前资产
        current_asset = cls.get_current_asset(trading_ctrl, paper,
                                              constant.region_currency_map[region],
                                              local)

        # 尝试获取CN_00_CNY的add_value
        target, add_value = loader.target_get_del_add_value(target)
        current_asset += add_value

        ####################################
        # 2. 实例化executor 查当前资产 由weights计算target_position
        ####################################
        with cls(paper=paper,
                 region=region,
                 target = target,
                 activate_futu=False,  # HK/US/CN
                 activate_factset=False,
                 activate_choice=(region == 'CN','HK'),
                 activate_msq=activate_rq,  # HK/US/CN
                 disable_callback=False,
                 endpoint=conf['network']['endpoint'],) as trading_exe:

            iuids = trading_exe.iuids
            considering_iuids = iuids
            considering_symbols = trading_exe.symbols

            lot_size = trading_utils.get_lot_size_series(iuids)

            # sync_holdings

            sync_result = trading_exe.trading_ctrl.sync_holdings(
                paper['broker'], paper['account'])  # cancel前先同步

            # current_price = getattr(market, f'get_real_time_data_from_{data_src}')(considering_symbols)['PRICE']
            real_time_data = trading_exe.get_real_time_data()
            current_price = real_time_data['PRICE']  # 获取所需品种的当前价格 series

            # pd.Series
            current_holdings = trading_exe.init_holdings.set_index('iuid')['holdingPosition'].astype(int)
            current_holdings.rename('current_position', inplace=True)
            logger.info(f"\ncurrent holdings:\n{'='*50}\n{current_holdings}\n")

            loader.target_file_validation(target, current_asset, current_price,
                                          leverage_limit)

            target_holdings = calculate_positions_from_weights(current_asset, target,
                                                                current_holdings,
                                                                current_price,
                                                                lot_size, strategy='min_drift')
            target_holdings.rename('target_position', inplace=True)

            logger.info('TARGET HOLDINGS:\n' + str(target_holdings))


            trading.cancel_all_active_orders(trading_ctrl, paper)

            import ipdb; ipdb.set_trace()
            # 处理 current_holdings 与 target_holdings, 使它们的index一致
            positions = pd.concat([ current_holdings.astype(int), target_holdings,], axis=1)
            positions.fillna(0, inplace=True)

            positions = cls.filter_orders(positions, current_price,
                                          trading_exe.trading_directions,
                                          min_order_value)



            # 包含 (current holding+target holding) 的symbol
            # minprice = trading_utils.get_min_price_series(considering_iuids)

            ####################################
            # 3. 由current/target_holdings计算待买卖的标的与数目
            #    获取当前的价格 最后下单
            ####################################

            diff1, diff2 = cls.get_flip_orders(positions)
            cash = cls.cal_cash(current_asset, current_holdings, current_price)
            trading_exe.order_manager.set_cash(cash)

            if not diff1 is None and len(diff1) > 0: # 如果有变动 需要第一次操作
                # real_time_data = trading_exe.get_real_time_data()
                # # series 数据类型原来是Object 需要改为float
                # buyprice = real_time_data['BID_1'].astype('float64')
                # sellprice = real_time_data['ASK_1'].astype('float64')

                # orders, orders_sell, orders_buy = signals_to_orders(
                #     diff1, buyprice_data=buyprice, sellprice_data=sellprice)
                # # orders = cls.create_orders(diff1, real_time_data)
                # trading_exe.update_direction(orders)

                # order_results1 = trading.execute_orders(orders, trading_ctrl, paper)
                # logger.info(order_results1)
                trading_exe.order_manager.create_orders(diff1)

            # 如果出现多转空或空转多 需要进一步操作
            # 否则第一轮就可以操作完毕
            if not diff2 is None and len(diff2) > 0:
                logger.info('Begin second round order submission')
                real_time_data = trading_exe.get_real_time_data()

                buyprice = real_time_data['BID_1'].astype('float64')
                sellprice = real_time_data['ASK_1'].astype('float64')

                orders, orders_sell, orders_buy = signals_to_orders(diff2, buyprice_data=buyprice, sellprice_data=sellprice)
                order_results2 = trading.execute_orders(orders, trading_ctrl, paper)
                logger.info(order_results2)

            time.sleep(30)
            positions = trading.get_current_holdings()
            positions.to_csv("after_trading_positions.csv")

            logger.info('orders done in main at {}'.format(datetime.now()))




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--local', action='store_true', default=True, help='read setting files from local path')
    args = parser.parse_args()
