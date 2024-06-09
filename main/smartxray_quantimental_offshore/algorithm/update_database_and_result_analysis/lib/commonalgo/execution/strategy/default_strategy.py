# -*- coding:utf-8 -*-
# 应被外部调用 不直接执行此文件

import io
import time
from datetime import datetime
from functools import partial
from pprint import pformat

import numpy as np
import pandas as pd

from .base_strategy import BaseStrategy
from ..log.get_logger import get_logger
from ..market.market import market
from ..trade.trading import trading
from ..util.trading_utils import signals_to_orders
from ..util.dingding import send_dingding_msg

logger = get_logger(__name__)


class DefaultStrategy(BaseStrategy):

    def __init__(self, data_dir: str, target_prefix='', target_suffix='', is_local=False):
        super().__init__(data_dir, target_prefix, target_suffix, is_local)

    def go(self):
        try:
            ####################################
            # 取消当前订单 处理current/target_holdings 计算待买卖的品种与数目
            ####################################
            if not self.configs.is_subaccount:
                self.trading_exe.trading_ctrl.sync_holdings(self.paper['broker'], self.paper['account'])  # cancel前先同步
            trading.cancel_all_active_orders(self.trading_exe.trading_ctrl, self.paper)

            # 处理 current_holdings 与 target_holdings, 使它们的index一致
            for iuid in self.considering_iuids:
                if iuid not in self.current_holdings.index:
                    self.current_holdings.loc[iuid] = 0
                if iuid not in self.target_holdings.index:
                    self.target_holdings.loc[iuid] = 0

            # 做多为正 做空为负 若多转空或空转多 则sign==2 需先平仓再进行后续操作
            sign = abs(np.sign(self.target_holdings) - np.sign(self.current_holdings))
            target_holdings_mid = self.target_holdings.copy()  # 对于多转空或空转多而言的 中间target holding
            target_holdings_mid[sign == 2] = 0
            diff1 = target_holdings_mid - self.current_holdings  # 第一次的操作变化量
            diff2 = self.target_holdings - target_holdings_mid  # 第二次的操作变化量

            ####################################
            # 4. 获取当前的价格 最后下单
            ####################################
            logger.debug('get realtime data for ordering..')
            real_time_data = market.get_real_time_data_func(self.considering_symbols)
            logger.debug('\n' + real_time_data.to_string())

            buyprice = real_time_data['BID_1'].astype('float64')  # series 数据类型原来是Object 需要改为float
            sellprice = real_time_data['ASK_1'].astype('float64')
            # minprice = minprice[considering_iuids]

            # buyprice = np.round(buyprice / minprice) * minprice  # 取最小价格单位的整数倍 其实当前这个策略不需要minprice
            # sellprice = np.round(sellprice / minprice) * minprice

            orders, orders_sell, orders_buy = signals_to_orders(diff1, buyprice_data=buyprice, sellprice_data=sellprice, source=self.configs.account)
            # orders, orders_sell, orders_buy = signals_to_orders(diff1, buyprice_data=sellprice, sellprice_data=buyprice)  # 成本更低 但成交更慢

            if self.configs.confirm:
                input('\nORDERS TO SUBMIT:\n' + pformat(orders) + '\npress enter to confirm\n')

            order_results = pd.DataFrame()
            if orders:  # 如果有变动 需要第一次操作
                if self.configs.margin_enabled:
                    order_results1 = trading.execute_orders(orders, self.trading_exe.trading_ctrl, self.paper)
                else:
                    order_results1 = trading.execute_sellbuy_cont(orders_sell, orders_buy, self.split_asset, self.trading_exe.trading_ctrl, self.paper)
                logger.info(order_results1.to_string())
                order_results = order_results.append(order_results1)

            # 如果出现多转空或空转多 需要进一步操作
            # 否则第一轮就可以操作完毕
            # 若账户不支持卖空 则不可能执行到这里面
            if diff2.any(axis=0)[0]:  # 如果diff2有不为0的项 .any返回长度为1的series
                logger.info('Begin second round order submission')
                if not self.configs.is_subaccount:
                    self.trading_exe.trading_ctrl.sync_holdings(self.paper['broker'], self.paper['account'])  # 第二轮下单前先同步
                logger.debug('get realtime data for ordering..')
                real_time_data = market.get_real_time_data_func(self.considering_symbols)

                buyprice = real_time_data['BID_1'].astype('float64')
                sellprice = real_time_data['ASK_1'].astype('float64')
                # buyprice = np.round(buyprice / minprice) * minprice  # 取最小价格单位的整数倍
                # sellprice = np.round(sellprice / minprice) * minprice

                orders, orders_sell, orders_buy = signals_to_orders(diff2, buyprice_data=buyprice, sellprice_data=sellprice)
                assert self.configs.margin_enabled, 'Must be margin account if diff2 exists'
                order_results2 = trading.execute_orders(orders, self.trading_exe.trading_ctrl, self.paper)
                logger.info(order_results2.to_string())
                order_results = order_results.append(order_results2)

            order_results.to_csv(f'order_results_{time.strftime("%Y%m%d%H%M")}.csv')

            time.sleep(1)
            if not self.configs.is_subaccount:
                self.trading_exe.trading_ctrl.sync_holdings(self.paper['broker'], self.paper['account'])  # 查持仓前先同步
            positions_new = trading.get_current_positions(self.trading_exe.trading_ctrl, self.paper, self.trading_exe.region, self.configs.is_subaccount)
            positions_new.to_csv(f'{self.configs.account}_after_trading_positions_{time.strftime("%Y%m%d%H%M")}.csv')

            logger.info('FINISHED!')

            if len(order_results) > 0 and self.configs.dingding_token:
                msg = f"{self.paper['account']}\n"
                for i in range(len(order_results)):
                    row = order_results.iloc[i]
                    direction = '买入' if row['direction'] == 'BUY' else '卖出'
                    unit = '手' if '_60_' in row['iuid'] else '股'
                    msg += f"{direction}{int(row['quantity'])}{unit}{row['iuid']}，价格{row['price']}\n"
                msg += f"\n当前持仓：\n{(positions_new[['iuid', 'holdingPosition']]).to_dict('records')}"
                send_dingding_msg(self.configs.dingding_token, msg)

        except BaseException as e:
            logger.error('Error, please check, the program will exit! Error message:', exc_info=True)
            if self.configs.dingding_token:
                send_dingding_msg(self.configs.dingding_token, str(e))
            raise
        finally:
            if hasattr(self, 'trading_exe'):
                if hasattr(self.trading_exe, 'rq_client'):
                    self.trading_exe.rq_client.disconnect_client()  # 最后记得关闭, 否则后台线程关不掉
                if hasattr(self.trading_exe, 'socket_client'):
                    self.trading_exe.socket_client.disconnect_client()  # 最后记得关闭, 否则后台线程关不掉
