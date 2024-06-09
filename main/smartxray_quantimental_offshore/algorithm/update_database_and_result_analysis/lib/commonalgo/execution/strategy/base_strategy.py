# -*- coding:utf-8 -*-

import io
import time
from datetime import datetime
from functools import partial
import os
import glob
import numpy as np
import pandas as pd

from ..config.config_manager import ConfigManager
from ..config.path import PathManager
from ...data.oss_client import oss_client
from ..log.get_logger import get_logger
from ..market.market import market
from ..trade.trading import trading, trading_callback
from ..trading_executor import TradingExecutor
from ..util.target_positions import calculate_positions_from_weights_with_backup
from ..util.trading_utils import DATA_SRC_MAPPING, SYMBOL_PREFIX_MAPPING, get_min_price_series, get_lot_size_series

logger = get_logger(__name__)
configs = None  # 一个全局变量 共享运行参数给其他文件调用


class BaseStrategy:

    def __init__(self, data_dir: str, target_prefix='', target_suffix='', is_local=False):
        self.paths = PathManager(is_local)
        self.paths.init_paths(data_dir=data_dir, prefix=target_prefix, suffix=target_suffix)
        self.target_prefix = target_prefix
        self.target_suffix = target_suffix

        ####################################
        # 1. 从本地/远端读取csv
        ####################################
        target_backup = None
        if not is_local:
            logger.info('************ 远程csv文件 ************')
            while True:
                file_path = self.paths.path['target']
                try:
                    csv_bytes = oss_client.download_file_to_stream(file_path)
                    csv_file = io.BytesIO(csv_bytes)  # 把bytes转为一个类file object
                    target = pd.read_csv(csv_file)  # 无index 有标题  支持URL格式的路径
                    try:
                        csv_bytes_backup = oss_client.download_file_to_stream(self.paths.path['backup'])
                        csv_file_backup = io.BytesIO(csv_bytes_backup)  # 把bytes转为一个类file object
                        target_backup = pd.read_csv(csv_file_backup)  # 无index 有标题
                    except:
                        logger.info('no backup iuid list found')
                except:
                    logger.warning(f'target holdings file "{file_path}" not found, sleep for 10s then retry..')
                    time.sleep(10)
                else:
                    break
        else:
            logger.info('************ 本地csv文件 ************')
            target = pd.read_csv(self.paths.path['target'])
            try:
                target_backup = pd.read_csv(self.paths.path['backup'])
            except:
                logger.info('no backup iuid list found')

        self.configs = ConfigManager(self.paths.path['config_path'], is_local)  # 所有参数都保存在configs里面
        global configs
        configs = self.configs

        symbol_prefix = SYMBOL_PREFIX_MAPPING.get(self.configs.region)  # 会影响到iuid与不同symbol互转时用到的函数
        data_src = DATA_SRC_MAPPING.get(self.configs.region) # 主动拉取实时数据用到的数据源

        self.paper = {
            'broker': self.configs.broker,
            'account': self.configs.account
        }

        ####################################
        # 2. 实例化executor 查当前资产 由weights计算target_position
        ####################################
        # 尝试获取CN_00_CNY的add_value
        if not target[target['iuid'] == 'CN_00_CNY'].empty:
            add_value = target[target['iuid'] == 'CN_00_CNY']['target_position'].iloc[0]
            target = target[target['iuid'] != 'CN_00_CNY']  # 获取到之后去除这一行 避免后续影响
        else:
            add_value = 0
        self.target = target.set_index('iuid')
        target_backup_list = [] if target_backup is None else target_backup['iuid'].tolist()
        iuid_list = list(set(self.target.index).union(set(target_backup_list)))

        self.trading_exe = TradingExecutor(paper=self.paper,
                                           target_holding_iuids=iuid_list,
                                           region=self.configs.region,
                                           configs=self.configs,
                                           activate_choice=(self.configs.region in ('CN', 'HC', 'SG')),
                                           activate_rq_stream=self.configs.rq_stream_enabled,  # 是否使用rq client的实时行情
                                           activate_kuanrui=self.configs.kuanrui_enabled,
                                           is_subaccount=self.configs.is_subaccount,
                                           endpoint=self.configs.endpoint,
                                           order_callback=partial(trading_callback, source=self.configs.account))
        # considering_symbols包含用于获取实时数据所需的symbol  若有服务器推数据 则使用iuid
        self.considering_symbols = self.trading_exe.iuids if (self.configs.rq_stream_enabled or self.configs.kuanrui_enabled)\
                                     else getattr(self.trading_exe, f'{symbol_prefix}_symbols')
        self.considering_iuids = self.trading_exe.iuids

        # 统一调用的参数 调用时只需要传considering_symbols
        if self.configs.rq_stream_enabled:
            market.get_real_time_data_func = partial(market.get_real_time_data_from_rq_client, self.trading_exe.rq_client)
        elif self.configs.kuanrui_enabled:
            market.get_real_time_data_func = partial(market.get_real_time_data_from_socket_client, self.trading_exe.socket_client) 
        else:
            market.get_real_time_data_func = getattr(market, f'get_real_time_data_from_{data_src}')

        # minprice = get_min_price_series(considering_iuids)  # 当前策略其实没有用到minprice
        lot_size = get_lot_size_series(self.considering_iuids, rq_client=self.trading_exe.rq_client if hasattr(self.trading_exe, 'rq_client') else None)
        trading.lot_size = lot_size

        self.current_holdings = (self.trading_exe.init_holdings[['iuid', 'holdingPosition']]).copy()  # 原来的init_holdings有很多列 只取有关的列
        self.current_holdings.set_index('iuid', inplace=True)
        self.current_holdings = self.current_holdings.astype(int)
        logger.info(f"CURRENT holdings:\n{'='*50}\n{self.current_holdings.to_string()}\n")

        # 计算target_holdings 需要知道当前的资产 和各品种当前的价格
        current_cash_info = trading.get_current_cash_info(self.trading_exe.trading_ctrl, self.paper, self.trading_exe.currency, self.configs.is_subaccount)
        current_asset, current_cash = current_cash_info['asset'], current_cash_info['balance']
        current_asset += add_value
        current_asset = min(current_asset, self.configs.asset_limit)
        logger.debug('get realtime data to calc target positions..')
        real_time_data = market.get_real_time_data_func(self.considering_symbols)
        current_price = real_time_data['PRICE']  # 获取所需品种的当前价格 series

        nobuy = set((real_time_data[real_time_data.ASK_VOL_1 == 0.0]).index)
        nosell = set((real_time_data[real_time_data.BID_VOL_1 == 0.0]).index)
        notrade = nobuy.intersection(nosell)  # 停牌
        nobuy = nobuy - notrade  # 涨停
        nosell = nosell - notrade  # 跌停
        backup = [backup_iuid for backup_iuid in target_backup_list \
                              if (backup_iuid not in nobuy and backup_iuid not in nosell and backup_iuid not in notrade)]  # 不用差集以保证顺序不乱

        self.target_holdings = calculate_positions_from_weights_with_backup(current_asset, self.target,
                                    self.current_holdings['holdingPosition'],
                                    current_price, lot_size,
                                    strategy='min_drift', notrade=list(notrade), nobuy=list(nobuy), nosell=list(nosell), backup=backup)
        cash_remain = current_asset - sum((self.target_holdings * current_price).fillna(0))
        logger.debug('cash reamin:' + str(cash_remain))
        self.target_holdings = pd.DataFrame(self.target_holdings)
        logger.info('TARGET HOLDINGS:\n' + self.target_holdings.to_string())

        # 之前在变量lot_size中 期货的lot size其实是multiple 计算期货的手数之后 把期货的multiple改回真正的lot size 1
        for idx in lot_size.index:
            if '_60_' in idx:
                lot_size[idx] = 1

        self.split_asset = int(current_asset / self.configs.no_of_split) + 1
