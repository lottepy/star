# -*- coding: utf-8 -*-

import os
import time

from ..log.get_logger import get_logger
logger = get_logger(__name__)


class PathManager:
    def __init__(self, local_mode=False):
        self.local_mode = local_mode
        self.path = {}

    def init_paths(self, app_dir=os.path.dirname(os.path.abspath(__file__)), config_path=None, data_dir=None,
                   prefix='', suffix=''):
        """
            确定executor用到的路径
            
            - app_dir: 远程模式时无效 本地模式默认为*本文件*所在的文件夹
            - config_path: config.ini文件的路径 其中有账号信息 endpoint等 有对应的默认值
            - data_dir: target csv所在的文件夹 远程模式时必填
            - prefix: target csv文件名的前缀
            - suffix: target csv文件名的后缀
        """
        if not data_dir:
            if self.local_mode:
                data_dir = os.path.join(app_dir, 'data')
            else:
                raise RuntimeError('Path for data is missing')

        if not config_path:
            if self.local_mode:
                config_path = os.path.join(app_dir, 'data', 'config.ini')  # 适配windows系统 使用os.path.join
            else:
                config_path = f'{data_dir}/config.ini'  # ini文件

        if self.local_mode:
            target_path = os.path.join(data_dir, f'{prefix}target{suffix}.csv')
            backup_path = os.path.join(data_dir, f'{prefix}backup{suffix}.csv')
        else:
            target_path = f'{data_dir}/{prefix}{time.strftime("%Y%m%d")}{suffix}.csv'  # e.g. stock_20190801_20.csv
            backup_path = f'{data_dir}/{prefix}{time.strftime("%Y%m%d")}{suffix}_backup.csv'  # e.g. stock_20190801_20_backup.csv

        self.data_dir = data_dir
        self.path['data_dir'] = data_dir
        self.path['config_path'] = config_path
        self.path['target'] = target_path
        self.path['backup'] = backup_path

        logger.debug(f'paths: {self.path}')
