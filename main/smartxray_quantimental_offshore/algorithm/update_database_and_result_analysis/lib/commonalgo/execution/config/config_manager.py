# -*- coding: utf-8 -*-

import configparser
import urllib

from ...data.oss_client import oss_client
from ..log.get_logger import get_logger

logger = get_logger(__name__)


class ConfigManager:

    configs = {}
    def __init__(self, config_file_path: str, is_local: bool):
        parser = configparser.ConfigParser()
        if is_local:
            parser.read(config_file_path)
        else:
            conf_string = oss_client.download_file_to_stream(config_file_path).decode('utf8')
            parser.read_string(conf_string)  # URL需要下载为字符串再读入

        # self.configs = {}

        self.configs['broker'] = parser['info']['broker']
        self.configs['account'] = parser['info']['account']
        self.configs['is_subaccount'] = parser.getboolean('info', 'is_subaccount', fallback=False)
        self.configs['region'] = parser['info']['region']
        self.configs['endpoint'] = parser['network']['endpoint']
        self.configs['rq_stream_enabled'] = parser.getboolean('order', 'rq_stream')
        self.configs['margin_enabled'] = parser.getboolean('order', 'margin', fallback=False)
        self.configs['kuanrui_enabled'] = parser.getboolean('order', 'kuanrui')
        if self.configs['kuanrui_enabled']:
            logger.info('kuanrui mode is True, and rq_client will be forced to False')
            self.configs['rq_stream_enabled'] = False
        self.configs['confirm'] = parser.getboolean('order', 'confirm', fallback=True)
        self.configs['cash_limit'] = parser.getfloat('order', 'cash_limit', fallback=-1)
        self.configs['timeout'] = parser.getfloat('order', 'timeout', fallback=60)
        self.configs['dingding_token'] = parser.get('order', 'dingding_token', fallback=None)
        self.configs['asset_limit'] = parser.getfloat('order', 'asset_limit', fallback=-1)
        if self.configs['asset_limit'] == -1:  # 如果是-1 则不限制
            self.configs['asset_limit'] = float('inf')
        self.configs['no_of_split'] = parser.getint('order', 'no_of_split', fallback=1)

        # 向前兼容，无相关配置则不读入
        if 'HOST'.lower() in list(parser['network'].keys()):  # .keys()方法返回小写 其实键名不分大小写
            self.configs['HOST'] = parser['network']['HOST']
        if 'PORT_OES'.lower() in list(parser['network'].keys()):
            self.configs['PORT_OES'] = int(parser['network']['PORT_OES'])
        if 'PORT_MDS'.lower() in list(parser['network'].keys()):
            self.configs['PORT_MDS'] = int(parser['network']['PORT_MDS'])

        logger.debug(f'configs: {self}')

    def __str__(self):
        return str(self.configs)

    def __getattr__(self, attr: str):  # 调用不存在的实例属性时
        return self.configs.get(attr)
