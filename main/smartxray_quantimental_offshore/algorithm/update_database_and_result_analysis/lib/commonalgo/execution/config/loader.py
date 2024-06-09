#!/usr/bin/env python
import pandas as pd
import numpy as np
import configparser

from ..util.trading_utils import reconnect
from ..log.get_logger import logger
from ..config import constant


def target_file_validation(target, asset: float, price: pd.Series,
                           leverage_limit=0,):
    """
    Parameters:
    target (pd.Dataframe):  index - 'iuid'
    asset (float): total asset
    leverage_limit: margin account, 0~1, =0 means no margin
    """
    target = target.copy()
    if target.index.name != 'iuid':
        if 'iuid' not in target.columns or target['iuid'].isnull().any():
            raise ValueError("IUID missing in Target File")
        target = target.set_index('iuid')

    target, add_value = target_get_del_add_value(target)
    asset += add_value

    target.fillna(0, inplace=True)
    position_price = target['target_position'] * price
    total_weight = np.sum(target['target_weight']) + np.sum(position_price)/asset
    leverage_limit = max(0, leverage_limit)
    if total_weight > 1+leverage_limit:
        raise ValueError(f"Target Faile weight {total_weight} greater than 1+{leverage_limit}")
    return True


def target_file_symbol2iuid():
    raise NotImplemented("So far we do not accept symbol input.")


def target_get_del_add_value(target: pd.DataFrame):
    """ Get cash amount (if any) from target and remove the line
    """
    assert target.index.name == 'iuid'
    add_value = 0
    if constant.CASH_IUID in target.index:
        add_value = target.loc[constant.CASH_IUID, 'target_position']
        target = target.drop(constant.CASH_IUID, errors='ignore')

    return  target, add_value


@reconnect
def load_conf_target(paths):
    if not paths.local_mode:
        logger.info('************ 远程配置文件 ************')
        file_path = paths.path['target']
        # target_holdings = pd.read_csv(file_path, index_col=0, header=None)  # 旧输入格式
        # convert symbol to iuid if target has symbol input but not iuid
        target = pd.read_csv(file_path, index_col=0)  # 无index 有标题  支持URL格式的路径
    else:
        logger.info('************ 本地配置文件 ************')
        target = pd.read_csv(paths.path['target'], index_col=0)

    conf = configparser.ConfigParser()
    if paths.local_mode:
        conf.read(paths.path['config_path'])
    else:
        conf_string = urllib.request.urlopen(paths.path['config_path']).read().decode('utf8')
        conf.read_string(conf_string)  # URL需要下载为字符串再读入
    logger.info(f"broker: {conf['info']['broker']}, account: {conf['info']['account']}, endpoint: {conf['network']['endpoint']}")
    return conf, target
