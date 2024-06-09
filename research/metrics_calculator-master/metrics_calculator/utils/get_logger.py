# -*- coding: utf-8 -*-
# 推荐用法：
# from lib.commonalgo.execution.log.get_logger import get_logger
# logger = get_logger(__name__)
import logging
import os
import time


def get_logger(logger_name='default logger', log_path='', format_string='', add_handler=True):
    """
        - `logger_name`建议按Python的包名来设置，可以捕获sub-library的log
        - `log_path`要求输入文件的路径（包括文件名），例如`logs/xxx.log`，默认存在get_logger.py的同一级
        - `format_string` 不填则有默认值，填写方法可参考 https://docs.python.org/3.6/library/logging.html#logrecord-attributes
        - `add_handler` 是否添加handler，若为主logger，不需要再次添加handler，否则每行log都会重复1次
    """
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    if add_handler and (not logger.hasHandlers()):
        if log_path:
            fh = logging.FileHandler(log_path, encoding='utf-8')
        else:
            current_file_abs_path = os.path.dirname(os.path.abspath(__file__))
            fh = logging.FileHandler(f'{current_file_abs_path}/{time.strftime("%Y%m%d")}.log', encoding='utf-8')
        fh.setLevel(logging.DEBUG)

        sh = logging.StreamHandler()
        sh.setLevel(logging.DEBUG)

        if format_string:
            formatter = logging.Formatter(format_string)
        else:
            formatter = logging.Formatter('%(asctime)s - [%(levelname)s] %(name)s - %(filename)s:%(lineno)d - %(message)s')

        fh.setFormatter(formatter)
        sh.setFormatter(formatter)

        logger.addHandler(fh)
        logger.addHandler(sh)

    return logger
