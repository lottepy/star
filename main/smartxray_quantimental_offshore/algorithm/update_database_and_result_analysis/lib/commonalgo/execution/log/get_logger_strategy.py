# -*- coding: utf-8 -*-
# @Time    : 7/22/2019 2:50 PM
# @Author  : Joseph Chen
# @Email   : joseph.chen@magnumwm.com
# @FileName: get_logger.py
# @Software: PyCharm
import logging
import os
import time

logger = logging.getLogger("execution_strategy")
logger.setLevel(logging.DEBUG)

current_file_abs_path = os.path.dirname(os.path.abspath(__file__))

fh = logging.FileHandler(f'{current_file_abs_path}/executor_strategy{time.strftime("%Y%m%d")}.log', encoding='utf-8')
fh.setLevel(logging.DEBUG)

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(filename)s:%(lineno)d - %(name)s - %(levelname)s - %(process)d - %(threadName)s: %(message)s')
fh.setFormatter(formatter)
sh.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(sh)
