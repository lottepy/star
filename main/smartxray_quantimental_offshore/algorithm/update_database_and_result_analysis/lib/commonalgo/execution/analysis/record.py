# -*- coding: utf-8 -*-
# @Time    : 7/22/2019 2:16 PM
# @Author  : Joseph Chen
# @Email   : joseph.chen@magnumwm.com
# @FileName: record.py
# @Software: PyCharm
import os
from datetime import datetime
import pandas as pd
from log.get_logger import logger

class Record:
    """Record module to monitor and save DataFrames."""
    _data= {}

    @property
    def today(self):
        return datetime.today().strftime("%Y-%m-%d")

    def add(self, data:pd.DataFrame, name:str):
        assert isinstance(data, pd.DataFrame), "Data format is invalid. Only DataFrame is allowed."
        self._data[name] = data

    def save(self, name:str):
        if name in self._data.keys():
            directory = os.path.join(os.path.join(os.getcwd(), "analysis"), self.today)
            if not os.path.exists(directory):
                os.makedirs(directory)
            self._data[name].to_csv(f"{directory}/{name}.csv")
            logger.info(f"`{name}` has been saved successfully.")
        else:
            logger.error(f"`{name}` has not been added to record.")

    def save_all(self):
        for name in self._data.keys():
            self.save(name)

    def __str__(self):
        ret_str = "<Record>\n"
        for key, value in self._data.items():
            ret_str += f"\t{key}:\n"
            ret_str += "\t" + str(value.head()) + "\n"
        return ret_str
    __repr__ = __str__


