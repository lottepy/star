import csv
import os
from datetime import datetime

import numpy as np
import pandas as pd

from .db_connector import DatabaseConnector
from .portfolio_record import PortfolioRecord
from .strategy_id_generator import strategy_id_generator
from .trade_record import TradeRecord

class Observer:
    # observer类用于控制回测框架1的结果收集和回测框架2的数据提供
    def __init__(self, universe_set_id=0, save_path="result", file_name="result",
                 params=None, is_export_csv=False):
        self.universe_set_id = universe_set_id
        if params is None:
            params = []
        self.params = params  # 用于在level1存放大量的参数
        self.save_path = save_path  # 保存和读取结果的路径
        self.file_name = file_name  # 保存和读取结果的文件名称
        self.portfolio_record_list = []  # 类Portfolio_Record的list

        # timestamp of every sample (list) --example: [1765165165.0,1765165168.0,]
        self.timestamp_list = []

        self.is_export_csv = is_export_csv

    def get_portfolio_record_by_vid(self, vid) -> PortfolioRecord:
        db_path = self.get_database_path()
        db_connector = DatabaseConnector(file_path=db_path)
        portfolio_record = db_connector.read_record_by_vid(vid)
        return portfolio_record

    def get_database_path(self):
        return os.path.join(self.save_path, f"{self.file_name}_u_{self.universe_set_id}.db")

    def export_record_to_csv(self, record: PortfolioRecord):
        record_param = {}
        for name, param in zip(self.params, record.strategy_param):
            record_param[name] = param
        record.strategy_param = record_param
        opt_dict = {'strategy_id': record.strategy_id}
        opt_dict.update(record.strategy_param)
        df = pd.DataFrame(record.timestamp_list, columns=["datetime"])
        df1 = pd.DataFrame(record.pv_list, columns=record.universe)
        df2 = pd.DataFrame(record.position_list, columns=record.universe)
        df['datetime'] = list(map(datetime.fromtimestamp, df['datetime']))
        df1['pv'] = np.sum(df1.values, axis=1)
        df = df.join(df1)
        df = df.join(df2, lsuffix='_pv', rsuffix='_position')
        opt_dict['pv'] = df1.iloc[-1]['pv']
        df.to_csv(os.path.join(self.save_path,
                               f"{self.file_name}_u_{self.universe_set_id}_strategy_id_{record.strategy_id}.csv"))
        del df

    def export_opt_to_csv(self, record: PortfolioRecord):
        pv = sum(record.simple_pv)
        csv_row = [record.strategy_id-1, record.strategy_id]
        csv_row.extend(record.strategy_param)
        csv_row.append(pv)
        csv_path = os.path.join(
            self.save_path, f"{self.file_name}_u_{self.universe_set_id}_opt.csv")
        with open(csv_path, 'a', newline='')as f:
            f_csv = csv.writer(f)
            f_csv.writerow(csv_row)

    def read_from_sqlite(self, virtual=False):
        db_path = self.get_database_path()
        db_connector = DatabaseConnector(file_path=db_path)
        if not virtual:
            self.portfolio_record_list = db_connector.read_record_list()
        else:
            self.portfolio_record_list = db_connector.read_record_list_virtual()
        del db_connector

    def create_strategy_mapping(self, flatten=False):
        db_path = self.get_database_path()
        db_connector = DatabaseConnector(file_path=db_path)
        db_connector.drop_tables(['strategy_mapping'])
        db_connector.create_strategy_mapping(flatten=flatten)
