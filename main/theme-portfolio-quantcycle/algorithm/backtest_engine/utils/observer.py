import csv
import gzip
import os
from datetime import datetime

import numpy as np
import pandas as pd
from tqdm import tqdm

from .constants import TimeFreq
from .db_connector import DatabaseConnector
from .get_logger import get_logger
from .portfolio_record import PortfolioRecord
from .strategy_id_generator import strategy_id_generator
from .trade_record import TradeRecord


class Observer:
    # observer类用于控制回测框架1的结果收集和回测框架2的数据提供
    def __init__(self, collect_freq_mode=TimeFreq.MINLY, universe_set_id=0, save_path="result", file_name="result",
                 params=None, is_export_csv=False):
        self.universe_set_id = universe_set_id
        if params is None:
            params = []
        self.params = params  # 用于在level1存放大量的参数
        self.save_path = save_path  # 保存和读取结果的路径
        self.file_name = file_name  # 保存和读取结果的文件名称
        self.collect_freq_mode = collect_freq_mode  # level1收集类Portfolio_Record数据的频率
        self.portfolio_record_list = []  # 类Portfolio_Record的list

        self.timestamp_list = []  # timestamp of every sample (list) --example: [1765165165.0,1765165168.0,]

        self.is_export_csv = is_export_csv
        self.logger = get_logger('backtest_engine.utils.observer', log_path='backtest_engine.log')
        if not os.path.isdir(self.save_path):
            os.mkdir(self.save_path)

    def get_portfolio_record_by_strategy_id(self, strategy_id) -> PortfolioRecord:
        db_path = self.get_database_path()
        db_connector = DatabaseConnector(file_path=db_path)
        portfolio_record = db_connector.read_record_by_strategy_id(strategy_id)
        return portfolio_record

    def get_database_path(self):
        return os.path.join(self.save_path, f"{self.file_name}_u_{self.universe_set_id}.db")

    def collect_record_from_backtest(self, portfolio_record: PortfolioRecord):
        trade_record = portfolio_record.trade_record

        portfolio_record.array_to_list()
        trade_record.array_to_list()

        # 把nint64变成int
        trade_record.timestamp_list = [int(x) for x in trade_record.timestamp_list]
        self.timestamp_list = [int(x) for x in self.timestamp_list]
        portfolio_record.signal_remark['signal_time_list'] = [int(x) for x in portfolio_record.signal_remark['signal_time_list']]

        portfolio_record.signal_remark['signal_remark_list'] = [dict(np_dict) for np_dict in portfolio_record.signal_remark['signal_remark_list']]

        self.portfolio_record_list.append(portfolio_record)

    def save_to_sqlite(self, many=False):
        db_path = self.get_database_path()
        db_connector = DatabaseConnector(file_path=db_path)
        db_connector.insert_config_info(self.params, self.timestamp_list)
        if many:
            strategy_id_list = db_connector.insert_record_list_many(self.portfolio_record_list)
        else:
            strategy_id_list = db_connector.insert_record_list(self.portfolio_record_list)
        if self.is_export_csv:
            for index in range(len(self.portfolio_record_list)):
                record = self.portfolio_record_list[index]
                record.strategy_id = strategy_id_list[index]
                record.timestamp_list = self.timestamp_list
                self.export_opt_to_csv(record)
                self.export_record_to_csv(record)
        del db_connector

    def zip_db(self):
        db_path = self.get_database_path()
        f_src = open(db_path, "rb")  # 打开文件
        f_tar = gzip.open(db_path + ".gz", "wb")  # 创建压缩文件对象
        f_tar.writelines(f_src)
        f_tar.close()
        f_src.close()
        # f = zipfile.ZipFile(db_path, 'w', zipfile.ZIP_DEFLATED)
        # f.write(filename, file_url)
        # f.close()

    def clean_save_data(self, is_drop_table=False, strategy_param=None, soft_mode=False):
        if is_drop_table:
            db_path = self.get_database_path()
            db_connector = DatabaseConnector(file_path=db_path)
            if soft_mode:
                db_connector.drop_tables()
            else:
                db_connector.drop_database()
            db_connector.init_tables()
            del db_connector
        if self.is_export_csv:
            headers = [None, 'strategy_id']
            if strategy_param is not None:
                headers.extend(strategy_param)
            headers.append("pv")
            csv_path = os.path.join(self.save_path, f"{self.file_name}_u_{self.universe_set_id}_opt.csv")
            with open(csv_path, 'w', newline='')as f:
                f_csv = csv.writer(f)
                f_csv.writerow(headers)

    # def save_record_to_sqlite(self, record: PortfolioRecord):
    #     # self.logger.debug("save level1 result into sqlite...")
    #     db_path = self.get_database_path()
    #     db_connector = DatabaseConnector(file_path=db_path)
    #     self.timestamp_list = [int(x) for x in self.timestamp_list]
    #     db_connector.insert_config_info(self.params, self.timestamp_list)
    #     db_connector.insert_record(record)
    #     del db_connector
    #     # self.logger.debug("sqlite data saved!")

    def export_record_to_csv(self, record: PortfolioRecord):
        record_param = {}
        for name, param in zip(self.params, record.strategy_param):
            record_param[name] = param
        record.strategy_param = record_param
        # opt_dict = {'strategy_id': record.strategy_id}
        # opt_dict.update(record.strategy_param)
        df = pd.DataFrame(record.timestamp_list, columns=["datetime"])
        df['timestamp'] = df["datetime"]
        df1 = pd.DataFrame(record.pv_list, columns=list(map(lambda x: f"{x}_pv", record.universe)))
        df2 = pd.DataFrame(record.cost_list, columns=list(map(lambda x: f"{x}_cost", record.universe)))
        df3 = pd.DataFrame(record.position_list, columns=list(map(lambda x: f"{x}_position", record.universe)))
        df['datetime'] = list(map(datetime.utcfromtimestamp, df['datetime']))
        df1['pv'] = np.sum(df1.values, axis=1)
        df = df.join(df1)
        df = df.join(df2)
        df = df.join(df3)

        if record.signal_remark:
            df4 = pd.DataFrame(record.signal_remark["signal_remark_list"])
            df4 = df4.add_suffix('_remark')
            df4['timestamp'] = record.signal_remark["signal_time_list"]
            df = pd.merge(df, df4, on='timestamp', how='outer')
        # df['timestamp']

        # opt_dict['pv'] = df1.iloc[-1]['pv']
        df.to_csv(os.path.join(self.save_path,
                               f"{self.file_name}_u_{self.universe_set_id}_strategy_id_{record.strategy_id}.csv"))
        del df

    def export_opt_to_csv(self, record: PortfolioRecord):
        pv = sum(record.simple_pv)
        csv_row = [record.strategy_id]
        csv_row.extend(record.strategy_param)
        csv_row.append(pv)
        csv_path = os.path.join(self.save_path, f"{self.file_name}_u_{self.universe_set_id}_opt.csv")
        with open(csv_path, 'a', newline='')as f:
            f_csv = csv.writer(f)
            f_csv.writerow(csv_row)

    def export_other_to_csv(self, results, columns, timestamp_list):
        df0 = pd.DataFrame(timestamp_list, columns=['timestamp'])
        df0['datetime'] = list(map(datetime.fromtimestamp, df0['timestamp']))

        for i in range(len(columns)):
            result = results[i]
            df = pd.DataFrame(result, columns=[columns[i]])
            df0 = df0.join(df)
        df0.to_csv(os.path.join(self.save_path,
                                f"{self.file_name}_u_{self.universe_set_id}_allocation.csv"))

    def read_from_sqlite(self):
        db_path = self.get_database_path()
        db_connector = DatabaseConnector(file_path=db_path)
        self.logger.info("read level1 record data from sqlite...")
        self.portfolio_record_list = db_connector.read_record_list()
        del db_connector
        self.logger.info("level1 record data loaded!")

    def add_symbol_name(self, universe, value_set):
        holding_one = {}
        for i in range(len(universe)):
            holding_one[universe[i]] = value_set[i]
        return holding_one

    def structure_time_result(self, record: PortfolioRecord, holdings):
        if len(record.universe) != len(holdings[0]):
            print("The len is not the same?!")
            return None
        holdings_new = {}
        collect_timing = record.timestamp_list[0]
        for h in range(len(record.timestamp_list)):
            if collect_timing == record.timestamp_list[h]:
                holdings_new[record.timestamp_list[h]] = self.add_symbol_name(record.universe, holdings[h])
                collect_timing += self.collect_freq_mode.value
        return holdings_new

    def flatten_record(self, save=True):
        db_path = self.get_database_path()
        db_connector = DatabaseConnector(file_path=db_path)

        new_portfolio_record_list = []
        new_strategy_id = strategy_id_generator()

        if len(self.portfolio_record_list) == 0:
            self.read_from_sqlite()

        for portfolio_record in tqdm(self.portfolio_record_list, desc='Flatten result'):
            # 目前所有portfolio_record都还没有从数据库中读出来, 先使用db_connector读取数据
            portfolio_record_with_real_data = db_connector.read_record_by_strategy_id(strategy_id=portfolio_record.strategy_id)
            # for i in range(len(portfolio_record_with_real_data.universe)):
            #     new_portfolio_record = PortfolioRecord()
            assert portfolio_record_with_real_data.trade_record.trade_symbol_list == portfolio_record_with_real_data.universe  # 不然之后可能会有问题
            # TODO: 目前拆分trade_record的方法会导致有重复的记录产生, 如[[10,10],[10,0]]会被拆成两条记录, 每一条的记录为[10,10]/[10,0]. 但是, 事实上, 原生的trade_record会记录成[10,]/[10,0]; 目前这一步的实现是通过两层的list_comp实现, 没有办法做到完全还原.
            temp_portfolio_record_list = [PortfolioRecord(universe=[portfolio_record_with_real_data.universe[i]],
                                                          pv0=[portfolio_record_with_real_data.pv0[i]],
                                                          simple_pv=[portfolio_record_with_real_data.simple_pv[i]],
                                                          position_list=[[portfolio_record_with_real_data.position_list[j][i]] for j in
                                                                         range(len(portfolio_record_with_real_data.position_list))],
                                                          pv_list=[[portfolio_record_with_real_data.pv_list[j][i]] for j in
                                                                   range(len(portfolio_record_with_real_data.pv_list))],
                                                          timestamp_list=portfolio_record_with_real_data.timestamp_list,
                                                          ref_aum_list=portfolio_record_with_real_data.ref_aum_list,
                                                          ccy_name_list=None,
                                                          ccy_metrics_list=None,
                                                          strategy_param=portfolio_record_with_real_data.strategy_param,
                                                          strategy_id=next(new_strategy_id),
                                                          signal_remark=portfolio_record_with_real_data.signal_remark,
                                                          trade_record=TradeRecord(
                                                              timestamp_list=portfolio_record_with_real_data.trade_record.timestamp_list,
                                                              volume_list=[[portfolio_record_with_real_data.trade_record.volume_list[j][i]] for j in
                                                                           range(len(portfolio_record_with_real_data.trade_record.volume_list))],
                                                              # 默认trade_record 和 portfolio_record中的顺序相同
                                                              price_list=[[portfolio_record_with_real_data.trade_record.price_list[j][i]] for j in
                                                                          range(len(portfolio_record_with_real_data.trade_record.price_list))] if len(
                                                                  portfolio_record_with_real_data.trade_record.price_list) > 0 else [],
                                                              trade_symbol_list=[portfolio_record_with_real_data.trade_record.trade_symbol_list[i]]))
                                          for i in range(len(portfolio_record.universe))]
            new_portfolio_record_list += temp_portfolio_record_list
        self.portfolio_record_list = new_portfolio_record_list
        self.params = list(self.portfolio_record_list[0].strategy_param.keys())
        self.timestamp_list = self.portfolio_record_list[0].timestamp_list
        if save:
            if len(self.portfolio_record_list) > 0:
                self.clean_save_data(is_drop_table=True, strategy_param=self.params, soft_mode=True)

                self.save_to_sqlite(many=True)

                self.read_from_sqlite()  # 释放掉完整的self.portfolio_record_list, 不然的话会占用很大的内存.
                del new_portfolio_record_list
