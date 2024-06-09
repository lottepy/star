import json
import os

import dataset

from .portfolio_record import PortfolioRecord
from .strategy_id_generator import strategy_id_generator
from .trade_record import TradeRecord


class DatabaseConnector:
    def __init__(self, file_path='result.db'):
        self.file_path = file_path
        self.db_path = 'sqlite:///' + file_path

    def drop_tables(self, tables:list=['strategy','portfolio_record','trade_record','config_info']):
        db = dataset.connect(self.db_path)
        db.begin()
        try:
            for table_name in tables:
                temp_table = db.load_table(table_name)
                temp_table.drop()
            db.commit()
        except:
            db.rollback()

    def create_strategy_mapping(self, flatten=False):
        db = dataset.connect(self.db_path)
        db.begin()
        try:
            table_strategy_mapping = db.create_table('strategy_mapping')
            table_strategy_mapping.create_column('vid', db.types.integer)
            table_strategy_mapping.create_column(
                'strategy_id', db.types.integer)
            table_strategy_mapping.create_column('n', db.types.integer)
            db.commit()
        except:
            db.rollback()

        if not flatten:

            virtual_strategy_list = []
            vid_generator = strategy_id_generator()

            table_strategy = db.load_table('strategy')
            # table_portfolio_record = db.load_table('portfolio_record')
            # table_trade_record=db.load_table('trade_record')

            resiter = table_strategy.find()
            for row in resiter:
                virtual_strategy_list.append(
                    {'vid': next(vid_generator), 'strategy_id': row['id'], 'n': -1})

            table_strategy_mapping = db.load_table('strategy_mapping')
            table_strategy_mapping.insert_many(virtual_strategy_list)

        else:

            virtual_strategy_list = []
            vid_generator = strategy_id_generator()

            table_strategy = db.load_table('strategy')

            resiter = table_strategy.find()
            for row in resiter:
                universe = json.loads(row['universe'])
                for i in range(len(universe)):
                    virtual_strategy_list.append(
                        {'vid': next(vid_generator), 'strategy_id': row['id'], 'n': i})

            table_strategy_mapping = db.load_table('strategy_mapping')
            table_strategy_mapping.insert_many(virtual_strategy_list)

    def read_record_list_virtual(self):
        portfolio_list = []
        db = dataset.connect(self.db_path, engine_kwargs=dict(poolclass=None))
        config_info = db.load_table('config_info')
        config_info = config_info.find_one(id=1)
        parameter_name = json.loads(config_info["params"])

        table_strategy_mapping = db.load_table('strategy_mapping')
        table_strategy = db.load_table('strategy')
        resiter = table_strategy_mapping.find()
        for row in resiter:
            vid = row['vid']
            n = row['n']
            strategy_id = row['strategy_id']

            strategy = table_strategy.find_one(id=strategy_id)

            record_param = {}
            for name, param in zip(parameter_name, json.loads(strategy['parameter'])):
                record_param[name] = param
            portfolio_record = PortfolioRecord(strategy_id=row['vid'],
                                               universe=[json.loads(strategy['universe'])[
                                                   n]] if n != -1 else json.loads(strategy['universe']),
                                               strategy_param=record_param)
            portfolio_list.append(portfolio_record)
        return portfolio_list

    def read_record_by_vid(self, vid):
        db = dataset.connect(self.db_path, engine_kwargs=dict(poolclass=None))
        config_info = db.load_table('config_info')
        config_info = config_info.find_one(id=1)
        parameter_name = json.loads(config_info["params"])
        timestamp_list = json.loads(config_info["timestamp_list"])

        table_strategy_mapping = db.load_table('strategy_mapping')
        table_strategy = db.load_table('strategy')
        table_portfolio_record = db.load_table('portfolio_record')
        table_trade_record = db.load_table('trade_record')

        virtual_strategy = table_strategy_mapping.find_one(vid=vid)
        strategy_id = virtual_strategy['strategy_id']
        n = virtual_strategy['n']
        strategy = table_strategy.find_one(id=strategy_id)
        portfolio_data = table_portfolio_record.find_one(
            strategy_id=strategy_id)
        trade_data = table_trade_record.find_one(strategy_id=strategy_id)
        # 得到record_param所需格式
        record_param = {}
        for name, param in zip(parameter_name, json.loads(strategy['parameter'])):
            record_param[name] = param

        trade_record = TradeRecord(trade_symbol_list=[json.loads(trade_data['trade_symbol_list'])[n]]
                                   if n != -1 else json.loads(trade_data['trade_symbol_list']),
                                   volume_list=[[i[n]]for i in json.loads(
                                       trade_data['volume_list'])] if n != -1 else json.loads(trade_data['volume_list']),
                                   price_list=[[i[n]]for i in json.loads(
                                       trade_data['price_list'])] if n != -1 else json.loads(trade_data['price_list']),
                                   timestamp_list=json.loads(
                                       trade_data['timestamp_list'])
                                   )
        portfolio_record = PortfolioRecord(strategy_id=vid,
                                           universe=[json.loads(strategy['universe'])[
                                               n]] if n != -1 else json.loads(strategy['universe']),
                                           strategy_param=record_param,
                                           pv0=[json.loads(portfolio_data['pv0'])[
                                               n]] if n != -1 else json.loads(portfolio_data['pv0']),
                                           simple_pv=[json.loads(portfolio_data['simple_pv'])[
                                               n]] if n != -1 else json.loads(portfolio_data['simple_pv']),
                                           position_list=[[i[n]]for i in json.loads(
                                               portfolio_data['position_list'])]
                                           if n != -1 else json.loads(portfolio_data['position_list']),
                                           trade_record=trade_record,
                                           signal_remark=json.loads(
                                               portfolio_data['signal_remark']),
                                           pv_list=[[i[n]]for i in json.loads(
                                               portfolio_data['pv_list'])]
                                           if n != -1 else json.loads(portfolio_data['pv_list']),
                                           ref_aum_list=[i/len(json.loads(strategy['universe'])) for i in json.loads(
                                               portfolio_data['ref_aum_list'])] if n != -1 else json.loads(portfolio_data['ref_aum_list']),
                                           ccy_name_list=None if n != -
                                           1 else json.loads(portfolio_data['ccy_name_list']),
                                           ccy_metrics_list=None if n != -1 else json.loads(portfolio_data['ccy_metrics_list']),
                                           commission=[[i[n]]for i in json.loads(
                                               portfolio_data['commission_list'])]
                                           if n != -1 else json.loads(portfolio_data['commission_list']))
        portfolio_record.timestamp_list = timestamp_list
        return portfolio_record
