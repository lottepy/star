import os, shutil
import dataset
import json
from datafreeze import freeze
from .trade_record import TradeRecord
from .portfolio_record import PortfolioRecord


class DatabaseConnector:
    def __init__(self, file_path='result.db'):
        self.file_path = file_path
        self.db_path = 'sqlite:///' + file_path

    def __del__(self):
        pass

    def drop_database(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
        else:
            print("操作：删除上一个数据库 -> 要删除的文件不存在！开始创建新数据库...")

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

    def init_tables(self):
        db = dataset.connect(self.db_path)
        db.begin()
        try:
            table_strategy = db.create_table('strategy')
            table_strategy.create_column('universe', db.types.text)
            table_strategy.create_column('parameter', db.types.text)

            table_portfolio_record = db.create_table('portfolio_record')
            table_portfolio_record.create_column('strategy_id', db.types.integer)
            table_portfolio_record.create_column('simple_pv', db.types.text)
            table_portfolio_record.create_column('position_list', db.types.text)
            table_portfolio_record.create_column('pv_list', db.types.text)
            table_portfolio_record.create_column('ccy_name_list', db.types.text)
            table_portfolio_record.create_column('ccy_metrics_list', db.types.text)
            table_portfolio_record.create_column('signal_remark', db.types.text)

            table_trade_record = db.create_table('trade_record')
            table_trade_record.create_column('strategy_id', db.types.integer)
            table_trade_record.create_column('trade_symbol_list', db.types.text)
            table_trade_record.create_column('volume_list', db.types.text)
            table_trade_record.create_column('price_list', db.types.text)
            table_trade_record.create_column('timestamp_list', db.types.text)

            table_config_info = db.create_table('config_info')
            table_config_info.create_column('params', db.types.text)
            table_config_info.create_column('timestamp_list', db.types.text)
            db.commit()
        except:
            db.rollback()

    def insert_config_info(self, params, timestamp_list):
        db = dataset.connect(self.db_path)
        config_info = db.load_table('config_info')
        data = config_info.find_one(id=1)
        if data is None:
            config_info.insert(dict(params=json.dumps(params), timestamp_list=json.dumps(timestamp_list)))

    def insert_record_list(self, portfolio_list):
        strategy_id_list = []
        db = dataset.connect(self.db_path)
        db.begin()
        try:
            table_strategy = db.load_table('strategy')
            table_portfolio_record = db.load_table('portfolio_record')
            table_trade_record = db.load_table('trade_record')
            for portfolio_record in portfolio_list:
                trade_record = portfolio_record.trade_record
                strategy_id = table_strategy.insert(dict(universe=json.dumps(portfolio_record.universe),
                                                         parameter=json.dumps(portfolio_record.strategy_param)))
                table_portfolio_record.insert(dict(strategy_id=strategy_id,
                                                   pv0=json.dumps(portfolio_record.pv0),
                                                   simple_pv=json.dumps(portfolio_record.simple_pv),
                                                   position_list=json.dumps(portfolio_record.position_list),
                                                   pv_list=json.dumps(portfolio_record.pv_list),
                                                   ref_aum_list=json.dumps(portfolio_record.ref_aum_list),
                                                   ccy_name_list=json.dumps(portfolio_record.ccy_name_list),
                                                   ccy_metrics_list=json.dumps(portfolio_record.ccy_metrics_list),
                                                   signal_remark=json.dumps(portfolio_record.signal_remark)))
                table_trade_record.insert(dict(strategy_id=strategy_id,
                                               trade_symbol_list=json.dumps(trade_record.trade_symbol_list),
                                               volume_list=json.dumps(trade_record.volume_list),
                                               price_list=json.dumps(trade_record.price_list),
                                               timestamp_list=json.dumps(trade_record.timestamp_list)))
                strategy_id_list.append(strategy_id)
            db.commit()
        except BaseException as e:
            print(e)
            db.rollback()
        return strategy_id_list

    def insert_record_list_many(self, portfolio_list):
        # use insert_many to speed up
        # 只适用于干净的表
        db = dataset.connect(self.db_path)
        db.begin()
        try:
            table_strategy = db.load_table('strategy')
            table_portfolio_record = db.load_table('portfolio_record')
            table_trade_record = db.load_table('trade_record')
            
            strategy_id_list = [i.strategy_id for i in portfolio_list]
            strategy_list = [dict(universe=json.dumps(i.universe),
                                  parameter=json.dumps(list(i.strategy_param.values())))
                             for i in portfolio_list]
            portfolio_record_list = [dict(strategy_id=i.strategy_id,
                                          pv0=json.dumps(i.pv0),
                                          simple_pv=json.dumps(i.simple_pv),
                                          position_list=json.dumps(i.position_list),
                                          pv_list=json.dumps(i.pv_list),
                                          ref_aum_list=json.dumps(i.ref_aum_list),
                                          ccy_name_list=json.dumps(i.ccy_name_list),
                                          ccy_metrics_list=json.dumps(i.ccy_metrics_list),
                                          signal_remark=json.dumps(i.signal_remark))
                                    for i in portfolio_list]
            trade_record_list = [dict(strategy_id=i.strategy_id,
                                      trade_symbol_list=json.dumps(i.trade_record.trade_symbol_list),
                                      volume_list=json.dumps(i.trade_record.volume_list),
                                      price_list=json.dumps(i.trade_record.price_list),
                                      timestamp_list=json.dumps(i.trade_record.timestamp_list))
                                 for i in portfolio_list]
            
            table_strategy.insert_many(strategy_list)
            table_portfolio_record.insert_many(portfolio_record_list)
            table_trade_record.insert_many(trade_record_list)
            
            db.commit()
            
        except BaseException as e:
            print(e)
            db.rollback()
        return strategy_id_list

    # def insert_record(self, portfolio_record):
    #     db = dataset.connect(self.db_path)
    #     db.begin()
    #     try:
    #         table_strategy = db.load_table('strategy')
    #         table_portfolio_record = db.load_table('portfolio_record')
    #         table_trade_record = db.load_table('trade_record')
    #         trade_record = portfolio_record.trade_record
    #         strategy_id = table_strategy.insert(dict(universe=json.dumps(portfolio_record.universe),
    #                                                  parameter=json.dumps(portfolio_record.strategy_param)))
    #         table_trade_record.insert(dict(strategy_id=strategy_id,
    #                                        trade_symbol_list=json.dumps(trade_record.trade_symbol_list),
    #                                        volume_list=json.dumps(trade_record.volume_list),
    #                                        price_list=json.dumps(trade_record.price_list),
    #                                        timestamp_list=json.dumps(trade_record.timestamp_list)))
    #         table_portfolio_record.insert(dict(strategy_id=strategy_id,
    #                                            simple_pv=json.dumps(portfolio_record.simple_pv),
    #                                            position_list=json.dumps(portfolio_record.position_list),
    #                                            pv_list=json.dumps(portfolio_record.pv_list),
    #                                            signal_remark=json.dumps(portfolio_record.signal_remark)))
    #         db.commit()
    #     except BaseException as e:
    #         print(e)
    #         db.rollback()

    def convert_sqlite_to_csv(self, csv_path):
        db = dataset.connect(self.db_path)
        strategy = db.load_table('strategy').all()
        freeze(strategy, format='csv', filename=os.path.join("result", "strategy.csv"))
        portfolio_record = db.load_table('portfolio_record').all()
        freeze(portfolio_record, format='csv', filename=os.path.join("result", "portfolio_record.csv"))
        trade_record = db.load_table('trade_record').all()
        freeze(trade_record, format='csv', filename=os.path.join("result", "trade_record.csv"))
        config_info = db.load_table('config_info').all()
        freeze(config_info, format='csv', filename=os.path.join("result", "config_info.csv"))
        if csv_path != "result":
            self.move_file(os.path.join("result", "strategy.csv"), os.path.join(csv_path, "strategy.csv"))
            self.move_file(os.path.join("result", "portfolio_record.csv"),
                           os.path.join(csv_path, "portfolio_record.csv"))
            self.move_file(os.path.join("result", "trade_record.csv"), os.path.join(csv_path, "trade_record.csv"))
            self.move_file(os.path.join("result", "config_info.csv"), os.path.join(csv_path, "config_info.csv"))

    def read_record_list_old(self):
        portfolio_list = []
        db = dataset.connect(self.db_path, engine_kwargs=dict(poolclass=None))
        config_info = db.load_table('config_info')
        config_info = config_info.find_one(id=1)
        parameter_name = json.loads(config_info["params"])
        timestamp_list = json.loads(config_info["timestamp_list"])
        table_strategy = db.load_table('strategy')
        table_portfolio_record = db.load_table('portfolio_record')
        table_trade_record = db.load_table('trade_record')

        strategy_list = table_strategy.find()
        portfolio_record_list = table_portfolio_record.find()
        trade_record_list = table_trade_record.find()
        for strategy, portfolio_data, trade_data in zip(strategy_list, portfolio_record_list, trade_record_list):
            # 得到record_param所需格式
            record_param = {}
            for name, param in zip(parameter_name, json.loads(strategy['parameter'])):
                record_param[name] = param

            trade_record = TradeRecord(trade_symbol_list=json.loads(trade_data['trade_symbol_list']),
                                       volume_list=json.loads(trade_data['volume_list']),
                                       price_list=json.loads(trade_data['price_list']),
                                       timestamp_list=json.loads(trade_data['timestamp_list']))
            portfolio_record = PortfolioRecord(strategy_id=strategy['id'],
                                               universe=json.loads(strategy['universe']),
                                               strategy_param=record_param,
                                               simple_pv=json.loads(portfolio_data['simple_pv']),
                                               position_list=json.loads(portfolio_data['position_list']),
                                               trade_record=trade_record,
                                               signal_remark=json.loads(portfolio_data['signal_remark']),
                                               pv_list=json.loads(portfolio_data['pv_list']))
            portfolio_record.timestamp_list = timestamp_list
            portfolio_list.append(portfolio_record)

        return portfolio_list

    def read_record_list(self):
        portfolio_list = []
        db = dataset.connect(self.db_path, engine_kwargs=dict(poolclass=None))
        config_info = db.load_table('config_info')
        config_info = config_info.find_one(id=1)
        parameter_name = json.loads(config_info["params"])
        table_strategy = db.load_table('strategy')
        strategy_list = table_strategy.find()
        for strategy in strategy_list:
            # 得到record_param所需格式
            # record_param = json.loads(strategy['parameter'])
            record_param = {}
            for name, param in zip(parameter_name, json.loads(strategy['parameter'])):
                record_param[name] = param
            portfolio_record = PortfolioRecord(strategy_id=strategy['id'],
                                               universe=json.loads(strategy['universe']),
                                               strategy_param=record_param)
            portfolio_list.append(portfolio_record)
        return portfolio_list

    # TODO:实现单独读取1个
    def read_record_by_strategy_id(self, strategy_id):
        db = dataset.connect(self.db_path, engine_kwargs=dict(poolclass=None))
        config_info = db.load_table('config_info')
        config_info = config_info.find_one(id=1)
        parameter_name = json.loads(config_info["params"])
        timestamp_list = json.loads(config_info["timestamp_list"])
        table_strategy = db.load_table('strategy')
        table_portfolio_record = db.load_table('portfolio_record')
        table_trade_record = db.load_table('trade_record')

        strategy = table_strategy.find_one(id=strategy_id)
        portfolio_data = table_portfolio_record.find_one(strategy_id=strategy_id)
        trade_data = table_trade_record.find_one(strategy_id=strategy_id)
        # 得到record_param所需格式
        record_param = {}
        for name, param in zip(parameter_name, json.loads(strategy['parameter'])):
            record_param[name] = param
        # record_param = json.loads(strategy['parameter'])

        trade_record = TradeRecord(trade_symbol_list=json.loads(trade_data['trade_symbol_list']),
                                   volume_list=json.loads(trade_data['volume_list']),
                                   price_list=json.loads(trade_data['price_list']),
                                   timestamp_list=json.loads(trade_data['timestamp_list']))
        portfolio_record = PortfolioRecord(strategy_id=strategy['id'],
                                           universe=json.loads(strategy['universe']),
                                           strategy_param=record_param,
                                           pv0=json.loads(portfolio_data['pv0']),
                                           simple_pv=json.loads(portfolio_data['simple_pv']),
                                           position_list=json.loads(portfolio_data['position_list']),
                                           trade_record=trade_record,
                                           signal_remark=json.loads(portfolio_data['signal_remark']),
                                           pv_list=json.loads(portfolio_data['pv_list']),
                                           ref_aum_list=json.loads(portfolio_data['ref_aum_list']),
                                           ccy_name_list=json.loads(portfolio_data['ccy_name_list']),
                                           ccy_metrics_list=json.loads(portfolio_data['ccy_metrics_list']))
        portfolio_record.timestamp_list = timestamp_list
        return portfolio_record

    def move_file(self, srcfile, dstfile):
        if not os.path.isfile(srcfile):
            print("%s not exist!" % (srcfile))
        else:
            fpath, fname = os.path.split(dstfile)  # 分离文件名和路径
            if not os.path.exists(fpath):
                os.makedirs(fpath)  # 创建路径
            shutil.move(srcfile, dstfile)  # 移动文件
            # print("move %s -> %s" % (srcfile, dstfile))

    def copy_file(self, srcfile, dstfile):
        if not os.path.isfile(srcfile):
            print("%s not exist!" % (srcfile))
        else:
            fpath, fname = os.path.split(dstfile)  # 分离文件名和路径
            if not os.path.exists(fpath):
                os.makedirs(fpath)  # 创建路径
            shutil.copyfile(srcfile, dstfile)  # 复制文件
            # print("copy %s -> %s" % (srcfile, dstfile))

# 关于sqlite的dataset报错：产生原因

# More on Invalidation
# The Pool provides “connection invalidation” services which allow both explicit invalidation of a connection as well as automatic invalidation in response to conditions that are determined to render a connection unusable.
# “Invalidation” means that a particular DBAPI connection is removed from the pool and discarded. The .close() method is called on this connection if it is not clear that the connection itself might not be closed, however if this method fails, the exception is logged but the operation still proceeds.
# When using a Engine, the Connection.invalidate() method is the usual entrypoint to explicit invalidation. Other conditions by which a DBAPI connection might be invalidated include:
# a DBAPI exception such as OperationalError, raised when a method like connection.execute() is called, is detected as indicating a so-called “disconnect” condition. As the Python DBAPI provides no standard system for determining the nature of an exception, all SQLAlchemy dialects include a system called is_disconnect() which will examine the contents of an exception object, including the string message and any potential error codes included with it, in order to determine if this exception indicates that the connection is no longer usable. If this is the case, the _ConnectionFairy.invalidate() method is called and the DBAPI connection is then discarded.
# When the connection is returned to the pool, and calling the connection.rollback() or connection.commit() methods, as dictated by the pool’s “reset on return” behavior, throws an exception. A final attempt at calling .close() on the connection will be made, and it is then discarded.
# When a listener implementing PoolEvents.checkout() raises the DisconnectionError exception, indicating that the connection won’t be usable and a new connection attempt needs to be made.
# All invalidations which occur will invoke the PoolEvents.invalidate() event.

# 有关失效的更多信息
# 在Pool提供了“连接无效”的服务，其允许连接的，以被确定为不可用的渲染的连接条件显性无效，以及在响应自动失效。
# “无效”表示从池中删除了特定的DBAPI连接并将其丢弃。.close()如果不清楚连接本身是否可能未关闭，则在此连接上调用该方法，但是，如果此方法失败，则会记录异常，但操作仍会继续。
# 当使用时Engine，该Connection.invalidate()方法是显式失效的常用入口点。使DBAPI连接无效的其他条件包括：
# 检测到DBAPI异常（例如OperationalError，在connection.execute()调用诸如like的方法时引发的异常）指示了所谓的“断开连接”条件。由于Python DBAPI没有提供用于确定异常性质的标准系统，因此所有SQLAlchemy方言都包含一个名为的系统is_disconnect()，该系统将检查异常对象的内容，包括字符串消息和其附带的任何潜在错误代码，以确定如果该异常表明该连接不再可用。在这种情况下，将_ConnectionFairy.invalidate()调用该方法，然后丢弃DBAPI连接。
# 当连接返回到池时，并根据池的“返回时重置”行为指示调用connection.rollback()or connection.commit()方法，将引发异常。将最终尝试调用.close()该连接，然后将其丢弃。
# 当实现的侦听器PoolEvents.checkout()引发 DisconnectionError异常时，表明该连接将不可用，需要进行新的连接尝试。
# 发生的所有无效都会调用该PoolEvents.invalidate() 事件。
