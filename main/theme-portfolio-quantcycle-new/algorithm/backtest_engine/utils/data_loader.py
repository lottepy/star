import math
import os
from datetime import datetime, timedelta
from functools import reduce
from multiprocessing import Queue

import numpy as np
import pandas as pd
from datamaster import dm_client

from ..utils.constants import DATA_LOADER_DAILY_DATA_BATCH, InstrumentType
from .ccy_mapping import (CCY_INDEX, NDF, SPOT, exchange_id_exchange_map,
                          fx_bbg_dividends_ccy_map)
from .get_logger import get_logger
# from .helper import get_bt_dates
from .rate_calculator import fp_to_rate, tn_to_rate, usd_to_rate

# for datetime objects
get_year = lambda x: x.year
get_month = lambda x: x.month
get_day = lambda x: x.day
get_hour = lambda x: x.hour
get_minute = lambda x: x.minute
get_second = lambda x: x.second


class DataLoader():

    def __init__(self, symbol: list, ccy: list, symbol2bbg_dict: dict, symbol2back_up_bbg_dict: dict, symbol2instrument_type_dict: dict,
                 ccy2bbg_dict: dict, ccy2back_up_bbg_dict: dict, ccy2trading_ccy_dict: dict, symbol2source_dict: dict, start: datetime, end: datetime,
                 params: dict, base_ccy,
                 local_data_dir,local_rate_data_dir, local_ref_data_dir, local_ref_data_symbols,stock_data_type,symbol2exchange_id_dict):
        self.symbol = symbol
        self.ccy = ccy
        self.start = start
        self.end = end
        self.params = params
        self.symbol2bbg_dict = symbol2bbg_dict
        self.symbol2back_up_bbg_dict = symbol2back_up_bbg_dict
        self.symbol2instrument_type_dict = symbol2instrument_type_dict
        self.ccy2bbg_dict = ccy2bbg_dict
        self.ccy2back_up_bbg_dict = ccy2back_up_bbg_dict
        self.ccy2trading_ccy_dict = ccy2trading_ccy_dict
        self.symbol2source_dict = symbol2source_dict
        self.base_ccy = base_ccy
        self.local_data_dir = local_data_dir
        self.local_rate_data_dir = local_rate_data_dir
        self.symbol2exchange_id_dict = symbol2exchange_id_dict

        self.local_ref_data_dir = local_ref_data_dir
        self.local_ref_data_symbols = local_ref_data_symbols
        self.logger = get_logger('backtest_engine.utils.data_loader', log_path='backtest_engine.log')
        self.stock_data_type = stock_data_type
        # self.download_dates = get_bt_dates(start=start, end=end)

        self.ccy_queue = Queue(maxsize=1)
        self.dividends_queue = Queue(maxsize=1)
        self.minute_queue = Queue(maxsize=1)
        self.hour_queue = Queue(maxsize=1)
        self.daily_queue = Queue(maxsize=1)

    def modify_date(self, start, end):
        self.start = start
        self.end = end

    ''' def start_load_snapshot(self, processes=1):
        """
            往队列中推送 snapshot_time_array, snapshot_data_array。
            假设n为时间点的数量，k为股票的数量，

            snapshot_time_array 的维度为(n, 8)，8列分别为 timestamp, time, year, month, day, hour, minute, second。
            这些列的列偏移量分别为 SnapshotTime.TIMESTAMP.value, SnapshotTime.TIME.value, SnapshotTime.YEAR.value 以此类推。

            snapshot_data_array 的维度为(n, k, 50)，第3维的50列分别为 open, high, low, preclose, last, totalVolumeTrade 等。
            这些列的列偏移量为 SnapshotData.OPEN.value 等
        """
        for symbol_universe_id, symbols in enumerate(self.symbol_universe):
            _batch_size = self.params['universe'][symbol_universe_id]['batch_size']
            for symbols_i in range(0, len(symbols), _batch_size):  # 分批处理 每次只放batch_size只股票的数据到队列中
                symbol_batch = symbols[symbols_i: symbols_i + _batch_size]

                for date in self.download_dates:
                    self.logger.debug(f'\n--------------loading snapshot data {date}-------------------')
                    snapshot_df = get_snapshot_df(symbol_batch, date, date, processes)  # 一次只返回一批股票一天的数据
                    time_column = ['timestamp']
                    field_column = ['open', 'high', 'low', 'preclose'] + \
                                   ['last', 'totalVolumeTrade', 'totalValueTrade'] + \
                                   [f'b{i}' for i in range(1, 11)] + [f'bv{i}' for i in range(1, 11)] + \
                                   [f'a{i}' for i in range(1, 11)] + [f'av{i}' for i in range(1, 11)] + \
                                   ['iopv', 'totalBidQty', 'totalAskQty']  # length=50
                    symbol_column = []
                    for sym in symbol_batch:
                        symbol_column += [col + f"_{sym}" for col in field_column]
                    all_column = time_column + symbol_column  # length=1+50k

                    snapshot_df = snapshot_df[all_column]  # 排序各列 使第1列为timestamp
                    snapshot_df_values = snapshot_df.values

                    snapshot_data_array = snapshot_df_values[:, 1:]  # 去除第1列的timestamp剩余的array
                    snapshot_data_array = snapshot_data_array.reshape(snapshot_data_array.shape[0], len(symbol_batch),
                                                                      -1).copy()  # shape (n, k, 50)

                    _datetime_list = list(map(datetime.fromtimestamp, snapshot_df_values[:, 0]))
                    snapshot_time_array = np.empty((snapshot_data_array.shape[0], 8))  # shape (n, 8)  8列 下面分别填充
                    snapshot_time_array[:, SnapshotTime.TIMESTAMP.value] = snapshot_df_values[:, 0]
                    snapshot_time_array[:, SnapshotTime.YEAR.value] = list(map(get_year, _datetime_list))
                    snapshot_time_array[:, SnapshotTime.MONTH.value] = list(map(get_month, _datetime_list))
                    snapshot_time_array[:, SnapshotTime.DAY.value] = list(map(get_day, _datetime_list))
                    snapshot_time_array[:, SnapshotTime.HOUR.value] = list(map(get_hour, _datetime_list))
                    snapshot_time_array[:, SnapshotTime.MINUTE.value] = list(map(get_minute, _datetime_list))
                    snapshot_time_array[:, SnapshotTime.SECOND.value] = list(map(get_second, _datetime_list))
                    snapshot_time_array[:, SnapshotTime.TIME.value] = get_today_seconds(
                        snapshot_time_array[:, SnapshotTime.HOUR.value],
                        snapshot_time_array[:, SnapshotTime.MINUTE.value],
                        snapshot_time_array[:, SnapshotTime.SECOND.value])
                    if self.snapshot_queue.full():
                        self.logger.info(
                            'snapshot queue is full, will wait until at least one content is consumed..')  # 按照输出的log 分配多少核处理数据 多少核回测
                    self.snapshot_queue.put(
                        (date, symbol_batch, snapshot_time_array, snapshot_data_array, symbol_universe_id))
        self.snapshot_queue.put('FINISH')
        self.logger.info(f'all snapshot data loaded, thread will quit') '''

    ''' def start_load_minute(self, processes=1):
        """
            往队列中推送 minute_time_array, minute_data_array。
            假设n为时间点的数量，k为股票的数量，

            minute_time_array 的维度为(n, 8)，8列分别为 timestamp, time, year, month, day, hour, minute, second。
            这些列的列偏移量分别为 MinuteTime.TIMESTAMP.value, MinuteTime.TIME.value, MinuteTime.YEAR.value 以此类推。

            minute_data_array 的维度为(n, k, 6)，第3维的6列分别为 open, high, low, preclose, last, totalVolumeTrade 等。
            这些列的列偏移量为 Data.OPEN.value 等
        """
        for symbol_universe_id, symbols in enumerate(self.symbol_universe):
            _batch_size = self.params['universe'][symbol_universe_id]['batch_size']
            for symbols_i in range(0, len(symbols), _batch_size):  # 分批处理 每次只放batch_size只股票的数据到队列中
                symbol_batch = symbols[symbols_i: symbols_i + _batch_size]

                self.logger.debug(f'\n--------------loading minute data-------------------')
                minute_df = get_minute_df(symbol_batch, self.download_dates[0], self.download_dates[-1],
                                          processes)  # 返回从头到尾的数据
                time_column = ['timestamp']
                field_column = ['open', 'high', 'low', 'close', 'volume', 'amount']
                symbol_column = []
                for sym in symbol_batch:
                    symbol_column += [col + f"_{sym}" for col in field_column]
                all_column = time_column + symbol_column  # length=1+6k

                minute_df = minute_df[all_column]  # 排序各列 使第1列为timestamp
                minute_df_values = minute_df.values

                minute_data_array = minute_df_values[:, 1:]  # 去除第1列的timestamp剩余的array
                minute_data_array = minute_data_array.reshape(minute_data_array.shape[0], len(symbol_batch),
                                                              -1).copy()  # shape (n, k, -1)  -1的维度应为6

                _datetime_list = list(map(datetime.fromtimestamp, minute_df_values[:, 0]))
                minute_time_array = np.empty((minute_data_array.shape[0], 8))  # shape (n, 8)  8列 下面分别填充
                minute_time_array[:, MinuteTime.TIMESTAMP.value] = minute_df_values[:, 0]
                minute_time_array[:, MinuteTime.YEAR.value] = list(map(get_year, _datetime_list))
                minute_time_array[:, MinuteTime.MONTH.value] = list(map(get_month, _datetime_list))
                minute_time_array[:, MinuteTime.DAY.value] = list(map(get_day, _datetime_list))
                minute_time_array[:, MinuteTime.HOUR.value] = list(map(get_hour, _datetime_list))
                minute_time_array[:, MinuteTime.MINUTE.value] = list(map(get_minute, _datetime_list))
                minute_time_array[:, MinuteTime.SECOND.value] = list(map(get_second, _datetime_list))
                minute_time_array[:, MinuteTime.TIME.value] = get_today_seconds(
                    minute_time_array[:, MinuteTime.HOUR.value],
                    minute_time_array[:, MinuteTime.MINUTE.value],
                    minute_time_array[:, MinuteTime.SECOND.value])
                if self.minute_queue.full():
                    self.logger.info(
                        'minute queue is full, will wait until at least one content is consumed..')  # 按照输出的log 分配多少核处理数据 多少核回测
                self.minute_queue.put((self.download_dates[-1], symbol_batch, minute_time_array, minute_data_array))
        self.minute_queue.put('FINISH')
        self.logger.info(f'all minute data loaded, thread will quit') '''

    ''' def start_load_trade(self, processes=1):
        """
            往队列中推送 trade_time_array, trade_data_array。
            假设n为时间点的数量，k为股票的数量，

            trade_time_array 的维度为(n, 8)，8列分别为 timestamp, time, year, month, day, hour, trade, second。
            这些列的列偏移量分别为 TradeTime.TIMESTAMP.value, TradeTime.TIME.value, TradeTime.YEAR.value 以此类推。

            trade_data_array 的维度为(n, k, 3)，第3维的3列分别为 price, side, volume。
            这些列的列偏移量为 TradeData.PRICE.value 等
        """
        for symbol_universe_id, symbols in enumerate(self.symbol_universe):
            _batch_size = self.params['universe'][symbol_universe_id]['batch_size']
            for symbols_i in range(0, len(symbols), _batch_size):  # 分批处理 每次只放batch_size只股票的数据到队列中
                symbol_batch = symbols[symbols_i: symbols_i + _batch_size]

                for date in self.download_dates:
                    self.logger.debug(f'\n--------------loading trade data {date}-------------------')
                    trade_df = get_trade_df(symbol_batch, date, date, processes)  # 一次只返回一批股票一天的数据
                    time_column = ['timestamp']
                    field_column = ['price', 'side', 'volume']
                    symbol_column = []
                    for sym in symbol_batch:
                        symbol_column += [col + f"_{sym}" for col in field_column]
                    all_column = time_column + symbol_column  # length=1+3k

                    trade_df = trade_df[all_column]  # 排序各列 使第1列为timestamp
                    trade_df_values = trade_df.values

                    trade_data_array = trade_df_values[:, 1:]  # 去除第1列的timestamp剩余的array
                    trade_data_array = trade_data_array.reshape(trade_data_array.shape[0], len(symbol_batch),
                                                                -1).copy()  # shape (n, k, -1)  -1的维度应为3

                    _datetime_list = list(map(datetime.fromtimestamp, trade_df_values[:, 0]))
                    trade_time_array = np.empty((trade_data_array.shape[0], 8))  # shape (n, 8)  8列 下面分别填充
                    trade_time_array[:, TradeTime.TIMESTAMP.value] = trade_df_values[:, 0]
                    trade_time_array[:, TradeTime.YEAR.value] = list(map(get_year, _datetime_list))
                    trade_time_array[:, TradeTime.MONTH.value] = list(map(get_month, _datetime_list))
                    trade_time_array[:, TradeTime.DAY.value] = list(map(get_day, _datetime_list))
                    trade_time_array[:, TradeTime.HOUR.value] = list(map(get_hour, _datetime_list))
                    trade_time_array[:, TradeTime.MINUTE.value] = list(map(get_minute, _datetime_list))
                    trade_time_array[:, TradeTime.SECOND.value] = list(map(get_second, _datetime_list))
                    trade_time_array[:, TradeTime.TIME.value] = get_today_seconds(
                        trade_time_array[:, TradeTime.HOUR.value],
                        trade_time_array[:, TradeTime.MINUTE.value],
                        trade_time_array[:, TradeTime.SECOND.value])
                    if self.trade_queue.full():
                        self.logger.info(
                            'trade queue is full, will wait until at least one content is consumed..')  # 按照输出的log 分配多少核处理数据 多少核回测
                    self.trade_queue.put((date, symbol_batch, trade_time_array, trade_data_array))
        self.trade_queue.put('FINISH')
        self.logger.info(f'all trade data loaded, thread will quit') '''

    '''def start_load_daily(self, processes=1):
        """
            往队列中推送 daily_time_array, daily_data_array。
            假设n为时间点的数量，k为股票的数量，

            daily_time_array 的维度为(n, 4)，内容为timestamp, year, month, day。

            daily_data_array 的维度为(n, k, 6)，第3维的6列分别为 open, high, low, close, volume, amount。
            这些列的列偏移量为 Data.OPEN.value 等
        """
        for symbol_universe_id, symbols in enumerate(self.symbol_universe):
            _batch_size = self.params['universe'][symbol_universe_id]['batch_size']
            for symbols_i in range(0, len(symbols), _batch_size):  # 分批处理 每次只放batch_size只股票的数据到队列中
                symbol_batch = symbols[symbols_i: symbols_i + _batch_size]

                self.logger.debug(f'\n--------------loading daily data-------------------')
                daily_df = get_daily_df(symbol_batch, self.download_dates[0], self.download_dates[-1],
                                        processes)  # 返回从头到尾的数据
                time_column = ['timestamp']
                field_column = ['open', 'high', 'low', 'close', 'volume', 'amount']
                symbol_column = []
                for sym in symbol_batch:
                    symbol_column += [col + f"_{sym}" for col in field_column]
                all_column = time_column + symbol_column  # length=1+6k

                daily_df = daily_df[all_column]  # 排序各列 使第1列为timestamp
                daily_df_values = daily_df.values

                daily_data_array = daily_df_values[:, 1:]  # 去除第1列的timestamp剩余的array
                daily_data_array = daily_data_array.reshape(daily_data_array.shape[0], len(symbol_batch),
                                                            -1).copy()  # shape (n, k, -1)  -1的维度应为6

                _datetime_list = list(map(datetime.fromtimestamp, daily_df_values[:, 0]))
                daily_time_array = np.empty((daily_data_array.shape[0], 4))  # shape (n, 4)  4列 下面分别填充
                daily_time_array[:, DailyTime.TIMESTAMP.value] = daily_df_values[:, 0]
                daily_time_array[:, DailyTime.YEAR.value] = list(map(get_year, _datetime_list))
                daily_time_array[:, DailyTime.MONTH.value] = list(map(get_month, _datetime_list))
                daily_time_array[:, DailyTime.DAY.value] = list(map(get_day, _datetime_list))

                if self.daily_queue.full():
                    self.logger.info(
                        'daily queue is full, will wait until at least one content is consumed..')  # 按照输出的log 分配多少核处理数据 多少核回测
                self.daily_queue.put((self.download_dates[-1], symbol_batch, daily_time_array, daily_data_array))
        self.daily_queue.put('FINISH')
        self.logger.info(f'all daily data loaded, thread will quit')'''
    def get_exchange_exchange_calendar(self):

        exchange_trading_calendar_map = {}
        for exchange_id in set(self.symbol2exchange_id_dict.values()):
            exchange_name = exchange_id_exchange_map.get(exchange_id,None)
            if exchange_name is not None:
                res = dm_client.get("/exchange_calendar/",ex=exchange_name,start_date = self.start.strftime('%Y-%m-%d'),end_date = (self.end + timedelta(days=1)).strftime('%Y-%m-%d'))
                temp_calendar = pd.to_datetime(sorted(res), format='%Y-%m-%d', utc=True)
                time_df = pd.DataFrame(temp_calendar)
                time_df.rename(columns={0: 'date'}, inplace=True)
                time_df['year'] = time_df['date'].apply(lambda x: x.year)
                time_df['month'] = time_df['date'].apply(lambda x: x.month)
                time_df['day'] = time_df['date'].apply(lambda x: x.day)
                DatatimeArray = time_df[['year', 'month', 'day']].values.astype('uint64')
                exchange_trading_calendar_map[exchange_id] = DatatimeArray
        return exchange_trading_calendar_map



    def get_ref_factor(self, factor):
        """ read form local csv file """
        if factor in self.local_ref_data_symbols:
            self.logger.debug(f'\n--------------loading ref data {factor}-------------------')
            data_df = pd.read_csv(os.path.join(self.local_ref_data_dir, factor + ".csv"), index_col='date').ffill(axis=0)
            data_df.index = pd.to_datetime(data_df.index, format='%Y-%m-%d', utc=True)
            DataArray = data_df.values
            FxArray_time_df = pd.DataFrame(data_df.index)
            FxArray_time_df.rename(columns={0: 'date'}, inplace=True)
            FxArray_time_df['timestamp'] = FxArray_time_df['date'].apply(lambda x: datetime.timestamp(x))
            FxArray_time_df['end_timestamp'] = FxArray_time_df['date'].apply(lambda x: datetime.timestamp(x))
            FxArray_time_df['weekday'] = FxArray_time_df['date'].apply(lambda x: x.weekday())
            FxArray_time_df['year'] = FxArray_time_df['date'].apply(lambda x: x.year)
            FxArray_time_df['month'] = FxArray_time_df['date'].apply(lambda x: x.month)
            FxArray_time_df['day'] = FxArray_time_df['date'].apply(lambda x: x.day)
            DatatimeArray = FxArray_time_df[['timestamp', 'end_timestamp', 'weekday', 'year', 'month', 'day']].values.astype('uint64')

            return DataArray, DatatimeArray
        else:
            return None

    ###............................................get all data(dataframe) from diff data source and merge , only support data in dm............................................
    """ for min,hourly or daily trading data(OHLC), data loader first load data which is dataframe from each function base on instrument type
            and merge into one big dataframe and finally into numpy array """

    """ minute only hv fx """

    def start_load_minute(self, processes=1):
        self.logger.debug(f'\n--------------loading minute data {self.start} to {self.end}-------------------')
        """
            DataArray:shape(number of timepoint,number of symbol, enum constants.Data)
            DatatimeArray:shape(number of timepoint, enum constants.Time)
        """
        t_all_in = datetime.now()
        fx_symbol = list(filter(lambda x: self.symbol2instrument_type_dict[x] == InstrumentType.FX.value, self.symbol))
        select_columns = ['open', 'high', 'low', 'close']
        fx_data_df = pd.DataFrame()
        if len(fx_symbol) > 0:
            fx_data_df = get_dm_fx_min_data(fx_symbol, self.symbol2bbg_dict.copy(), self.start, self.end, select_columns=select_columns)
        stock_symbol = list(filter(lambda x: self.symbol2instrument_type_dict[x] != InstrumentType.FX.value, self.symbol))
        stock_data_df = pd.DataFrame()
        if len(stock_symbol) > 0:
            # stock_data_df = get_dm_daily_data(stock_symbol, self.symbol2bbg_dict.copy(), self.symbol2back_up_bbg_dict.copy(), self.start, self.end,select_columns=select_columns)
            # todo get_stock_min_function
            stock_data_df = pd.DataFrame()
        columns = list([f"{y}_{x}" for x in self.symbol for y in select_columns])
        data_df = pd.concat([fx_data_df, stock_data_df], axis=1,join='outer').fillna(method='ffill')
        data_df = data_df[columns]
        data_df = data_df.ffill(axis=0)
        DataArray = data_df.values.reshape(data_df.shape[0], len(self.symbol), -1)
        FxArray_time_df = pd.DataFrame(data_df.index)
        FxArray_time_df.rename(columns={0: 'date'}, inplace=True)
        FxArray_time_df['timestamp'] = FxArray_time_df['date'].apply(lambda x: datetime.timestamp(x))
        FxArray_time_df['end_timestamp'] = FxArray_time_df['date'].apply(lambda x: datetime.timestamp(x + timedelta(days=1)))
        FxArray_time_df['weekday'] = FxArray_time_df['date'].apply(lambda x: x.weekday())
        FxArray_time_df['year'] = FxArray_time_df['date'].apply(lambda x: x.year)
        FxArray_time_df['month'] = FxArray_time_df['date'].apply(lambda x: x.month)
        FxArray_time_df['day'] = FxArray_time_df['date'].apply(lambda x: x.day)
        FxArray_time_df['hour'] = FxArray_time_df['date'].apply(lambda x: x.hour)
        FxArray_time_df['minute'] = FxArray_time_df['date'].apply(lambda x: x.minute)
        FxArray_time_df['second'] = FxArray_time_df['date'].apply(lambda x: x.second)
        DatatimeArray = FxArray_time_df[['timestamp', 'end_timestamp', 'weekday', 'year', 'month', 'day', 'hour', 'minute', 'second']].values.astype(
            'uint64')

        t_all_out = datetime.now()
        self.logger.info(f'data download time:{t_all_out - t_all_in}')
        self.minute_queue.put((self.end, self.symbol, DataArray, DatatimeArray))
        self.logger.info(f'all minute data loaded, thread will quit')

    """ hour only hv fx """

    def start_load_hour(self, processes=1):
        """
            DataArray:shape(number of timepoint,number of symbol, enum constants.Data)
            DatatimeArray:shape(number of timepoint, enum constants.Time)
        """
        self.logger.debug(f'\n--------------loading hour data {self.start} to {self.end}-------------------')
        t_all_in = datetime.now()
        fx_symbol = list(filter(lambda x: self.symbol2instrument_type_dict[x] == InstrumentType.FX.value, self.symbol))
        select_columns = ['open', 'high', 'low', 'close']
        fx_data_df = pd.DataFrame()
        if len(fx_symbol) > 0:
            fx_data_df = get_dm_fx_hour_data(fx_symbol, self.symbol2bbg_dict.copy(), self.start, self.end, select_columns=select_columns)
        stock_symbol = list(filter(lambda x: self.symbol2instrument_type_dict[x] != InstrumentType.FX.value, self.symbol))
        stock_data_df = pd.DataFrame()
        if len(stock_symbol) > 0:
            # stock_data_df = get_dm_daily_data(stock_symbol, self.symbol2bbg_dict.copy(), self.symbol2back_up_bbg_dict.copy(), self.start, self.end,select_columns=select_columns)
            # todo get_stock_hour_function
            stock_data_df = pd.DataFrame()
        columns = list([f"{y}_{x}" for x in self.symbol for y in select_columns])
        data_df = pd.concat([fx_data_df, stock_data_df], axis=1,join='outer').fillna(method='ffill')
        data_df = data_df[columns]
        data_df = data_df.ffill(axis=0)
        DataArray = data_df.values.reshape(data_df.shape[0], len(self.symbol), -1)
        FxArray_time_df = pd.DataFrame(data_df.index)
        FxArray_time_df.rename(columns={0: 'date'}, inplace=True)
        FxArray_time_df['timestamp'] = FxArray_time_df['date'].apply(lambda x: datetime.timestamp(x))
        FxArray_time_df['end_timestamp'] = FxArray_time_df['date'].apply(lambda x: datetime.timestamp(x + timedelta(seconds=60 * 60)))
        FxArray_time_df['weekday'] = FxArray_time_df['date'].apply(lambda x: x.weekday())
        FxArray_time_df['year'] = FxArray_time_df['date'].apply(lambda x: x.year)
        FxArray_time_df['month'] = FxArray_time_df['date'].apply(lambda x: x.month)
        FxArray_time_df['day'] = FxArray_time_df['date'].apply(lambda x: x.day)
        FxArray_time_df['hour'] = FxArray_time_df['date'].apply(lambda x: x.hour)
        FxArray_time_df['minute'] = FxArray_time_df['date'].apply(lambda x: x.minute)
        FxArray_time_df['second'] = FxArray_time_df['date'].apply(lambda x: x.second)
        DatatimeArray = FxArray_time_df[['timestamp', 'end_timestamp', 'weekday', 'year', 'month', 'day', 'hour', 'minute', 'second']].values.astype(
            'uint64')

        t_all_out = datetime.now()
        self.logger.info(f'data download time:{t_all_out - t_all_in}')
        self.hour_queue.put((self.end, self.symbol, DataArray, DatatimeArray))
        self.logger.info(f'all hour data loaded, thread will quit')

    """ daily only hv fx and hk,cn equity """

    def start_load_daily(self, processes=1):
        """
            DataArray:shape(number of timepoint,number of symbol, enum constants.Data)
            DatatimeArray:shape(number of timepoint, enum constants.Time)
        """
        self.logger.debug(f'\n--------------loading daily data {self.start} to {self.end}-------------------')

        select_columns = ['open', 'high', 'low', 'close','volume']
        # select_columns = ['open', 'high', 'low', 'close']

        """ load self declared data from csv """
        data_store_symbols = list(filter(lambda x: self.symbol2source_dict[x] == "self", self.symbol))
        data_store_symbol_df_dict = {}
        for symbol in data_store_symbols:
            data_store_symbol_df_dict[symbol] = pd.read_csv(os.path.join(self.local_data_dir, symbol + ".csv"), index_col='date')
            data_store_symbol_df_dict[symbol].columns = [f"{x}_{symbol}" for x in select_columns]
            data_store_symbol_df_dict[symbol].index = pd.to_datetime(data_store_symbol_df_dict[symbol].index, format='%Y-%m-%d', utc=True)
            # data_store_symbol_df_dict[symbol].index = pd.to_datetime(data_store_symbol_df_dict[symbol].index, format='%m/%d/%Y', utc=True)

        """ load self data from dm """
        dm_symbols = list(filter(lambda x: self.symbol2source_dict[x] == "data_master", self.symbol))
        t_all_in = datetime.now()
        fx_symbol = list(filter(lambda x: self.symbol2instrument_type_dict[x] == InstrumentType.FX.value, dm_symbols))
        select_columns = ['open', 'high', 'low', 'close','volume']
        fx_data_df = pd.DataFrame()
        if len(fx_symbol) > 0:
            fx_data_df = get_dm_daily_data(fx_symbol, self.symbol2bbg_dict.copy(), self.symbol2back_up_bbg_dict.copy(), self.start, self.end,
                                           select_columns=select_columns)
        stock_symbol = list(filter(lambda x: self.symbol2instrument_type_dict[x] != InstrumentType.FX.value, dm_symbols))
        stock_data_df = pd.DataFrame()
        if len(stock_symbol) > 0:
            stock_data_df = get_dm_daily_data(stock_symbol, self.symbol2bbg_dict.copy(), self.symbol2back_up_bbg_dict.copy(), self.start, self.end,
                                              select_columns=select_columns,adjust_type=self.stock_data_type)

        data_df = pd.concat([fx_data_df, stock_data_df] + list(data_store_symbol_df_dict.values()), axis=1,join='outer')
        columns = list([f"{y}_{x}" for x in self.symbol for y in select_columns])
        data_df = data_df[columns]
        data_df = data_df.ffill(axis=0)
        data_df = data_df.fillna(0)
        DataArray = data_df.values.reshape(data_df.shape[0], len(self.symbol), -1)
        FxArray_time_df = pd.DataFrame(data_df.index)
        FxArray_time_df.rename(columns={0: 'date'}, inplace=True)
        FxArray_time_df['timestamp'] = FxArray_time_df['date'].apply(lambda x: datetime.timestamp(x))
        FxArray_time_df['end_timestamp'] = FxArray_time_df['date'].apply(lambda x: datetime.timestamp(x + timedelta(days=1) - timedelta(seconds=1)))
        FxArray_time_df['weekday'] = FxArray_time_df['date'].apply(lambda x: x.weekday())
        FxArray_time_df['year'] = FxArray_time_df['date'].apply(lambda x: x.year)
        FxArray_time_df['month'] = FxArray_time_df['date'].apply(lambda x: x.month)
        FxArray_time_df['day'] = FxArray_time_df['date'].apply(lambda x: x.day)
        FxArray_time_df['hour'] = 23
        FxArray_time_df['minute'] = 59
        FxArray_time_df['second'] = 59
        DatatimeArray = FxArray_time_df[['timestamp', 'end_timestamp', 'weekday', 'year', 'month', 'day', 'hour', 'minute', 'second']].values.astype('uint64')

        t_all_out = datetime.now()
        self.logger.info(f'data download time:{t_all_out - t_all_in}')
        self.daily_queue.put((self.end, self.symbol, DataArray, DatatimeArray))
        self.logger.info(f'all daily data loaded, thread will quit')

    ###............................................get fx ccy data(numpy) in dm,get np.ones if base_ccy = None............................................

    """ load fx data for ccy convert """

    def start_load_fx_ccy_minute(self, processes=1):
        """
            FxDataArray:shape(number of timepoint,number of ccy)
            DatatimeArray:shape(number of timepoint, enum constants.Time)
        """
        self.logger.debug(f'\n--------------loading fx ccy minute data {self.start} to {self.end}-------------------')
        t_all_in = datetime.now()
        FxDataArray, DatatimeArray = None, None
        if self.base_ccy in ["USD"]:
            FxDataArray, DatatimeArray = get_dm_ccy_fx_min_data(self.ccy, self.ccy2bbg_dict.copy(), self.ccy2trading_ccy_dict.copy(), self.start,
                                                             self.end)
        else:
            FxDataArray = np.ones([1, len(self.ccy)])
            DatatimeArray = np.zeros([1, 1])
            DatatimeArray[0][0] = datetime.timestamp(pd.to_datetime(str(self.start), format='%Y-%m-%d', utc=True))
        t_all_out = datetime.now()
        self.logger.info(f'ccy minute data download time:{t_all_out - t_all_in}')
        self.ccy_queue.put((FxDataArray, DatatimeArray))
        self.logger.info(f'all ccy minute data loaded, thread will quit')

    def start_load_fx_ccy_hour(self, processes=1):
        """
            FxDataArray:shape(number of timepoint,number of ccy)
            DatatimeArray:shape(number of timepoint, enum constants.Time)
        """
        self.logger.debug(f'\n--------------loading fx ccy hour data {self.start} to {self.end}-------------------')
        t_all_in = datetime.now()
        FxDataArray, DatatimeArray = None, None
        if self.base_ccy in ["USD"]:
            FxDataArray, DatatimeArray = get_dm_ccy_fx_hourly_data(self.ccy, self.ccy2bbg_dict.copy(), self.ccy2trading_ccy_dict.copy(), self.start,
                                                                self.end)
        else:
            FxDataArray = np.ones([1, len(self.ccy)])
            DatatimeArray = np.zeros([1, 1])
            DatatimeArray[0][0] = datetime.timestamp(pd.to_datetime(str(self.start), format='%Y-%m-%d', utc=True))
        t_all_out = datetime.now()
        self.logger.info(f'ccy hourly data download time:{t_all_out - t_all_in}')
        self.ccy_queue.put((FxDataArray, DatatimeArray))
        # self.ccy_queue.put('FINISH')
        self.logger.info(f'all ccy hourly data loaded, thread will quit')

    def start_load_fx_ccy_daily(self, processes=1):
        """
            FxDataArray:shape(number of timepoint,number of ccy)
            DatatimeArray:shape(number of timepoint, enum constants.Time)
        """
        self.logger.debug(f'\n--------------loading fx ccy daily data {self.start} to {self.end}-------------------')
        t_all_in = datetime.now()
        FxDataArray, DatatimeArray = None, None
        if self.base_ccy in ["USD", "HKD", "CNY", "CNH"]:
            FxDataArray, DatatimeArray = get_dm_ccy_fx_daily_data(self.ccy, self.ccy2bbg_dict.copy(), self.ccy2back_up_bbg_dict.copy()
                                                               , self.ccy2trading_ccy_dict.copy(), self.base_ccy, self.start, self.end)
        else:
            FxDataArray = np.ones([1, len(self.ccy)])
            DatatimeArray = np.zeros([1, 1])
            DatatimeArray[0][0] = datetime.timestamp(pd.to_datetime(str(self.start), format='%Y-%m-%d', utc=True))
        t_all_out = datetime.now()
        self.logger.info(f'fx ccy daily download time:{t_all_out - t_all_in}')
        self.ccy_queue.put((FxDataArray, DatatimeArray))
        # self.ccy_queue.put('FINISH')
        self.logger.info(f'all fx ccy daily loaded, thread will quit')

    ###............................................get dividend data(numpy) in dm,only hx fx,get np.zeros array for stocks ............................................
    """ load dividends data """

    def start_load_dividends_daily(self, processes=1):
        """
            RateArray:shape(number of timepoint,number of symbol)
            RatetimeArray:shape(number of timepoint, enum constants.Time)
        """
        select_columns = ["LAST","BID","ASK"]

        data_store_symbols = list(filter(lambda x: self.symbol2source_dict[x] == "self", self.symbol))
        data_store_symbol_df_dict = {}
        data_store_Rate_df = pd.DataFrame()
        for symbol in data_store_symbols:
            path = os.path.join(self.local_rate_data_dir, symbol + ".csv")
            if os.path.isfile(path):
                data_store_symbol_df_dict[symbol] = pd.read_csv(path, index_col='date')
                data_store_symbol_df_dict[symbol].columns = [f"{symbol}_{x}" for x in select_columns]
                data_store_symbol_df_dict[symbol].index = pd.to_datetime(data_store_symbol_df_dict[symbol].index, format='%Y-%m-%d', utc=True)
            else:
                column = [f"{symbol}_{x}" for x in select_columns]
                start_dt = pd.to_datetime(str(self.start), format='%Y-%m-%d', utc=True)
                data_store_symbol_df_dict[symbol] = pd.DataFrame({},columns=column,index=[start_dt])
        if len(data_store_symbols) > 0:
            data_store_Rate_df = pd.concat(list(data_store_symbol_df_dict.values()), axis=1,join='outer')
        

        dm_symbols = list(filter(lambda x: self.symbol2source_dict[x] == "data_master", self.symbol))
        self.logger.debug(f'\n--------------loading dividends daily data {self.start} to {self.end}-------------------')
        t_all_in = datetime.now()
        fx_symbol = list(filter(lambda x: self.symbol2instrument_type_dict[x] == InstrumentType.FX.value, dm_symbols))
        fx_symbol_index = list(map(lambda x: self.symbol.index(x), fx_symbol))
        #fx_RateArray, RatetimeArray = None, None
        Rate_df = pd.DataFrame()
        if len(fx_symbol) > 0:
            Rate_df = get_dm_fx_daily_dividends(fx_symbol,self.symbol2bbg_dict.copy(), self.start, self.end)

        non_fx_symbol = list(set(self.symbol) - set(data_store_symbol_df_dict.keys()) - set(fx_symbol))
        non_fx_Rate_df = pd.DataFrame()
        if len(non_fx_symbol) > 0:
            column = [f"{symbol}_{x}" for x in select_columns for symbol in non_fx_symbol]
            start_dt = pd.to_datetime(str(self.start), format='%Y-%m-%d', utc=True)
            non_fx_Rate_df = pd.DataFrame({},columns=column,index=[start_dt])
        
        data_df = pd.concat( [data_store_Rate_df, Rate_df,non_fx_Rate_df] , axis=1,join='outer')
        columns = list([f"{x}_{y}" for x in self.symbol for y in select_columns])
        data_df = data_df[columns]
        data_df = data_df.ffill(axis=0)
        data_df = data_df.fillna(0)

        RateArray = data_df.values.reshape(data_df.shape[0], len(self.symbol), -1)
        time_df = pd.DataFrame(data_df.index)
        time_df.rename(columns={0: 'date'}, inplace=True)
        time_df['timestamp'] = time_df['date'].apply(lambda x: datetime.timestamp(x))
        time_df['end_timestamp'] = time_df['date'].apply(lambda x: datetime.timestamp(x + timedelta(days=1)))
        time_df['weekday'] = time_df['date'].apply(lambda x: x.weekday())
        time_df['year'] = time_df['date'].apply(lambda x: x.year)
        time_df['month'] = time_df['date'].apply(lambda x: x.month)
        time_df['day'] = time_df['date'].apply(lambda x: x.day)
        time_df['hour'] = time_df['date'].apply(lambda x: x.hour)
        time_df['minute'] = time_df['date'].apply(lambda x: x.minute)
        time_df['second'] = time_df['date'].apply(lambda x: x.second)
        RatetimeArray = time_df[['timestamp','end_timestamp', 'weekday', 'year', 'month', 'day', 'hour', 'minute', 'second']].values
        RatetimeArray = RatetimeArray.astype('uint64')
        t_all_out = datetime.now()
        self.logger.info(f'dividends daily data download time:{t_all_out - t_all_in}')
        self.dividends_queue.put((RateArray, RatetimeArray))
        self.logger.info(f'all dividends daily data loaded, thread will quit')


''' ################################## ↓ snapshot ↓ ##################################
def get_snapshot_df(symbol, start, end, processes: int):
    t0 = time.time()
    if processes > 1:
        args = itertools.product(symbol, [start], [end])
        with Pool(processes) as pool:
            df_list = pool.map(_get_single_snapshot_data_star, args)
    else:
        df_list = []
        for sym in tqdm(symbol):
            df = get_single_snapshot_data(sym, start, end)
            df_list.append(df)
    res = pd.concat(df_list, axis=1)
    res.sort_index(inplace=True)
    res.reset_index(inplace=True)
    res.ffill(inplace=True)
    t1 = time.time()
    print(f'downloaded snapshot data of {symbol} from {start} to {end}, time usage {t1 - t0}')
    return res


def get_single_snapshot_data(symbol, start, end):
    """ 获取单symbol的数据，被get_snapshot_df调用 """
    select_columns = ['open', 'high', 'low', 'preclose'] + \
                     ['timestamp', 'last', 'totalVolumeTrade', 'totalValueTrade'] + \
                     [f'b{i}' for i in range(1, 11)] + [f'bv{i}' for i in range(1, 11)] + \
                     [f'a{i}' for i in range(1, 11)] + [f'av{i}' for i in range(1, 11)] + \
                     ['iopv', 'totalBidQty', 'totalAskQty']
    df = get_history_data(symbol, select_columns, 'snapshot', start, end)
    df = df.replace('', np.nan)
    df = df.astype(np.float64)
    price_col = df.columns.values.tolist()[8: 18] \
                + df.columns.values.tolist()[28: 38] \
                + [df.columns.values.tolist()[5]]
    df[price_col] = df[price_col].replace(0., np.nan).ffill()
    df['timestamp'] = df['timestamp'].astype(str).apply(lambda x: x[:10]).astype(np.float64)
    df = df[~df['timestamp'].duplicated('last')].reset_index(drop=True)
    df.columns = [f"{col}_{symbol}" if col not in ['timestamp'] else col for col in df.columns]
    df.set_index('timestamp', inplace=True)
    return df


def _get_single_snapshot_data_star(args):
    return get_single_snapshot_data(*args)


################################## ↓ minute ↓ ##################################
def get_minute_df(symbol, start, end, processes: int):
    t0 = time.time()
    if processes > 1:
        args = itertools.product(symbol, [start], [end])
        with Pool(processes) as pool:
            df_list = pool.map(_get_single_minute_data_star, args)
    else:
        df_list = []
        for sym in tqdm(symbol):
            df = get_single_minute_data(sym, start, end)
            df_list.append(df)
    res = pd.concat(df_list, axis=1)
    res.reset_index(inplace=True)
    res.ffill(inplace=True)
    t1 = time.time()
    print(f'downloaded minute data of {symbol} from {start} to {end}, time usage {t1 - t0}')
    return res


def get_single_minute_data(symbol, start, end):
    select_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'amount']
    df = get_history_data(symbol, select_columns, 'k_min', start, end)
    df = df.astype(np.float64)
    df.columns = [f"{col}_{symbol}" if col not in ['timestamp'] else col for col in df.columns]
    df.set_index('timestamp', inplace=True)
    return df


def _get_single_minute_data_star(arg):
    return get_single_minute_data(*arg)


################################## ↓ trade ↓ ##################################
def get_trade_df(symbol, start, end, processes: int):
    t0 = time.time()
    if processes > 1:
        args = itertools.product(symbol, [start], [end])
        with Pool(processes) as pool:
            df_list = pool.map(_get_single_trade_data_star, args)
    else:
        df_list = []
        for sym in tqdm(symbol):
            df = get_single_trade_data(sym, start, end)
            df_list.append(df)
    res = pd.concat(df_list, axis=1)
    price_col = [col for col in res.columns if 'price' in col]
    res[price_col] = res[price_col].ffill()
    res.fillna(0, inplace=True)
    res.sort_index(inplace=True)
    res.reset_index(inplace=True)
    res['timestamp'] = res['timestamp'].apply(np.floor)
    t1 = time.time()
    print(f'downloaded trade data of {symbol} from {start} to {end}, time usage {t1 - t0}')
    return res


def get_single_trade_data(symbol, start, end):
    select_columns = ['timestamp', 'price', 'side', 'volume']
    df = get_history_data(symbol, select_columns, 'trade', start, end)
    df['side'] = df['side'].map({'S': 1, 'B': 2})
    df = df.astype(np.float64)
    df['timestamp'] = df['timestamp'] + (df.groupby('timestamp').cumcount() / 10000)
    df.columns = [f"{col}_{symbol}" if col not in ['timestamp'] else col for col in df.columns]
    df.set_index('timestamp', inplace=True)
    return df


def _get_single_trade_data_star(arg):
    return get_single_trade_data(*arg)


################################## ↓ daily ↓ ##################################
def get_daily_df(symbol, start, end, processes: int):
    t0 = time.time()
    if processes > 1:
        args = itertools.product(symbol, [start], [end])
        with Pool(processes) as pool:
            df_list = pool.map(_get_single_daily_data_star, args)
    else:
        df_list = []
        for sym in tqdm(symbol):
            df = get_single_daily_data(sym, start, end)
            df_list.append(df)
    res = pd.concat(df_list, axis=1)
    res = res.ffill()
    res.reset_index(inplace=True)
    t1 = time.time()
    print(f'downloaded daily data of {symbol} from {start} to {end}, time usage {t1 - t0}')
    return res


def get_single_daily_data(symbol, start, end):
    select_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'amount']
    df = get_history_data(symbol, select_columns, 'k_day', start, end)
    df = df.astype(np.float64)
    df.columns = [f"{col}_{symbol}" if col not in ['timestamp'] else col for col in df.columns]
    df.set_index('timestamp', inplace=True)
    return df


def _get_single_daily_data_star(arg):
    return get_single_daily_data(*arg) '''


################################## ↓ fx ↓ ##################################

def fx_init():
    dm_client.refresh_config()
    dm_client.start()


def get_dm_ccy_fx_min_data(ccy, ccy2bbg_dict, ccy2trading_ccy_dict, start, end):
    all_ccy_non_usd = set(filter(lambda x: x != 'USD', ccy))
    ccy2bbgticker = dict(map(lambda x: (x, ccy2bbg_dict[x]), all_ccy_non_usd))
    bbgticker2ccy = dict(map(lambda x: (x[1], x[0]), ccy2bbgticker.items()))
    bbgtickers = list(bbgticker2ccy.keys())
    res = dm_client.get_minute_data_multi_thread(symbols=','.join(bbgtickers), start_date=start.strftime('%Y-%m-%d'),
                                                 end_date=str(end.date() + timedelta(days=2)), fields=['close'],
                                                 fill_method='ffill', threads=2)
    FX_AGAINST_USD_df = pd.DataFrame({bbgticker2ccy[k]: v['close'] for k, v in res.items()})
    FX_AGAINST_USD_df = FX_AGAINST_USD_df[list(all_ccy_non_usd)]
    FX_AGAINST_USD_df.fillna(method='ffill')
    FX_AGAINST_USD_df.index = pd.to_datetime(FX_AGAINST_USD_df.index, format='%Y-%m-%dT%H:%M:%SZ', utc=True)
    for k in FX_AGAINST_USD_df:
        ticker = ccy2trading_ccy_dict[k]
        if ticker  != 'USD':
            FX_AGAINST_USD_df[k] = 1 / FX_AGAINST_USD_df[k]
    if 'USD' in ccy:
        FX_AGAINST_USD_df['USD'] = 1

    utc_start = pd.to_datetime(start, format='%Y-%m-%d', utc=True)
    utc_end = pd.to_datetime(end, format='%Y-%m-%d', utc=True)
    FX_AGAINST_USD_df = FX_AGAINST_USD_df.ffill(axis=0)
    FX_AGAINST_USD_df = FX_AGAINST_USD_df.loc[utc_start: utc_end, ccy]
    FxDataArray = FX_AGAINST_USD_df.values.reshape(FX_AGAINST_USD_df.shape[0], -1)

    Time = pd.DataFrame(FX_AGAINST_USD_df.index)
    Time.columns = ['date']
    Time['timestamp'] = Time['date'].apply(lambda x: datetime.timestamp(x))
    Time['end_timestamp'] = Time['date'].apply(lambda x: datetime.timestamp(x + timedelta(seconds=60)))
    Time['weekday'] = Time['date'].apply(lambda x: x.weekday())
    Time['year'] = Time['date'].apply(lambda x: x.year)
    Time['month'] = Time['date'].apply(lambda x: x.month)
    Time['day'] = Time['date'].apply(lambda x: x.day)
    Time['hour'] = Time['date'].apply(lambda x: x.hour)
    Time['minute'] = Time['date'].apply(lambda x: x.minute)
    Time['second'] = Time['date'].apply(lambda x: x.second)
    DatatimeArray = Time[['timestamp', 'weekday', 'year', 'month', 'day', 'hour', 'minute', 'second']].values

    return FxDataArray, DatatimeArray


def get_dm_ccy_fx_hourly_data(ccy, ccy2bbg_dict, ccy2trading_ccy_dict, start, end):
    # prepare df
    all_ccy_non_usd = set(filter(lambda x: x != 'USD', ccy))
    ccy2bbgticker = dict(map(lambda x: (x, ccy2bbg_dict[x]), all_ccy_non_usd))
    bbgticker2ccy = dict(map(lambda x: (x[1], x[0]), ccy2bbgticker.items()))

    bbgtickers = list(bbgticker2ccy.keys())
    res = {}
    for bbgticker in bbgtickers:
        res.update(dm_client.get_minute_data_multi_thread(symbols=','.join([bbgticker]), start_date=start.strftime('%Y-%m-%d'),
                                                    end_date=str(end.date() + timedelta(days=2)), fields=['close'],
                                                    fill_method='ffill', threads=2))

    for key in res.keys():
        res[key].index = pd.DatetimeIndex(res[key].index)
        close = res[key]['close'].resample('60min').last()
        res[key] = pd.DataFrame({'close': close}).dropna(axis=0, how='all')

    FX_AGAINST_USD_df = pd.DataFrame({bbgticker2ccy[k]: v['close'] for k, v in res.items()})
    FX_AGAINST_USD_df = FX_AGAINST_USD_df[list(all_ccy_non_usd)]
    FX_AGAINST_USD_df.fillna(method='ffill')
    FX_AGAINST_USD_df.index = pd.to_datetime(FX_AGAINST_USD_df.index, format='%Y-%m-%dT%H:%M:%SZ', utc=True)
    for k in FX_AGAINST_USD_df:
        ticker = ccy2trading_ccy_dict[k]
        if ticker != 'USD':
            FX_AGAINST_USD_df[k] = 1 / FX_AGAINST_USD_df[k]
    if 'USD' in ccy:
        FX_AGAINST_USD_df['USD'] = 1
    utc_start = pd.to_datetime(start, format='%Y-%m-%d', utc=True)
    utc_end = pd.to_datetime(end, format='%Y-%m-%d', utc=True)
    FX_AGAINST_USD_df = FX_AGAINST_USD_df.loc[utc_start: utc_end, ccy]
    FX_AGAINST_USD_df = FX_AGAINST_USD_df.ffill(axis=0)
    # prepare fx_usd data
    FxDataArray = FX_AGAINST_USD_df.values.reshape(FX_AGAINST_USD_df.shape[0], -1)

    # prepare fx_usd time
    Time = pd.DataFrame(FX_AGAINST_USD_df.index)
    Time.columns = ['date']
    Time['timestamp'] = Time['date'].apply(lambda x: datetime.timestamp(x))
    Time['end_timestamp'] = Time['date'].apply(lambda x: datetime.timestamp(x + timedelta(seconds=3600)))
    Time['weekday'] = Time['date'].apply(lambda x: x.weekday())
    Time['year'] = Time['date'].apply(lambda x: x.year)
    Time['month'] = Time['date'].apply(lambda x: x.month)
    Time['day'] = Time['date'].apply(lambda x: x.day)
    Time['hour'] = Time['date'].apply(lambda x: x.hour)
    Time['minute'] = Time['date'].apply(lambda x: x.minute)
    Time['second'] = Time['date'].apply(lambda x: x.second)

    DatatimeArray = Time[['timestamp', 'weekday', 'year', 'month', 'day', 'hour', 'minute', 'second']].values

    return FxDataArray, DatatimeArray


def get_dm_ccy_fx_daily_data(ccy, ccy2bbg_dict, ccy2back_up_bbg_dict, ccy2trading_ccy_dict, base_ccy, start, end):
    if len(set(ccy)) != len(ccy):
        raise Exception(f"{ccy} hv repeat ccy")
    if base_ccy not in ["USD", "HKD", "CNY", "CNH"]:
        raise Exception(f"base_ccy:{base_ccy} is not allowed")
    all_ccy_non_usd = list(filter(lambda x: x != 'USD', ccy))
    utc_start = pd.to_datetime(start, format='%Y-%m-%d', utc=True)
    utc_end = pd.to_datetime(end, format='%Y-%m-%d', utc=True)

    if len(all_ccy_non_usd) != 0 :
        ccy2bbgticker = dict(map(lambda x: (x, ccy2bbg_dict[x]), all_ccy_non_usd))
        bbgticker2ccy = dict(map(lambda x: (x[1], x[0]), ccy2bbgticker.items()))
        bbgtickers = list(bbgticker2ccy.keys())
        result = dm_client.get_historical_data(symbols=list(bbgtickers), start_date=str(start.date()), end_date=str(end.date())
                                            , fields=['close'], fill_method='ffill')
        FX_AGAINST_USD_df = pd.concat(list(map(lambda x: pd.Series(list(map(lambda y: y[int(result['fields'].index('close'))], x[1]))
                                                                , name=bbgticker2ccy[x[0]],
                                                                index=list(map(lambda y: y[int(result['fields'].index('date'))], x[1])))
                                            , result['values'].items())), axis=1, sort=True)
        FX_AGAINST_USD_df.index = pd.to_datetime(FX_AGAINST_USD_df.index, format='%Y-%m-%d', utc=True)

        ccy2bbgticker_backup = dict(map(lambda x: (x, ccy2back_up_bbg_dict[x]), all_ccy_non_usd))
        bbgticker2ccy_backup = dict(map(lambda x: (x[1], x[0]), ccy2bbgticker_backup.items()))
        bbgtickers_backup = list(bbgticker2ccy_backup.keys())
        result_backup = dm_client.get_historical_data(symbols=list(bbgtickers_backup), start_date=str(start.date()),
                                                    end_date=str(end.date()), fields=['close'], fill_method='ffill')
        FX_AGAINST_USD_df_backup = pd.concat(
            list(map(lambda x: pd.Series(list(map(lambda y: y[int(result_backup['fields'].index('close'))], x[1])),
                                        name=bbgticker2ccy_backup[x[0]],
                                        index=list(map(lambda y: y[int(result_backup['fields'].index('date'))], x[1]))),
                    result_backup['values'].items())), axis=1, sort=True)
        FX_AGAINST_USD_df_backup.index = pd.to_datetime(FX_AGAINST_USD_df_backup.index, format='%Y-%m-%d', utc=True)

        join_FX_AGAINST_USD_df = FX_AGAINST_USD_df.join(FX_AGAINST_USD_df_backup, rsuffix='_back_up', how='outer')
        for ticker in FX_AGAINST_USD_df:
            for i in range(len(join_FX_AGAINST_USD_df[ticker])):
                if not join_FX_AGAINST_USD_df[ticker][i] or math.isnan(join_FX_AGAINST_USD_df[ticker][i]):
                    join_FX_AGAINST_USD_df[ticker][i] = join_FX_AGAINST_USD_df[ticker + '_back_up'][i]
        join_FX_AGAINST_USD_df = join_FX_AGAINST_USD_df[list(all_ccy_non_usd)]
    else:
        join_FX_AGAINST_USD_df = pd.DataFrame({},index=[utc_start])

    if base_ccy != None and base_ccy in CCY_INDEX:
        ccy_index = CCY_INDEX[base_ccy]
        result = dm_client.get_historical_data(symbols=list([ccy_index]), start_date=str(start.date()), end_date=str(end.date()), fields=['close'],
                                               fill_method='ffill')
        df = pd.DataFrame(list(result['values'].values())[0], columns=['date', 'close'])
        df.index = df['date']
        df.index = pd.to_datetime(df.index, format='%Y-%m-%d', utc=True)
        del df['date']
        join_FX_AGAINST_USD_df = join_FX_AGAINST_USD_df.join(df, rsuffix=f'_{base_ccy}', how='outer')
    else:
        join_FX_AGAINST_USD_df['close'] = 1
    join_FX_AGAINST_USD_df.fillna(method='ffill')
    base_ccy_FX_df = join_FX_AGAINST_USD_df['close']

    join_FX_AGAINST_USD_df = join_FX_AGAINST_USD_df[list(all_ccy_non_usd)]
    join_FX_AGAINST_USD_df.index = pd.to_datetime(join_FX_AGAINST_USD_df.index, format='%Y-%m-%d')
    for k in join_FX_AGAINST_USD_df:
        ticker = ccy2trading_ccy_dict[k]
        if ticker != 'USD':
            join_FX_AGAINST_USD_df[k] = 1 / join_FX_AGAINST_USD_df[k] * base_ccy_FX_df
    if 'USD' in ccy:
        join_FX_AGAINST_USD_df['USD'] = 1 * base_ccy_FX_df

    join_FX_AGAINST_USD_df = join_FX_AGAINST_USD_df.loc[utc_start: utc_end, ccy]
    join_FX_AGAINST_USD_df = join_FX_AGAINST_USD_df.ffill(axis=0)
    # prepare fx_usd data
    FxDataArray = join_FX_AGAINST_USD_df.values.reshape(join_FX_AGAINST_USD_df.shape[0], -1)

    # prepare fx_usd time
    time_df = pd.DataFrame(join_FX_AGAINST_USD_df.index)
    time_df.rename(columns={0: 'date'}, inplace=True)
    time_df['timestamp'] = time_df['date'].apply(lambda x: datetime.timestamp(x))
    time_df['end_timestamp'] = time_df['date'].apply(lambda x: datetime.timestamp(x + timedelta(days=1)))
    time_df['weekday'] = time_df['date'].apply(lambda x: x.weekday())
    time_df['year'] = time_df['date'].apply(lambda x: x.year)
    time_df['month'] = time_df['date'].apply(lambda x: x.month)
    time_df['day'] = time_df['date'].apply(lambda x: x.day)
    time_df['hour'] = time_df['date'].apply(lambda x: x.hour)
    time_df['minute'] = time_df['date'].apply(lambda x: x.minute)
    time_df['second'] = time_df['date'].apply(lambda x: x.second)

    DatatimeArray = time_df[['timestamp', 'weekday', 'year', 'month', 'day', 'hour', 'minute', 'second']].values


    return FxDataArray, DatatimeArray


def get_dm_fx_min_data(symbols, symbol2bbg_dict, start, end, adjust_type=0, select_columns=['open', 'high', 'low', 'close']):
    # 1、先读取汇率的数据
    bbg_tickers = list(map(lambda x: symbol2bbg_dict[x], symbols))
    result = dm_client.get_minute_data_multi_thread(symbols=','.join(bbg_tickers),
                                                    start_date=start.strftime('%Y-%m-%d'),
                                                    end_date=(end + timedelta(days=2)).strftime('%Y-%m-%d'),
                                                    fields=select_columns, fill_method='ffill',
                                                    threads=2)
    for symbol in symbols:
        ticker = symbol2bbg_dict[symbol]
        df = result[ticker]
        df.index = pd.to_datetime(df.index, format='%Y-%m-%dT%H:%M:%SZ', utc=True)
        df.index.name = 'date'
        df = df[select_columns]
        df.columns = [f"{f}_{symbol}" for f in df.columns]
        df = df.astype(np.float64)

        if "res" not in locals():
            res = df
        else:
            res = res.join(df, how='outer')
    del df
    res.sort_index(inplace=True)
    res = res.ffill(axis=0)
    
    # 将res的内容转换成array
    all_column = []
    for sym in symbols:
        sym_column = [col + f"_{sym}" for col in select_columns]
        all_column += sym_column
    utc_start = pd.to_datetime(start, format='%Y-%m-%d', utc=True)
    utc_end = pd.to_datetime(end, format='%Y-%m-%d', utc=True)
    res_data = res[all_column].loc[utc_start: utc_end]
    return res_data


def get_dm_fx_hour_data(symbols, symbol2bbg_dict, start, end, adjust_type=0, select_columns=['open', 'high', 'low', 'close']):
    bbg_tickers = list(map(lambda x: symbol2bbg_dict[x], symbols))
    result = {}
    for bbg_ticker in bbg_tickers:
        result.update( dm_client.get_minute_data_multi_thread(symbols=','.join([bbg_ticker]),
                                                        start_date=start.strftime('%Y-%m-%d'),
                                                        end_date=(end + timedelta(days=2)).strftime('%Y-%m-%d'),
                                                        fields=['open', 'high', 'low', 'close'], fill_method='ffill',
                                                        threads=2)  )
    for symbol in symbols:
        ticker = symbol2bbg_dict[symbol]
        result[ticker].index = pd.to_datetime(result[ticker].index, format='%Y-%m-%dT%H:%M:%SZ', utc=True)
        df_dict = {}
        for col in select_columns:
            df_dict[col] = result[ticker][col].resample('60min')
            if col == "open":
                df_dict[col] = df_dict[col].first()
            elif col == "high":
                df_dict[col] = df_dict[col].max()
            elif col == "low":
                df_dict[col] = df_dict[col].min()
            elif col == "close":
                df_dict[col] = df_dict[col].last()
        result[ticker] = pd.DataFrame(df_dict).dropna(axis=0, how='all')
        # result[key].to_csv("result/check_hour_data_"+str(key))

    for symbol in symbols:
        ticker = symbol2bbg_dict[symbol]
        df = result[ticker]
        df.index = pd.to_datetime(df.index, format='%Y-%m-%dT%H:%M:%SZ', utc=True)
        df.index.name = 'date'
        df = df[select_columns]
        df.columns = [f"{f}_{symbol}" for f in df.columns]
        df = df.astype(np.float64)
        if "res" not in locals():
            res = df
        else:
            res = res.join(df, how='outer')
    del df
    res.sort_index(inplace=True)
    res = res.ffill(axis=0)
    # 将res的内容转换成array
    all_column = []
    for sym in symbols:
        sym_column = [col + f"_{sym}" for col in select_columns]
        all_column += sym_column
    utc_start = pd.to_datetime(start, format='%Y-%m-%d', utc=True)
    utc_end = pd.to_datetime(end, format='%Y-%m-%d', utc=True)
    res_data = res[all_column].loc[utc_start: utc_end]
    return res_data


def get_dm_daily_data(symbols, symbol2bbg_dict, symbol2back_up_bbg_dict, start, end, adjust_type = 0, select_columns=['open', 'high', 'low', 'close','volume']):


    batch = DATA_LOADER_DAILY_DATA_BATCH
    result = {}
    result_backup = {}
    number_of_batch = int(len(symbols)/batch) +1

    
    for i in range(number_of_batch):
        temp_symbols = symbols[i*batch:min((i+1)*batch,len(symbols))]
        if len(temp_symbols)==0:
            continue

        bbg_tickers = list(map(lambda x: symbol2bbg_dict[x], temp_symbols))
        temp_res = dm_client.get_historical_data(symbols=bbg_tickers, start_date=start.strftime('%Y-%m-%d'), end_date=end.strftime('%Y-%m-%d'),
                                            fields=select_columns, to_dataframe=True, fill_method='ffill', adjust_type=adjust_type)
        result.update(temp_res)

        bbg_tickers_backup = list(map(lambda x: symbol2back_up_bbg_dict[x], temp_symbols))



        bbg_tickers_backup = list(filter(lambda x: not pd.isnull(x),bbg_tickers_backup))
        if len(bbg_tickers_backup) > 0 :
            temp_res_backup = dm_client.get_historical_data(symbols=bbg_tickers_backup, start_date=start.strftime('%Y-%m-%d'),
                                                        end_date=end.strftime('%Y-%m-%d'),
                                                        fields=select_columns, to_dataframe=True, fill_method='ffill', adjust_type=adjust_type)
            result_backup.update(temp_res_backup)
    
    join_df_list = []
    for symbol in symbols:

        ticker = symbol2bbg_dict[symbol]
        df = result[ticker]
        df = df.set_index('date')
        df.index = pd.to_datetime(df.index, format='%Y-%m-%d', utc=True)
        df.index.name = 'date'
        #df.to_csv(f"{symbol}.csv")
        
        ticker_back_up = symbol2back_up_bbg_dict[symbol]
        if not pd.isnull(ticker_back_up):
            df_backup = result_backup[ticker_back_up]
            df_backup = df_backup.set_index('date')
            df_backup.index = pd.to_datetime(df_backup.index, format='%Y-%m-%d', utc=True)
            df_backup.index.name = 'date'

            join_df = df.join(df_backup, rsuffix='_back_up', how='outer')

            for col in select_columns:
                for i in range(len(join_df.index)):
                    back_up_val = join_df[f'{col}_back_up'][i]
                    val = join_df[col][i]
                    close_val = join_df["close"][i]
                    back_up_close_val = join_df["close_back_up"][i]
                    if (not val or math.isnan(val)):
                        if (back_up_val and not math.isnan(back_up_val)):
                            join_df[col][i] = back_up_val
                        elif (close_val and not math.isnan(close_val)):
                            join_df[col][i] = close_val
                        elif (back_up_close_val and not math.isnan(back_up_close_val)):
                            join_df[col][i] = back_up_close_val
                #join_df[f"{col}_{symbol}"] = join_df[col]
                #del join_df[col]
                
            for col in select_columns:
                del join_df[f'{col}_back_up']
        else:
            join_df = df
        join_df.columns = list(map(lambda x: f"{x}_{symbol}",join_df.columns))
        join_df = join_df.astype(np.float64)
        join_df_list.append(join_df)

    res = pd.concat(join_df_list, axis=1,join='outer')
    del join_df
    del join_df_list
    res.sort_index(inplace=True)
    res = res.ffill(axis=0)

    # df to array
    # 1)trading data
    res_column = []
    for symbol in symbols:
        sym_column = [col + f"_{symbol}" for col in select_columns]
        res_column += sym_column
    utc_start = pd.to_datetime(start, format='%Y-%m-%d', utc=True)
    utc_end = pd.to_datetime(end, format='%Y-%m-%d', utc=True)
    res_data = res[res_column].loc[utc_start: utc_end]
    return res_data


def get_dm_fx_daily_dividends(symbols,symbol2bbg_dict, start, end):

    temp_symbols = list(map(lambda x: symbol2bbg_dict[x].split(' ')[0],symbols))
    all_ccy = list(sorted(set(reduce(lambda x, y: x + y, [ fx_bbg_dividends_ccy_map.get(symbol,[symbol[:3],symbol[3:] ] )  for symbol in temp_symbols ]  ))))

    ccy_matrix_df = pd.DataFrame(np.zeros((len(all_ccy), len(temp_symbols))), index=all_ccy, columns=temp_symbols)
    for symbol in temp_symbols:
        left_ccy,right_ccy = fx_bbg_dividends_ccy_map.get(symbol,[symbol[:3],symbol[3:] ] )
        ccy_matrix_df.loc[left_ccy][symbol] = 1
        ccy_matrix_df.loc[right_ccy][symbol] = -1

    # 3、读取currency的 interest rate
    all_ccy_non_usd = set(filter(lambda x: x != 'USD', all_ccy))
    all_spot = all_ccy_non_usd.intersection(set(SPOT))
    all_ndf = all_ccy_non_usd.intersection(set(NDF))
    df_fp = pd.DataFrame()
    df_tn = pd.DataFrame()
    if all_spot:
        df_tn = tn_to_rate(all_spot, start, end + timedelta(days=1))
    if all_ndf:
        df_fp = fp_to_rate((all_ndf), start, end + timedelta(days=1))
    df_usd = usd_to_rate(start, end + timedelta(days=1))
    FX_ON_RATE_df_BASE = pd.concat([df_fp, df_tn, df_usd], axis=1)

    FX_ON_RATE_df_BASE = FX_ON_RATE_df_BASE.fillna(method='ffill')
    Rate_df_list = []
    ccy_matrix = ccy_matrix_df.values
    for field in ['BID', 'ASK', 'LAST']:
        all_ccy_column = [f'{ccy}_{field}' for ccy in all_ccy]
        FX_ON_RATE_df = FX_ON_RATE_df_BASE[all_ccy_column]
        Rate_value = np.matmul(FX_ON_RATE_df.values, ccy_matrix)[1:]
        time_index = FX_ON_RATE_df.index[:-1]
        FX_ON_RATE_df = pd.DataFrame(Rate_value, index = time_index,
                                     columns=[f'{sym}_{field}' for sym in symbols])

        Rate_df_list.append(FX_ON_RATE_df)
    Rate_df = pd.concat(Rate_df_list, axis=1).fillna(0)
    Rate_df.index = pd.to_datetime(Rate_df.index, format='%Y-%m-%d', utc=True)
    Rate_df = Rate_df.ffill(axis=0)


    # 3)rate data
    rate_column = []
    rate_select = ['LAST', 'BID', 'ASK']
    rate_column = [f"{sym}_{field}"  for sym in symbols for field in rate_select ]
    Rate_df = Rate_df[rate_column]
    

    # 3)rate data time
    time_df = pd.DataFrame(Rate_df.index)
    time_df.rename(columns={0: 'date'}, inplace=True)
    time_df['timestamp'] = time_df['date'].apply(lambda x: datetime.timestamp(x))
    time_df['end_timestamp'] = time_df['date'].apply(lambda x: datetime.timestamp(x + timedelta(days=1)))
    time_df['weekday'] = time_df['date'].apply(lambda x: x.weekday())
    time_df['year'] = time_df['date'].apply(lambda x: x.year)
    time_df['month'] = time_df['date'].apply(lambda x: x.month)
    time_df['day'] = time_df['date'].apply(lambda x: x.day)
    time_df['hour'] = time_df['date'].apply(lambda x: x.hour)
    time_df['minute'] = time_df['date'].apply(lambda x: x.minute)
    time_df['second'] = time_df['date'].apply(lambda x: x.second)
    DatatimeArray = time_df[['timestamp','end_timestamp', 'weekday', 'year', 'month', 'day', 'hour', 'minute', 'second']].values



    time_index_weekday = (time_df['weekday'].values == 2).astype(int)
    rate_multiple = ( time_index_weekday*2 + np.ones(len(time_index_weekday)) ) / 36000
    for col in Rate_df.columns:
        Rate_df[col] = Rate_df[col]*rate_multiple

    return Rate_df
