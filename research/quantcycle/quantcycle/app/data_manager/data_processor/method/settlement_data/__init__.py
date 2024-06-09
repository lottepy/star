import pandas as pd

from ..method_base import MethodBase
from ....utils.timestamp_manipulation import string2timestamp, timestamp2datetimestring
from ....utils.csv_import import backup_mnemonic_from_info_df, get_csv_info_df
from quantcycle.app.data_manager.data_loader.data_center.mapping import EXCHANGE_CALENDAR_MAPPING, EXCHANGE_TRADE_TIME_MAPPING


class MethodSETTLEMENT(MethodBase):
    '''
        ENUM: 
        1 -> close
        0 -> open
    '''
    
    def __init__(self):
        super().__init__()
        self.is_final_step = False
        self.output_data_type = 'df'

    def create_data_mapping(self, data_bundle):
        self.symbols = data_bundle["Symbol"]
        data_source = data_bundle["SymbolArgs"].get("DataSource",'')
        self.settlement_type = data_bundle["SymbolArgs"].get("SettlementType",'close')
        dc_args = data_bundle["DataCenterArgs"]
        info_df = get_csv_info_df(dc_args.get("info",''))
        self.symbol_mnemonic = [i+" "+data_source if data_source else i for i in self.symbols]
        self.symbol_mnemonic = backup_mnemonic_from_info_df(self.symbols, self.symbol_mnemonic, info_df)
        self.freq = data_bundle['Frequency']
        self.slot = data_bundle['Slot']
        self.time = (data_bundle['StartTS'],data_bundle['EndTS'])
        # self.new_data_source = self.data_source+"+"+self.backup_data_source
        for symbol in self.symbol_mnemonic:
            self.data_mapping[symbol +
                              "/data"] = f"{symbol}/data"
            self.data_mapping[symbol +
                              "/info"] = f"{symbol}/info"

    def run(self):
        data = {}
        for symbol in self.symbol_mnemonic:
            data[symbol + "/data"] = self._format_data(self.data_dict[symbol + "/data"], self.data_dict[symbol + "/info"])
        return data

    def _format_data(self, df, info):
        trade_time = info['trade_time']
        timezone = info['timezone']
        if self.freq.upper() == 'DAILY':
            df = self._format_daily_data(df, trade_time, timezone)
        else:
            df = self._format_hourly_data(df, trade_time, timezone)
        return df

    def _format_daily_data(self, df, trade_time, timezone):
        if self.settlement_type.lower() == "close":
            df['field'] = 1  # 1 -> close
            df = df[['close','field']]
            df.columns = ['data','field']
            return df
        c = df[['close']]
        c['field'] = 1
        o = df[['open']]
        o['field'] = 0
        c.columns = ['data','field']
        o.columns = ['data','field']
        o.index = timestamp2datetimestring(o.index, timezone)
        day_open_time = trade_time[0].split('-')[0]
        day_close_time = trade_time[-1].split('-')[-1]
        o.index = [f'{d.split(" ")[0]} {day_open_time}' for d in o.index]
        _format_timeindex(o, timezone)
        if day_open_time == day_close_time:
            o.index = o.index - 1
        if len(o) != 0 and len(c) != 0: 
            df = o.append(c).sort_values(["timestamp","field"])
        else:
            df = o
        return df
    
    def _format_hourly_data(self, df, trade_time, timezone):
        if self.settlement_type.lower() == "close":
            df['field'] = 1  # 1 -> close
            df = df[['close','field']]
            df.columns = ['data','field']
            return df
        start_t = trade_time[0].split('-')[0]
        end_t = trade_time[-1].split('-')[-1]
        c = df[['close']]
        c['field'] = 1
        df.index = pd.to_datetime(timestamp2datetimestring(df.index, timezone))
        df.index.name = 'timestamp'
        df['realtime'] = df.index
        o = df.resample('D').first().dropna()
        o.index = o.realtime
        o = o[['open']]
        o['field'] = 0
        c.columns = ['data','field']
        o.columns = ['data','field']

        if trade_time == EXCHANGE_TRADE_TIME_MAPPING["ALL_TRADABLE"].split(',')[:-1]:
            _format_timeindex(o, timezone)
        else:
            day_open_time = trade_time[0].split('-')[0]
            day_close_time = trade_time[-1].split('-')[-1]
            o.index = [f'{d.strftime("%Y-%m-%d")} {day_open_time}' for d in o.index]
            _format_timeindex(o, timezone)
            if day_open_time == day_close_time:
                o.index = o.index - 1
        if len(o) != 0 and len(c) != 0: 
            df = o.append(c).sort_values(["timestamp","field"])
        else:
            df = o

        return df

def _format_timeindex(df, timezone: str = 'UTC'):
    '''
        transform the original result from dm to desired format
        support timezone: 'HKT'
    '''

    df.index = string2timestamp(df.index, timezone)
    df.index.name = 'timestamp'
    return df