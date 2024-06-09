import pandas as pd
import numpy as np
import datetime as dt
import pytz

from ..method_base import MethodBase
from ....utils.timestamp_manipulation import string2timestamp
from ....utils.csv_import import backup_mnemonic_from_info_df, get_csv_info_df
from quantcycle.app.data_manager.data_loader.data_center.mapping import EXCHANGE_CALENDAR_MAPPING, EXCHANGE_TRADE_TIME_MAPPING


class MethodTRADABLE(MethodBase):
    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'df'

    def create_data_mapping(self, data_bundle):
        self.symbols = data_bundle["Symbol"]
        self.end_ts = data_bundle["EndTS"]
        self.settlement_type = data_bundle.get("SymbolArgs",{}).get("SettlementType",'close')
        data_source = data_bundle["SymbolArgs"].get("DataSource",'')
        dc_args = data_bundle["DataCenterArgs"]
        info_df = get_csv_info_df(dc_args.get("info",''))
        self.symbol_mnemonic = [i+" "+data_source if data_source else i for i in self.symbols]
        self.symbol_mnemonic = backup_mnemonic_from_info_df(self.symbols, self.symbol_mnemonic, info_df)
        self.freq = data_bundle['Frequency']
        self.slot = "tradable_table"
        self.time = (data_bundle['StartTS'],data_bundle['EndTS'])
        # self.new_data_source = self.data_source+"+"+self.backup_data_source
        for symbol in self.symbol_mnemonic:
            self.data_mapping[symbol +
                              " info"] = f"{symbol}/info"

    def run(self):
        exchange_calendar_map = {info['list_exchange']:info['exchange_calendar'] for info in self.data_dict.values()}
        trade_time_map = {info['list_exchange']:info['trade_time'] for info in self.data_dict.values()}
        time_zone_map = {info['list_exchange']:info['timezone'] for info in self.data_dict.values()}
        exchange2sym = [self.data_dict[sym+' info']['list_exchange'] for sym in self.symbol_mnemonic]
        all_exchanges = list(exchange_calendar_map.keys())
        if self.freq.upper() == 'DAILY':
            res_df = self._daily(exchange_calendar_map, all_exchanges, exchange2sym, trade_time_map, time_zone_map)
        else:
            res_df = self._intraday(exchange_calendar_map, all_exchanges, exchange2sym, trade_time_map, time_zone_map)
        return res_df, self.symbols, ['tradable_table']

    def _daily(self, exchange_calendar_map, all_exchanges, exchange2sym, trade_time_map, time_zone_map):
        list_df = []
        time_str = [dt.datetime.fromtimestamp(int(ts), pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z') for ts in self.time]
        dummy_df = pd.DataFrame(index=[time_str[0],time_str[1]], columns=['dummy'])
        dummy_df.index = pd.to_datetime(dummy_df.index)
        dummy_df = _format_timeindex(dummy_df.resample('1D').last())
        list_df.append(dummy_df)
        for ex in all_exchanges:
            if ex =='ALL_TRADABLE':
                # hardcode for daily FX day close time 
                time_info = EXCHANGE_TRADE_TIME_MAPPING["ALL_TRADABLE"]
                day_open_time, day_close_time, time_zone = time_info.split(',')[0].split('-')[0], time_info.split(',')[0].split('-')[-1], time_info.split(',')[1]
                time_index = [f'{d.strftime("%Y-%m-%d")} {day_close_time}' for d in pd.date_range(start=time_str[0],end=time_str[1])]
                # if self.settlement_type == 'open':
                #     time_index.extend([f'{d.strftime("%Y-%m-%d")} {day_open_time}' for d in pd.date_range(start=time_str[0],end=time_str[1])])
                list_df.append(_format_timeindex(pd.DataFrame(1.0, index=time_index, columns=[ex]).sort_index(), time_zone))
            else:
                # time_info = EXCHANGE_TRADE_TIME_MAPPING[EXCHANGE_CALENDAR_MAPPING.get(ex, ex)]
                # day_close_time = time_info.split(',')[-2].split('-')[-1]
                # time_zone = time_info.split(',')[-1]
                day_open_time, day_close_time = trade_time_map[ex][0].split('-')[0], trade_time_map[ex][-1].split('-')[-1]
                assert day_open_time != day_close_time, f"TRADABLE: NOT ALLOWED: {ex} open trade time {day_open_time} equals to close trade time {day_close_time}."
                time_zone = time_zone_map[ex]

                time_index = [f'{d} {day_close_time}' for d in exchange_calendar_map[ex]]
                if self.settlement_type == 'open':
                    time_index.extend([f'{d} {day_open_time}' for d in exchange_calendar_map[ex]])
                tmp = pd.DataFrame(1.0, index=time_index, columns=[ex])

                list_df.append(_format_timeindex(tmp, time_zone).sort_index())
        ex_df = pd.concat(list_df,axis=1).fillna(2.0)
        if 'ALL_TRADABLE' in all_exchanges:
            ex_df["ALL_TRADABLE"] = 1.0
        ex_df = ex_df[ex_df.index <= self.end_ts]
        return ex_df[exchange2sym]

    def _intraday(self,exchange_calendar_map,all_exchanges, exchange2sym, trade_time_map, time_zone_map):
        time_str = [dt.datetime.fromtimestamp(int(ts), pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z') for ts in self.time]
        # res_df = pd.DataFrame(index=[time_str[0],time_str[1]], columns=all_exchanges)
        # res_df.index = pd.to_datetime(res_df.index)
        # res_df = _format_timeindex(res_df.resample('30min').last())
        # need to check for FX
        list_df = []
        dummy_df = pd.DataFrame(index=[time_str[0],time_str[1]], columns=['dummy'])
        dummy_df.index = pd.to_datetime(dummy_df.index)
        dummy_df = _format_timeindex(dummy_df.resample('1D').last())
        list_df.append(dummy_df)

        for ex in all_exchanges:
            if ex == "ALL_TRADABLE":
                # time_info = EXCHANGE_TRADE_TIME_MAPPING["ALL_TRADABLE"]
                # day_close_time, time_zone = time_info.split(',')[0], time_info.split(',')[1]
                # time_index = [f'{d.strftime("%Y-%m-%d")} {day_close_time}' for d in pd.date_range(start=time_str[0],end=time_str[1])]
                # list_df.append(_format_timeindex(pd.DataFrame(1.0, index=time_index, columns=[ex]), time_zone))
                list_df.append(_format_timeindex(pd.DataFrame(1.0, index=[time_str[0]], columns=[ex])))
            else:
                tmp = pd.DataFrame(2.0, index=exchange_calendar_map[ex], columns=[ex])
                tmp.index = pd.to_datetime(tmp.index)
                tmp['date'] = tmp.index
                tmp = tmp.resample('30min').ffill(limit=47).dropna().iloc[:,0]
                # open_time = EXCHANGE_TRADE_TIME_MAPPING[EXCHANGE_CALENDAR_MAPPING.get(ex, ex)].split(',')
                # time_zone = open_time[-1]
                open_time = trade_time_map[ex]
                time_zone = time_zone_map[ex]
                lst_ind = []
                if open_time[0]:
                    for time in open_time:
                        t = time.split('-')
                        lst_ind.append(tmp.index.indexer_between_time(t[0],t[1],include_end=True)) # END point is included
                    tmp.iloc[np.sort(np.concatenate(lst_ind))] = 1
                    for time in open_time:
                        t = time.split('-')
                        assert t[0] != t[1], f"TRADABLE: NOT ALLOWED: {ex} open trade time {t[0]} equals to close trade time {t[1]}."
                        end_times = tmp.index.indexer_between_time((pd.to_datetime(t[1])+dt.timedelta(minutes=-30)).strftime('%H:%M'),t[1], include_end=True, include_start=False) # END point is included
                        for end_time in end_times:
                            # tmp.loc[tmp.index[end_time]] = 1
                            end_time_dt = pd.to_datetime(tmp.index[end_time].strftime("%Y-%m-%d"+' '+t[1]))
                            tmp.loc[end_time_dt] = 1
                            tmp.loc[end_time_dt+dt.timedelta(seconds=1)] = 2
                            # if self.settlement_type == 'open':
                            start_time_dt = pd.to_datetime(tmp.index[end_time].strftime("%Y-%m-%d"+' '+t[0]))
                            tmp.loc[start_time_dt] = 1
                    tmp.sort_index(inplace=True)
                else:
                    # assume all tradable if there is no trade time specified in the mapping
                    tmp.iloc[:] = 1
            
                # res_df.update(_format_timeindex(tmp, time_zone))
                list_df.append(_format_timeindex(tmp, time_zone))
        # res_df = pd.concat(list_df,axis=1).fillna(2.0)
        # if ex in all_exchanges:
        #     res_df['ALL_TRADABLE'] = 1.0
        res_df = pd.concat(list_df,axis=1).ffill().fillna(2.0)
        # res_df = res_df.fillna(2.0)
        if 'ALL_TRADABLE' in all_exchanges:
            res_df["ALL_TRADABLE"] = 1.0
        res_df = res_df[res_df.index <= self.end_ts]

        return res_df[exchange2sym]        

def _format_timeindex(df, timezone: str = 'UTC'):
    '''
        transform the original result from dm to desired format
        support timezone: 'HKT'
    '''

    df.index = string2timestamp(df.index, timezone)
    df.index.name = 'timestamp'
    return df
