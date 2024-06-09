import os
from datetime import datetime

import numpy as np
import pandas as pd
import pytz
from pandas.tseries.offsets import BDay

from quantcycle.app.data_manager import DataDistributorSub, DataManager
from quantcycle.utils import get_logger
from quantcycle.utils.production_mapping import instrument_type_map


def get_load_dt_range_by_window(start_dt, end_dt, window_size, frequency, window_day_count):
    if frequency == 'HOURLY':
        # Assuming a day has at least 6 timestamps
        window_day_count = window_size // 6 + 1
    elif frequency == 'MINUTELY':
        window_day_count = window_size // (6 * 60) + 1
    elif frequency == 'SECONDLY':
        window_day_count = window_size // (6 * 60 * 60) + 1
    elif frequency == 'DAILY':
        window_day_count = window_size + 1
    load_start = start_dt - BDay(2 * window_day_count + 11)
    load_end = end_dt + BDay(11)
    return load_start, load_end

def find_highest_frequency(frequency_list):
    ordered_frequency = ['DAILY', 'HOURLY', 'MINUTELY', 'SECONDLY']
    max_index = 0
    for freq in frequency_list:
        try:
            index = ordered_frequency.index(freq)
        except ValueError:
            raise ValueError(f'Invalid frequency "{freq}" in main data config.')
        if index > max_index:
            max_index = index
    return ordered_frequency[max_index]


def dt_to_date(load_dt: datetime, add_end_time=False):
    date_dict = {'Year': load_dt.year,
                 'Month': load_dt.month,
                 'Day': load_dt.day}
    if add_end_time:
        date_dict.update({'Hour': 23,
                          'Minute': 59,
                          'Second': 59})
    return date_dict


class DataLoader:

    def __init__(self, data_info: dict, base_ccy, type_to_symbols: dict, secondary_data_config: dict, sid_to_secondary,
                 ref_data_config: dict):
        self.data_info = data_info
        self.base_ccy = base_ccy
        self.secondary_data_config = secondary_data_config
        self.ref_data_config = ref_data_config
        self.type_to_symbols = type_to_symbols      # type - FX, STOCKS, FUTURES; symbols are from strategy pool
        self.sid_to_secondary = sid_to_secondary    # sid - strategy ID; secondary is like {'RSI': [0.0, 1.0...]}
        self.main_load_start = None     # datetime
        self.main_load_end = None       # datetime


    def find_load_dt_range_by_req(self, start_dt, end_dt, window_size, frequency):
        #! This method may be too slow on large data request
        window_day_count = 1  # default as "daily"

        if window_size != 1:
            load_start, load_end = get_load_dt_range_by_window(start_dt, end_dt, window_size,
                                                               frequency, window_day_count)

            ret_dict = self.get_main_data(load_start.year, load_start.month, load_start.day,
                                          load_end.year, load_end.month, load_end.day)
            ts = ret_dict['current_data_time_array']

            # find out the load_start datetime
            start_dt_filter = list(map(lambda x: x[0] >= int(start_dt.timestamp()), ts))
            start_dt_filter = start_dt_filter[window_size - 1:] + [False] * (window_size - 1)

            load_start = datetime.utcfromtimestamp(ts[start_dt_filter][0][0]).replace(tzinfo=pytz.utc)
        else:   # window_size == 1
            load_start = start_dt

        load_end = end_dt + BDay(1)
        self.main_load_start = load_start
        self.main_load_end = load_end
        return load_start, load_end


    def get_main_data(self, start_year, start_month, start_day, end_year, end_month, end_day, start_hour=0,end_hour=23):
        #TODO： implement不优，改输入参数结构更好 > starttime and endtime 

        need_INT = True     # multi-assets no longer allow omitting interest rate table
        concat_symbols = []
        if len(self.type_to_symbols) > 1:       # multi-assets
            freq_list = []
            for symbol_type, symbol_set in self.type_to_symbols.items():
                concat_symbols.extend(symbol_set)
                freq_list.append(self.data_info[symbol_type]['Frequency'])
            frequency = find_highest_frequency(freq_list)
            symbol_type = 'MIX'
        else:
            symbol_type = list(self.type_to_symbols.keys())[0]
            concat_symbols.extend(self.type_to_symbols[symbol_type])
            frequency = self.data_info[symbol_type]['Frequency']

        data_manager_config = {"Data": {}}
        i = 0
        for _symbol_type, symbol_set in self.type_to_symbols.items():
            symbol_batch = list(symbol_set)
            if _symbol_type not in self.data_info:
                raise Exception("f{sym_type} does not exist in data_info")
            temp_data_info = self.data_info[_symbol_type].copy()
            if temp_data_info["DataCenter"] == "DataMaster" and "AccountCurrency" not in temp_data_info["SymbolArgs"]:
                temp_data_info["SymbolArgs"]["AccountCurrency"] = self.base_ccy
            sub_data_manager_config = {
                "Label": _symbol_type,
                "Type": "Download",
                "DataCenter": temp_data_info["DataCenter"],
                "Symbol": symbol_batch,
                "SymbolArgs": temp_data_info.get("SymbolArgs", {}).copy(),
                # "Fields": temp_data_info["Fields"],
                "Fields": ["OHLC"],
                "StartDate": {"Year": start_year, "Month": start_month, "Day": start_day, "Hour": start_hour},
                "EndDate": {"Year": end_year, "Month": end_month, "Day": end_day, "Hour": end_hour},
                "Frequency": temp_data_info['Frequency']
            }
            add_dayend_to_endtime(sub_data_manager_config)
            if not need_INT:
                sub_data_manager_config["Other"] = {"NoINT": True}

            if temp_data_info["DataCenter"] == "DataMaster":
                data_manager_config["Data"].update({f"DataGroup{i}": sub_data_manager_config.copy()})
            else:       # LocalCSV
                DataCenterArgs = temp_data_info["DataCenterArgs"]
                """ temp_sub_data_manager_config = sub_data_manager_config.copy()
                temp_sub_data_manager_config["DataCenter"] = "DataMaster"
                temp_sub_data_manager_config["SymbolArgs"]["DMAdjustType"] = sub_data_manager_config["SymbolArgs"].get("DMAdjustType", 1)
                temp_sub_data_manager_config["SymbolArgs"]["AccountCurrency"] = self.base_ccy
                if 'info' in DataCenterArgs:
                    temp_sub_data_manager_config["SymbolArgs"]["DatacenterMap"] = DataCenterArgs['info']
                data_manager_config["Data"].update({f"DataGroup{i}": temp_sub_data_manager_config.copy()})
                i += 1 """

                need_dm = False
                temp_dict = {"main_dir": "MAIN", "fxrate_dir": "CCPFXRATE", "int_dir": "INT"}
                for data_dir in ["main_dir", "fxrate_dir", "int_dir"]:
                    i += 1
                    temp_sub_data_manager_config = sub_data_manager_config.copy()
                    if data_dir in DataCenterArgs:
                        temp_sub_data_manager_config["Label"] += f"-{temp_dict[data_dir]}"
                        temp_sub_data_manager_config["DataCenter"] = "LocalCSV"
                        temp_sub_data_manager_config["DataCenterArgs"] = {
                            'key_name': f'{_symbol_type}-{_symbol_type}/{temp_dict[data_dir]}',
                            'dir': DataCenterArgs[data_dir]
                        }
                        if 'info' in DataCenterArgs:
                            temp_sub_data_manager_config["DataCenterArgs"]["info"] = DataCenterArgs['info']
                        if data_dir == "fxrate_dir":
                            temp_sub_data_manager_config["Fields"] = "fx_rate"
                        elif data_dir == "int_dir":
                            temp_sub_data_manager_config["Fields"] = "interest_rate"
                    else:
                        if not need_dm:
                            temp_sub_data_manager_config["DataCenter"] = "DataMaster"
                            temp_sub_data_manager_config["SymbolArgs"]["DMAdjustType"] = temp_sub_data_manager_config[
                                "SymbolArgs"].get("DMAdjustType", 1)
                            temp_sub_data_manager_config["SymbolArgs"]["AccountCurrency"] = self.base_ccy
                            if 'info' in DataCenterArgs:
                                temp_sub_data_manager_config["SymbolArgs"]["DatacenterMap"] = DataCenterArgs['info']
                            need_dm = True
                        else:
                            continue
                    data_manager_config["Data"].update({f"DataGroup{i}": temp_sub_data_manager_config.copy()})
            i += 1

        # Tradable table
        tradable_time = datetime(start_year, start_month, start_day, start_hour, tzinfo=pytz.utc) - BDay(10)
        tradable_table_config = {'Label': 'TRADABLE', 'Type': 'Download', 'DataCenter': 'DataMaster',
                                 'Symbol': concat_symbols, 'Fields': 'tradable_table',
                                 'Frequency': frequency,
                                 'SymbolArgs': {'AccountCurrency': self.base_ccy},
                                 "StartDate": {"Year": tradable_time.year, "Month": tradable_time.month, "Day": tradable_time.day,"Hour":tradable_time.hour,"Minute":0,"Second":0},
                                 "EndDate": {"Year": end_year, "Month": end_month, "Day": end_day,"Hour":end_hour,"Minute":59,"Second":59}
                                 }
        #add_dayend_to_endtime(tradable_table_config)
        if "FX" in self.type_to_symbols and self.data_info["FX"]["DataCenter"] == "LocalCSV":
            tradable_table_config["DataCenterArgs"] = {"info": self.data_info["FX"]["DataCenterArgs"]["info"]}
        data_manager_config["Data"].update({f"DataGroup{i}": tradable_table_config})
        i += 1

        # Multi-assets
        if len(self.type_to_symbols) > 1:
            asset_lables = [f'{asset}-{asset}' for asset in self.type_to_symbols]
            group_config = {'Label': 'MIX', 'Type': 'Download', 'DataCenter': 'DataManager', 'DMActions': 'STACK',
                            'Symbol': asset_lables, 'Fields': [], 'Frequency': frequency,
                            "StartDate": {"Year": start_year, "Month": start_month, "Day": start_day, "Hour": start_hour},
                            "EndDate": {"Year": end_year, "Month": end_month, "Day": end_day},
                            }
            add_dayend_to_endtime(group_config)
            data_manager_config['Data'].update({f'DataGroup{i}': group_config})
            i += 1

        get_logger.get_logger().info(f"data_manager_config for get_main_data:{str(data_manager_config)}")
        data = get_data_from_data_manager(data_manager_config)

        # Retrieve data
        main_ticker = f'{symbol_type}-{symbol_type}/MAIN'
        current_data_array = data[main_ticker]['data_arr']
        current_time_array = data[main_ticker]['time_arr'].astype(np.int64)

        fx_ticker = f'{symbol_type}-{symbol_type}/CCPFXRATE'
        current_fx_array = data[fx_ticker]['data_arr']
        current_fx_time_array = data[fx_ticker]['time_arr'].astype(np.int64)

        trading_ccy = list(data[fx_ticker]['symbol_arr'])
        fx_raw_sym = list(data[main_ticker]['symbol_arr'])

        current_rate_array = None
        current_rate_time_array = None
        if need_INT:
            rate_ticker = f'{symbol_type}-{symbol_type}/INT'
            current_rate_array = data[rate_ticker]['data_arr']
            current_rate_time_array = data[rate_ticker]['time_arr'].astype(np.int64)

        tradable_ticker = 'TRADABLE-MIX/TRADABLE_TABLE'
        tradable_array = data[tradable_ticker]['data_arr']
        tradable_time_array = data[tradable_ticker]['time_arr'].astype(np.int64)

        # mainforce = data[f'{symbol_type}-{symbol_type}/CONTRACTS_INFO']['data_arr'][:,0]
        # mainforce_time_array = data[f'{symbol_type}-{symbol_type}/CONTRACTS_INFO']['time_arr'][:,0]

        #contracts_info = f'{symbol_type}-{symbol_type}/CONTRACTS_INFO'
        contract_key = [ key for key in list(data.keys()) if "CONTRACTS_INFO" in key]
        mainforce = data[contract_key[0]]['data_arr'][:,:,0].astype(np.int64) if len(contract_key) > 0 else None
        mainforce_time_array = data[contract_key[0]]['time_arr'].astype(np.int64) if len(contract_key) > 0 else None
        mainforce_symbols = data[contract_key[0]]['symbol_arr'] if len(contract_key) > 0 else []

        return_dict = {"current_data_array": current_data_array, "current_data_time_array": current_time_array,
                       "current_fx_array": current_fx_array, "current_fx_time_array": current_fx_time_array,
                       "current_rate_array": current_rate_array, "current_rate_time_array": current_rate_time_array,
                       "tradable_array": tradable_array, "tradable_time_array": tradable_time_array,
                       "trading_ccy": trading_ccy, "fx_raw_sym": fx_raw_sym, "mainforce": mainforce,
                       "mainforce_time_array": mainforce_time_array,"mainforce_symbols":mainforce_symbols}
        return return_dict


    def get_secondary_data(self, strategy_id, start_dt, end_dt, info_dict):
        secondary_data = {"strategy_id": strategy_id, "ID_symbol_map": {}}
        # window_size = self.config["algo"]["window_size"]
        # ['pnl', 'cost','position','metrics']
        for engine_name, temp_info_dict in info_dict.items():

            id_list = [int(i) for i in self.sid_to_secondary[strategy_id][engine_name]]
            hdf5_path = temp_info_dict["hdf5_path"]
            config_for_load_secondary_data = {
                "Data": {
                    "DataGroup1": {
                        "Label": "Strat",
                        "Type": "Download",
                        "DataCenter": self.secondary_data_config[engine_name]["DataCenter"],
                        "DataCenterArgs": {
                            'DataPath': hdf5_path,
                            'pnl_type': ['order_feedback'],
                        },
                        "Symbol": id_list,  # strat ID must be int
                        "Fields": self.secondary_data_config[engine_name]["Fields"],
                        "StartDate": {
                            "Year": start_dt.year,
                            "Month": start_dt.month,
                            "Day": start_dt.day
                        },
                        "EndDate": {
                            "Year": end_dt.year,
                            "Month": end_dt.month,
                            "Day": end_dt.day
                        },
                        "Frequency": self.secondary_data_config[engine_name]["Frequency"]
                    }
                }
            }
            if "SymbolArgs" in self.secondary_data_config[engine_name]:
                SymbolArgs = {
                    "SymbolArgs": {
                        "Metrics": {
                            "allocation_freq": self.secondary_data_config[engine_name]["SymbolArgs"]["Metrics"][
                                "allocation_freq"],
                            "lookback_points_list":
                                self.secondary_data_config[engine_name]["SymbolArgs"]["Metrics"][
                                    "lookback_points_list"],
                            "addition": self.secondary_data_config[engine_name]["SymbolArgs"]["Metrics"][
                                "addition"],
                            "multiplier": self.secondary_data_config[engine_name]["SymbolArgs"]["Metrics"][
                                "multiplier"]
                        }
                    }
                }
                # Frequency = {"Frequency": secondary_data_field_dict[engine_name]["SymbolArgs"]["Metrics"]["allocation_freq"]}
                config_for_load_secondary_data["Data"]["DataGroup1"].update(SymbolArgs)
                # config_for_load_secondary_data["Data"]["DataGroup1"].update(Frequency)

            output = get_data_from_data_manager(config_for_load_secondary_data)

            for field in self.secondary_data_config[engine_name]["Fields"]:
                if field == "metrics":
                    for n in [61, 252]:
                        if len(output[f"Strat-Strat/{field}_{n}"]["data_arr"]) != 0:
                            secondary_data[engine_name + '_' + field + f'_{n}'] = {}
                            secondary_data[engine_name + '_' + field + f'_{n}']["data"] = \
                                output[f"Strat-Strat/{field}_{n}"]["data_arr"]
                            secondary_data[engine_name + '_' + field + f'_{n}']["time"] = \
                                output[f"Strat-Strat/{field}_{n}"]["time_arr"]
                        else:
                            get_logger.get_logger().info(f"Strat-Strat/{field}_{n} missing")
                else:
                    if len(output[f"Strat-Strat/{field}"]["data_arr"]) != 0:
                        secondary_data[engine_name + '_' + field] = {}
                        secondary_data[engine_name + '_' + field]["data"] = output[f"Strat-Strat/{field}"][
                            "data_arr"]
                        secondary_data[engine_name + '_' + field]["time"] = output[f"Strat-Strat/{field}"][
                            "time_arr"]
                    else:
                        get_logger.get_logger().info(f"Strat-Strat/{field} missing")

            # secondary_data[engine_name + '_' + "ID_symbol_map"] = output[f"Strat-Strat/ID_symbol_map"]
            secondary_data["ID_symbol_map"][engine_name] = output[f"Strat-Strat/ID_symbol_map"]
            secondary_data["id_map"] = {}
            secondary_data["id_map"][engine_name] = {}
            secondary_data["id_map"][engine_name].update(np.array(output[f'Strat-Strat/id_map'], dtype=np.int64))

            # secondary_data["field_names"] = {}
            # secondary_data["field_names"][engine_name] = {}
        return secondary_data


    def get_ref_data(self, start_date=None, end_date=None):
        data_manager_config = {"Data": {}}
        i = 0
        for key, value in self.ref_data_config.items():
            i += 1
            sub_data_manager_config = {
                "Label": key + "-REF",
                "Type": "Download",
                "DataCenter": value["DataCenter"],
                # "DataCenterArgs": value["DataCenterArgs"],
                "Symbol": list(value["Symbol"]),
                "SymbolArgs": value.get("SymbolArgs", {"DataSource": "LocalCSV"}),
                "Fields": value.get("Fields", []),
                "StartDate": value.get("StartDate",
                                       dt_to_date(self.main_load_start)) if start_date is None else start_date,
                "EndDate": value.get("EndDate",
                                     dt_to_date(self.main_load_end, True)) if end_date is None else end_date,
                "Frequency": "DAILY",
            }
            if "DataCenterArgs" in value:
                DataCenterArgs = value["DataCenterArgs"]
                DataCenterArgs["key_name"] = key
                sub_data_manager_config["DataCenterArgs"] = DataCenterArgs.copy()
            data_manager_config["Data"].update({f"DataGroup{i}": sub_data_manager_config.copy()})

        get_logger.get_logger().info(f"data_manager_config for get_ref_data:{str(data_manager_config)}")
        data = get_data_from_data_manager(data_manager_config)
        data = {key: val for key, val in data.items() if 'INFO' not in key}

        temp_data = {}
        for key, value in data.items():
            if "CCPFXRATE" in key or "INT" in key:
                continue
            elif "MAIN" in key:
                temp_data[key.split("-REF")[0]] = data[key]
            else:
                temp_data[key] = data[key]

        return temp_data


    @staticmethod
    def get_signal_remark(remark_load_dir, remark_load_name, start_date=None, end_date=None):
        data_manager_config = {"Data": {}}
        i_group = 0
        if remark_load_dir is not None and remark_load_name is not None:
            group_config = {'Label': 'signal_remark', 'DataCenter': 'LocalCSV', 'Type': 'Download',
                            'DataCenterArgs': {'key_name': 'SIG-MRK/MAIN'},
                            'SymbolArgs': {'DataSource': 'signal_remark'}, 'Frequency': 'DAILY'}
            group_config['DataCenterArgs']['dir'] = remark_load_dir
            remark_names = [name[:-4] for name in os.listdir(remark_load_dir)
                            if name.startswith(remark_load_name) and name.endswith('.csv')]
            group_config['Symbol'] = remark_names
            group_config['Fields'] = get_all_field_names(remark_load_dir, remark_names)
            group_config['Fields'].sort()
            group_config_append_dates(group_config, start_date, end_date)
            data_manager_config['Data'].update({f'DataGroup{i_group}': group_config})
            i_group += 1
        else:
            return None

        get_logger.get_logger().info(f"data_manager_config for get_ref_data:{str(data_manager_config)}")
        data = get_data_from_data_manager(data_manager_config)
        # data = {key:val for key, val in data.items() if 'INFO' not in key}

        return data['SIG-MRK/MAIN']


    def check_future_mainforce(self,symbols,start_year,start_month,start_day):
        if len(symbols) == 0:
            return {},{},{}
        end_year,end_month,end_day = start_year,start_month,start_day 
        if end_month != 12:
            end_month = end_month + 1
        else:
            end_month = 1 
            end_year = end_year + 1
        return_dict = self.get_main_data(start_year,start_month,start_day,end_year,end_month,end_day)
        mainforce = return_dict["mainforce"]
        mainforce_time_array = return_dict["mainforce_time_array"]
        mainforce_symbols = return_dict["mainforce_symbols"]
        before_ticker_dict = dict(zip(mainforce_symbols,mainforce[0])) if mainforce is not None else {}
        before_ticker_dict = dict([(str(k),str(v)) for k,v in before_ticker_dict.items()])
        after_ticker_dict = dict(zip(mainforce_symbols,mainforce[1])) if mainforce is not None else {}
        after_ticker_dict = dict([(str(k),str(v)) for k,v in after_ticker_dict.items()])
        temp_roll_indictor = dict(zip(mainforce_symbols,mainforce[1] != mainforce[0])) if mainforce is not None else {}
        return temp_roll_indictor,before_ticker_dict,after_ticker_dict

    def get_info(self,symbol_batch = [],sym_type = None):
        sym_type = sym_type if sym_type is not None else list(self.data_info.keys())[0]
        if sym_type not in self.data_info:
            msg = "f{sym_type} does not exist in data_info"
            raise Exception(msg)
        temp_data_info = self.data_info[sym_type].copy()
        DataCenter = temp_data_info["DataCenter"]
        SymbolArgs = temp_data_info.get("SymbolArgs",{})
        SymbolArgs["AccountCurrency"] = self.base_ccy
        DataCenterArgs = temp_data_info.get("DataCenterArgs",{})
        
        data_manager_config = {
            "Data": {
                "DataGroup1": {
                    "Label": sym_type,
                    "Type": "Download",
                    "DataCenter": DataCenter,
                    "Symbol": symbol_batch,
                    "SymbolArgs": SymbolArgs,
                    "Fields": "INFO",
                    "StartDate": {"Year": 2020,"Month": 1,"Day": 1},
                    "EndDate": {"Year": 2020,"Month": 1,"Day": 1},
                    "Frequency": "DAILY",
                }
            }
        }
        if DataCenter != "DataMaster":
            if 'info' in DataCenterArgs:
                data_manager_config["Data"]["DataGroup1"]["DataCenterArgs"] = {'key_name': f'{sym_type}-{sym_type}/MAIN',
                                                                               'dir': DataCenterArgs['main_dir'],
                                                                               'info': DataCenterArgs['info']}
            else:
                data_manager_config["Data"]["DataGroup1"]["DataCenter"] = "DataMaster"

        data = get_data_from_data_manager(data_manager_config)
        return dict(list(data.values())[0])

    def get_info_df(self):
        iter = 0
        for sym_type, symbols in self.type_to_symbols.items():
            iter+=1
            if len(symbols) != 0:
                res_dict = self.get_info(symbol_batch = list(symbols),sym_type=sym_type)
                if iter==1:
                    df=pd.DataFrame(res_dict.values(), index=list(symbols))
                else:
                    temp_df = pd.DataFrame(res_dict.values(), index=list(symbols))
                    df = pd.concat([df,temp_df],axis=0)
        df.rename(columns={"trading_currency":"base_ccy","symboltype":'instrument_type'},inplace=True)
        df['instrument_type'] = df['instrument_type'].apply(lambda x: instrument_type_map.get(x,x))
        df = df.filter(['instrument_type', 'base_ccy','instrument_id'])
        return df


def get_data_from_data_manager(data_manager_config):
    data_manager = DataManager()
    data_manager.prepare()
    data_manager.load_config(data_manager_config)
    data_manager.run()
    data = data_manager.data_distributor_main.data_package
    data_distributor_sub = DataDistributorSub()
    data = data_distributor_sub.unpack_data(data)
    return data


def get_all_field_names(remark_load_dir, remark_names):
    all_fields = set()
    for name in remark_names:
        name += '.fields'
        f_field = os.path.join(remark_load_dir, name)
        fields = open(f_field, 'r').readlines()
        all_fields.update(fields)
    return [name[:-1] for name in all_fields]


def group_config_append_dates(group_config, load_start, load_end):
    group_config['StartDate'] = {'Year': load_start.year,
                                 'Month': load_start.month,
                                 'Day': load_start.day}
    group_config['EndDate'] = {'Year': load_end.year,
                               'Month': load_end.month,
                               'Day': load_end.day,
                               'Hour': 23,
                               'Minute': 59,
                               'Second': 59}

def add_dayend_to_endtime(group_config):
    group_config['EndDate'] = {'Year': group_config['EndDate']['Year'],
                               'Month': group_config['EndDate']['Month'],
                               'Day': group_config['EndDate']['Day'],
                               'Hour': 23,
                               'Minute': 59,
                               'Second': 59}
