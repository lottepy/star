import numpy as np

from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.signal_generator.base_strategy import BaseStrategy
from quantcycle.app.status_recorder.status_recorder import BaseStatusRecorder
from quantcycle.utils import get_logger
from quantcycle.utils.production_constant import Time


# control all strategy since all strategy may hv diff universe
class SuperStrategyManager():
    def __init__(self,symbols):
        self.strategies = {}
        self.id2universe = {}
        self.id2symbol_index = {}
        self.symbols = symbols

    def add_strategy(self,id : int ,strategy : BaseStrategy,symbol_batch):

        self.strategies[id] = strategy
        self.id2universe[id] = symbol_batch
        self.id2symbol_index[id] = np.array(list(map(lambda x: self.symbols.index(x),self.id2universe[id])))
    
    def save_current_data(self,current_fx_data, current_data,timestamp,is_tradable):
        for id,strategy in self.strategies.items(): 
            temp_current_data = current_data[self.id2symbol_index[id]]
            temp_current_fx_data = current_fx_data[self.id2symbol_index[id]]
            temp_is_tradable = is_tradable[self.id2symbol_index[id]]
            strategy.save_current_data(temp_current_fx_data, temp_current_data, timestamp)

    def on_pms_feedback(self,id,order,msg):
        return self.strategies[id].on_pms_feedback(order,msg)

    def on_risk_feedback(self,id,order,msg):
        return self.strategies[id].on_risk_feedback(order,msg)


    #def on_data(self,window_data,time_data,window_ref_data):
    def on_data(self,window_current_data, window_time_array, window_fx_data, window_fx_time, window_rate_data,
                window_rate_time, window_tradable_data, window_tradable_time, window_ref_data_dict,
                window_ref_time_dict, secondary_data, static_info_dict,order_feedback=None):


        get_logger.get_logger().info(f"{self.__class__.__name__} on_data order_feedback:{order_feedback}")
        get_logger.get_logger().info(f"{self.__class__.__name__} on_data window_current_data:{window_current_data.shape}")
        get_logger.get_logger().info(f"{self.__class__.__name__} on_data window_time_array:{window_time_array.shape} time_data:{window_time_array[0,Time.YEAR.value:Time.HOUR.value]}")
        get_logger.get_logger().info(f"{self.__class__.__name__} on_data window_ref_data:{window_ref_data_dict.keys()} {[x.shape for x in window_ref_data_dict.values()]}")
        get_logger.get_logger().info(f"{self.__class__.__name__} on_data window_ref_time_data:{window_ref_time_dict.keys()} {[x.shape for x in window_ref_time_dict.values()]}")
        get_logger.get_logger().info(f"{self.__class__.__name__} on_data secondary_data:{secondary_data.keys()} {[value.keys() for key,value in secondary_data.items()]}")


        order = {}
        backup_symbols_dict = {}
        for id,strategy in self.strategies.items():
            data_dict = {}
            time_dict={}
            data_dict["main"] = window_current_data[:,self.id2symbol_index[id],:]
            time_dict["main"] = window_time_array
            data_dict["fxrate"] = window_fx_data[:,self.id2symbol_index[id],:] if window_fx_data is not None else None
            time_dict["fxrate"] = window_fx_time
            data_dict["int"] = window_rate_data[:,self.id2symbol_index[id],:] if window_rate_data is not None else None
            time_dict["int"] = window_rate_time if window_rate_time is not None else None
            data_dict["ex_tradable"] = window_tradable_data[:,self.id2symbol_index[id],:].copy()
            time_dict["ex_tradable"] = window_tradable_time.copy()
            secondary_meta_data = {}
            if secondary_data:
                for key, item in secondary_data[id].items():
                    #if all(sub not in key for sub in ["ID_symbol_map", "id_map"]):
                    if key not in ["ID_symbol_map", "id_map"]:
                        data_dict[key] = item["data"]
                        time_dict[key] = item["time"]
                         
                        secondary_group_type = None
                        for group_type in secondary_data[id]["ID_symbol_map"]:
                            if group_type in key:
                                secondary_group_type = group_type
                                break

                        for i in range(len(secondary_data[id]["ID_symbol_map"][secondary_group_type])):
                            if str(i) not in secondary_meta_data:
                                secondary_meta_data[str(i)] = {}
                            if secondary_group_type not in secondary_meta_data[str(i)]:
                                secondary_meta_data[str(i)][secondary_group_type] = list(secondary_data[id]["ID_symbol_map"][secondary_group_type][i])
                        
                for strategy_name in secondary_data[id]['ID_symbol_map'].keys():
                    for strategy_id in range(len(secondary_data[id]['ID_symbol_map'][strategy_name])):
                        strategy.metadata[str(int(strategy_id))] = {}
                        strategy.metadata[str(int(strategy_id))][strategy_name] = \
                            secondary_data[id]['ID_symbol_map'][strategy_name][strategy_id]
                
                strategy.metadata.update(secondary_meta_data)
                strategy.id_mapping = secondary_data[id]["id_map"][strategy_name]

            if "signal_remark_fields" in static_info_dict:
                if 'remark' not in strategy.metadata:
                    strategy.metadata['remark'] = {}
                strategy.metadata['remark']['fields'] = static_info_dict["signal_remark_fields"]
            if "ref_data_fields" in static_info_dict:
                for key,value in static_info_dict["ref_data_fields"].items():
                    if key not in strategy.metadata:
                        strategy.metadata[key] = {}
                    strategy.metadata[key]['fields'] = static_info_dict["ref_data_fields"][key]
            temp_order = None
            if order_feedback is None:
                temp_order = strategy.on_data(data_dict,time_dict,window_ref_data_dict,window_ref_time_dict)
            else:
                temp_order = strategy.on_order_feedback(order_feedback,data_dict,time_dict,window_ref_data_dict,window_ref_time_dict)
            backup_symbols = strategy.backup_symbols.copy()
            strategy.remove_backup_symbols()
            if temp_order is None:
                if order_feedback is None:
                    get_logger.get_logger().info(f"receive None in order")
                    symbols = self.id2universe[id]
                    temp_order = np.zeros(len(symbols)).reshape(1, -1)
                else:
                    get_logger.get_logger().info(f"receive None in order for reload_order_feedback")
            if temp_order is not None:
                order[id] = temp_order[0]
                backup_symbols_dict[id] = backup_symbols
            #order[id] = strategy.on_data(data_dict,time_dict)[0]
        return order,backup_symbols_dict


    def load_status(self,id,pickle_dict):
        self.strategies[id].load_status(pickle_dict)
        

    def save_status(self,status_path):
        status_recorder = BaseStatusRecorder()
        for id,strategy in self.strategies.items():
            pickle_dict = strategy.save_status(id,status_path)
            status_recorder.dump(pickle_dict,id,status_path)
