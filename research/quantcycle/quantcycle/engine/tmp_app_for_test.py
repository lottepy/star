import numpy as np
import pandas as pd

class data_manager():
    def __init__(self):
        time_dot = 100
        self.data = {"stock1":np.random.rand(time_dot),
                     "stock2":np.random.rand(time_dot),
                     "USDCNY":np.ones(time_dot)}
        self.time = np.array(list(pd.date_range(start = '1-1-2018', periods=time_dot, freq ='D')))
        self.symbol_info = {"right_ccy":{"stock1": "CNY",
                                         "stock2": "CNY"},
                            "instrument_type": {"stock1": 1,
                                                "stock2": 1}}

    def get_css_data(self, symbol_list, field):
        symbol_info = []
        for symbol in symbol_list:
            symbol_info.append(self.symbol_info[field][symbol])
        return symbol_info

    def get_csd_data(self, symbol_list, start_dt, end_dt):
        start_index = np.where(self.time>=start_dt)[0][0]
        end_index = np.where(self.time<=end_dt)[0][-1]
        data_list = []
        for symbol in symbol_list:
            data_list.append(self.data[symbol][start_index:end_index+1])
        data_array = np.vstack(data_list).transpose()
        time_array = np.array([dt.timestamp() for dt in self.time[start_index:end_index+1]])
        return data_array, time_array




class data_distributor():
    def __init__(self, window_size, start_time):
        self.window_size = window_size
        self.start_time = start_time
        self.data_dict = {}

    def load_data(self,data_array,time_array, name):
        self.data_dict[f"data_{name}"] = data_array
        self.data_dict[f"time_{name}"] = time_array

    def prepare(self):
        self.start_idx = np.where(self.data_dict["time_main"]>=self.start_time)[0][0]
        self.time_dots = self.data_dict['time_main'].shape[0] - self.start_idx

    def distribute(self):
        window_dict = {}
        for key,value in self.data_dict.items():
            window_dict[key] = self.data_dict[key][self.start_idx-self.window_size+1:self.start_idx+1]
        self.start_idx += 1
        return window_dict

class order_crosser():
    def __init__(self):
        pass
    def cross_order(self, order_array, current_data, current_time, j):
        return j, order_array

class pms():
    def __init__(self, n_security):
        self.n_security = n_security
    def check_order(self, order_array):
        return order_array, np.zeros(self.n_security)
    def receive_order_feedback(self,trade_array):
        pass
    def calculate_spot(self,current_data):
        pass

class strategy():
    def __init__(self,n_security):
        self.n_security = n_security
    def on_data(self, window_data, window_time):
        return np.zeros(self.n_security)
