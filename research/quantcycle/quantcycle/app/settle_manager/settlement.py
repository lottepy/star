import importlib
import os
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from functools import reduce
from multiprocessing import Process, Queue

import numpy as np
import pandas as pd
import pytz
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay

from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.utils.production_constant import (DataType, InstrumentType,
                                                  TerminationType, Time)
from quantcycle.utils.production_data_loader import DataLoader


class Settler:
    def __init__(self):
        self.id2pms = {}
        self.id2symbols = {}
        self.id2symbol_types = {}
        self.id2task = {}
        self.data_loader = None

    def add(self,pms :PorfolioManager,symbols,symbol_types,id):
        if id in self.id2symbols:
            msg = f"id:{id} exists already"
            raise Exception(msg)
        self.id2pms[id] = pms
        self.id2symbols[id] = symbols
        self.id2symbol_types[id] = symbol_types

    def add_data_loader(self,data_loader : DataLoader):
        self.data_loader = data_loader

    def check_future_settle(self,start_year,start_month,start_day):
        id2task = {}
        for id,symbols in self.id2symbols.items():
            temp_symbols = [str(s) for s in symbols]
            temp_roll_indictor,before_ticker_dict,after_ticker_dict = self.data_loader.check_future_mainforce(temp_symbols,start_year,start_month,start_day)
            holding_dict = dict(zip(temp_symbols,self.id2pms[id].current_holding))
            temp_order_list = []
            main2ind_ticker = {}
            for symbol in temp_symbols:
                holding = holding_dict[symbol]
                if temp_roll_indictor.get(symbol,False) and holding != 0:
                    before_ticker = before_ticker_dict[symbol]
                    after_ticker = after_ticker_dict[symbol]
                    temp_order = {before_ticker:-holding,after_ticker:holding}
                    temp_order_list.append(temp_order)
                    main2ind_ticker[symbol] = {"before_ticker":before_ticker,"after_ticker":after_ticker}

            order = {}
            for temp_order in temp_order_list:
                for key,value in temp_order.items():
                    if key not in order:
                        order[key] = 0
                    order[key] += value
            id2task[id] = {"orders":order.copy() ,"main2ind_ticker":main2ind_ticker.copy() }  
        self.id2task = id2task.copy()
    
    def return_roll_task(self):
        roll_task = self.id2task.copy()
        self.id2task = {}
        return roll_task
