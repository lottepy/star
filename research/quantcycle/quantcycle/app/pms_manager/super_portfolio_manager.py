from datetime import datetime

import numpy as np
import pandas as pd
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager


# control all pms since all pms may hv diff universe
class SuperPorfolioManager():
    def __init__(self, symbols):
        self.id2pms = {}
        self.id2universe = {}
        self.id2symbol_index = {}
        self.symbols = symbols

    def add_pms(self, id: int, pms: PorfolioManager, symbols):
        self.id2pms[id] = pms
        self.id2universe[id] = symbols
        self.id2symbol_index[id] = np.array(list(map(lambda x: self.symbols.index(x), self.id2universe[id])))

    def check_order(self, id, order):
        return self.id2pms[id].check_order(order)

    def reset_field_rollover_day(self, is_tradable=None):
        for id, pms in self.id2pms.items():
            pms.reset_field_rollover_day()

    def receive_order_feedback(self, id, msg: dict):
        self.id2pms[id].receive_order_feedback(msg)

    def reset_trade_status(self, trade_status):
        for id, pms in self.id2pms.items():
            temp_trade_status = trade_status[self.id2symbol_index[id]]
            pms.reset_trade_status(temp_trade_status)

    def calculate_spot(self, current_data, current_fx_data):
        for id, pms in self.id2pms.items():
            temp_current_data = current_data[self.id2symbol_index[id]]
            temp_current_fx_data = current_fx_data[self.id2symbol_index[id]]
            pms.calculate_spot(temp_current_data, temp_current_fx_data)

    def calculate_rate(self, current_rate, current_data, current_fx_data):
        for id, pms in self.id2pms.items():
            temp_current_data = current_data[self.id2symbol_index[id]]
            temp_current_fx_data = current_fx_data[self.id2symbol_index[id]]
            temp_current_rate = current_rate[self.id2symbol_index[id]]
            pms.calculate_rate(temp_current_rate, temp_current_data, temp_current_fx_data)

    def sync_holding(self,id,holding):
        if id not in self.id2pms:
            raise Exception(f"id:{id} not in here")
        self.id2pms[id].sync_holding(holding)

    def sync_current_data(self,id,current_data):
        if id not in self.id2pms:
            raise Exception(f"id:{id} not in here")
        self.id2pms[id].sync_current_data(current_data)

    def sync_cash(self,id,cash):
        if id not in self.id2pms:
            raise Exception(f"id:{id} not in here")
        cash_residual = self.id2pms[id].sync_cash(cash)
        return cash_residual

    def capture(self, current_time):
        for id, pms in self.id2pms.items():
            # pass
            pms.capture(current_time)

    def output_result(self):
        id2df = {}
        for id, pms in self.id2pms.items():
            temp_symbol = list(map(lambda x: self.symbols[x], self.id2symbol_index[id]))
            df = pd.DataFrame({'datetime': [datetime.utcfromtimestamp(ts) for ts in pms.historial_time]})
            df['timestamp'] = pms.historial_time
            new_df = pd.DataFrame(pms.historial_pnl, columns=[f'{symbol}_pnl' for symbol in temp_symbol])
            df = pd.concat([df, new_df], axis=1)
            df['pnl'] = np.sum(pms.historial_pnl, axis=1)
            new_df = pd.DataFrame(pms.historial_commission_fee, columns=[f'{symbol}_cost' for symbol in temp_symbol])
            df = pd.concat([df, new_df], axis=1)
            df['cost'] = np.sum(pms.historial_commission_fee, axis=1)
            new_df = pd.DataFrame(pms.historial_position, columns=[f'{symbol}_position' for symbol in temp_symbol])
            df = pd.concat([df, new_df], axis=1)
            id2df[id] = df
        return id2df

