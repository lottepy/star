import os
import unittest

import numpy as np
from quantcycle.app.pms_manager.portfolio_manager import PorfolioManager
from quantcycle.app.settle_manager.settlement import Settler
from quantcycle.utils.production_constant import (InstrumentType,
                                                  OrderFeedback, PmsStatus,
                                                  TradeStatus)
from quantcycle.utils.production_data_loader import DataLoader


class TestSettler(unittest.TestCase):

    def test_settler_01(self):
        #initiate pms
        cash = 1000000
        ccy_matrix = np.array([[1]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FUTURES.value])
        pms = PorfolioManager(cash, ccy_matrix, instrument_type)
        pms.sync_holding([100])
        symbols = ["CN.EXP7.0.SGX"]
        id = 0
        #initiate pms
        settler = Settler()
        settler.add(pms,symbols,instrument_type,id)

        data_info = {
                        "FUTURES": {
                        "DataCenter": "DataMaster",
                        "SymbolArgs": {
                            "DMAdjustType": 0
                        },
                        "Fields": "OHLC",
                        "Frequency": "DAILY"
                        }
                    }
        base_ccy = "USD"
        data_loader = DataLoader(data_info, base_ccy, {"FUTURES":symbols},{}, {}, {})
        settler.add_data_loader(data_loader)


        settler.check_future_settle(2020,11,16)
        roll_task = settler.return_roll_task()
        assert len(roll_task[id]["orders"]) == 0

        settler.check_future_settle(2020,11,17)
        roll_task = settler.return_roll_task()
        assert roll_task[id]["main2ind_ticker"][symbols[0]] == {"before_ticker":"1601004125","after_ticker":"1601004126"}
        assert roll_task[id]["orders"]["1601004125"] == -100 #CNX20.SGF
        assert roll_task[id]["orders"]["1601004126"] == 100  #CNZ20.SGF

        settler.check_future_settle(2020,11,18)
        roll_task = settler.return_roll_task()
        assert len(roll_task[id]["orders"]) == 0


    def test_settler_02(self):
        #initiate pms
        cash = 1000000
        ccy_matrix = np.array([[1,1]], dtype=np.float64)
        instrument_type = np.array([InstrumentType.FUTURES.value])*2
        pms = PorfolioManager(cash, ccy_matrix, instrument_type)
        pms.sync_holding([100,200])
        symbols = ["RB.EXP7.0.SHF","AG.EXP7.0.SHF"]
        id = 0
        #initiate pms
        settler = Settler()
        settler.add(pms,symbols,instrument_type,id)

        data_info = {
                        "FUTURES": {
                        "DataCenter": "DataMaster",
                        "SymbolArgs": {
                            "DMAdjustType": 0
                        },
                        "Fields": "OHLC",
                        "Frequency": "DAILY"
                        }
                    }
        base_ccy = "USD"
        data_loader = DataLoader(data_info, base_ccy, {"FUTURES":symbols},{}, {}, {})
        settler.add_data_loader(data_loader)

        settler.check_future_settle(2020,9,2)
        roll_task = settler.return_roll_task()
        assert len(roll_task[id]["orders"]) == 0

        settler.check_future_settle(2020,9,3)
        roll_task = settler.return_roll_task()
        assert roll_task[id]["main2ind_ticker"][symbols[0]] == {"before_ticker":"1601003057","after_ticker":"1601003058"}
        assert roll_task[id]["main2ind_ticker"][symbols[1]] == {"before_ticker":"1601000388","after_ticker":"1601000389"}
        assert roll_task[id]["orders"]["1601000388"] == -200 #AG2009.SHF
        assert roll_task[id]["orders"]["1601000389"] == 200 #AG2010.SHF
        assert roll_task[id]["orders"]["1601003057"] == -100 #RB2009.SHF
        assert roll_task[id]["orders"]["1601003058"] == 100  #RB2010.SHF

        settler.check_future_settle(2020,9,4)
        roll_task = settler.return_roll_task()
        assert len(roll_task[id]["orders"]) == 0
