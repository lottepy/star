#!/usr/bin/env python
import time
import logging
import threading
import os
import sys
import time
import sched
import queue
from datetime import datetime
import collections

import numpy as np
import pandas as pd

from ..config import loader, constant
from ..log import get_logger
from ..market.market import market
from ..symbol.base import symbol_converter, Symbols
from ..trade.trading import trading, Trading
from ..trade.trading_controller import TradingController
from ..util.target_positions import calculate_positions_from_weights
from ..util import trading_utils
from .order import Order
from .manager import OrderManager


class TWAPOrderManager(OrderManager):
    def __init__(self, market,  trading_ctrl, paper, directions,
                 max_order_amount=20000, order_interval=900,
                 cash=0, timeout=60):
        super().__init__(market, trading_ctrl, paper,
                         directions, cash, timeout)
        self.scheduling = collections.defaultdict(list) # int -> orders
        self.interval_count = 0

    def create_orders(self, quantities, lot_size):
        pass

    def submit_orders(self):
        pass
