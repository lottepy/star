# -- coding: utf-8 --
import pandas as pd
import numpy as np
import log as logger
# from algo_hitbtc import get_hisdata, get_lastprice
# from binance_api import binance_base
import os
# from utils.db_utility import update_weight_table, fetch_algocontrol, PositionSync
import cvxopt
# import ccxt
import time
import logging
import pprint
import uuid
import requests
import re
import json

# binance_client = binance_base()
# LOCAL_DEBUG = True
session = requests.session()

def submit_orders(orders=[]):
    submit_order_dict = {
        'token': 'dX8mPbl4RdY4UqzF2FGd',
        'tid': uuid.uuid4().hex,
        'exchange': 'shipane',
        'orders': orders
    }
    # logger.info("order to sumbit: %s ", submit_order_dict)
    try:
        reply = session.post('http://192.168.9.144:8000/orders', json=submit_order_dict)
        return reply.json()
    except:
        logger.error()


def get_buy_share_number(init_cap, opt_weight, price, board_lot, buffer=0.):
    """
    计算交易时所需股数

    :param init_cap: flaoat
         初始资本
    :param opt_weight: ndarray
         最优权重
    :param price: ndarray
         资产价格
    :param board_lot: ndarray
         最小交易单位
    :param buffer: float
        缓冲量，默认为0
    :return: ndarray
        股数向量
    """

    init_cap *= (1 - buffer)
    entry_price = price * board_lot
    opt_entry_num = opt_weight * init_cap / entry_price
    buy_entry_num = np.floor(opt_entry_num)
    buy_stock_num = buy_entry_num * board_lot

    return buy_stock_num


def initial_submit():
    # reb_df = pd.read_csv('order_rebalance_500000.csv')
    # n_stock = len(reb_df)
    # reb_df['amount'] = get_buy_share_number(init_cap=500000,
    #                                         opt_weight=reb_df['weight'].values,
    #                                         price=reb_df['price'].values,
    #                                         board_lot=np.ones(n_stock) * 100,
    #                                         buffer=0.0)

    # from trading log
    reb_df = pd.read_csv('2nd_order.csv')
    # print reb_df.to_string()

    order_list = [
        {
            "symbol": reb_df['iuid'].iloc[i],
            "amount": reb_df['amount'].iloc[i],
            "action": 'BUY',
            "type": 'market',

        } for i in range(len(reb_df.index))
    ]

    # reb_df['real_w'] = reb_df['price']*reb_df['amount']
    # reb_df['real_w'] = reb_df['real_w'] / reb_df['real_w'].sum()
    # reb_df['w_diff'] = reb_df['real_w'] - reb_df['weight']

    # print reb_df.to_string()
    # reb_df.to_csv('trading_log.csv')
    submit_orders(order_list)


def main():
    initial_submit()


if __name__ == "__main__":
    main()
