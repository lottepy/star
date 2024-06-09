# -- coding: utf-8 --
import pandas as pd
import numpy as np
import log as logger
import os
import uuid
import requests
import re
import json

from futuquant import *

# binance_client = binance_base()
# LOCAL_DEBUG = True
session = requests.session()

def get_futu_rtdata(futusymbols=[]):
    quote_ctx = OpenQuoteContext(host='192.168.9.175', port=11111)
    quote_ctx.subscribe(futusymbols, [SubType.QUOTE])
    prices_df = quote_ctx.get_stock_quote(futusymbols)[1]
    prices_df['aqmsymbol'] = prices_df['code'].apply(lambda x: swap_prefix(x))
    return pd.Series(index=prices_df['aqmsymbol'].values, data=prices_df['last_price'].values).to_dict()


def swap_prefix(symbol='000001.SZ'):
    a,b = symbol.split('.')
    return '.'.join([b,a])

def parse_symbol(symbol='000001',method='aqmalgo'):
    if symbol.startswith('6'):
        prefix = 'SH'
    else:
        prefix = 'SZ'
    if method == 'aqmalgo':
        return '.'.join([symbol,prefix])
    else:
        return '.'.join([prefix, symbol])

def get_target_weight():
    fname = 'fake_weight.csv'
    weight_df = pd.read_csv(fname,index_col=0)
    return weight_df['weight'].to_dict()

def get_current_pos(exchange='shipane'):
    params = {
        'token': 'dX8mPbl4RdY4UqzF2FGd',
        'exchange': exchange
    }
    try:
        reply = session.get('http://127.0.0.1:8000/position', params=params)
        position = reply.json()
        holdings = {}
        holdings['cash'] = float(position['cash'])

        holdings_df = pd.DataFrame(position['holdings'])
        holdings_df['symbol'] = holdings_df['symbol'].apply(lambda x: parse_symbol(x))
        holdings['stocks'] = pd.Series(index=holdings_df['symbol'].values, data=holdings_df['quantity'].values).astype(
            'float').to_dict()

        return holdings
    except:
        logger.error()

def get_current_weight(position={}):
    holdings = position['holdings']
    cash = float(position['cash'])
    holdings_df = pd.DataFrame(holdings)
    holdings_df['symbol'] = holdings_df['symbol'].apply(lambda x: parse_symbol(x))
    positions = pd.Series(index=holdings_df['symbol'].values, data=holdings_df['quantity'].values).astype('float').to_dict()
    symbols = list(holdings_df['symbol'])
    futusymbols = [swap_prefix(x) for x in symbols]
    prices = get_futu_rtdata(futusymbols)
    position_df = pd.concat([pd.Series(prices).rename('price'), pd.Series(positions).rename('position')],axis=1)
    position_df['value'] = position_df['price'] * position_df['position']
    port_value = position_df['value'].sum() + cash
    position_df['weight'] = position_df['value']/port_value
    return position_df['weight'].to_dict()

def calculate_port_value(holdings={}):
   symbols = list(holdings['stocks'].keys())
   futusymbols = [swap_prefix(x) for x in symbols]
   prices = get_futu_rtdata(futusymbols)
   position_df = pd.concat([pd.Series(prices).rename('price'), pd.Series(holdings['stocks']).rename('position')], axis=1)
   position_df['value'] = position_df['price'] * position_df['position']
   return position_df['value'].sum() + holdings['cash']



def calculate_target_pos(init_cap, target_weight, board_lot,
                         buffer_ratio=0., buffer_amount = 50):
    init_cap -= min(buffer_amount, init_cap*buffer_ratio)
    prices = get_futu_rtdata([swap_prefix(x) for x in list(target_weight.keys())])
    data_df = pd.concat([pd.Series(prices).rename('price'), pd.Series(target_weight).rename('weight')],
                            axis=1)
    data_df['board_lot'] = board_lot
    data_df['entry_price'] = data_df['price'] * board_lot
    data_df['tar_pos'] = init_cap * data_df['weight'] /data_df['price']
    data_df['tar_pos_round'] = np.floor(init_cap * data_df['weight'] /data_df['entry_price']) * board_lot
    data_df['diff'] = (data_df['tar_pos'] - data_df['tar_pos_round'])/board_lot
    adj_cash = init_cap - (data_df['tar_pos_round']*data_df['price']).sum()
    data_df = data_df.sort_values(by='diff',ascending = False)
    data_df['flag'] = np.cumsum(data_df['entry_price'])/adj_cash
    data_df['tar_pos_adj'] = data_df.apply(lambda x: x['tar_pos_round'] + x['board_lot'] if x['flag']<=1 else x['tar_pos_round'],axis=1)

    # init_cap *= (1 - buffer)
    # entry_price = price * board_lot
    # opt_entry_num = opt_weight * init_cap / entry_price
    # buy_entry_num = np.floor(opt_entry_num)
    # buy_stock_num = buy_entry_num * board_lot

    return data_df['tar_pos_adj'].to_dict()


def calculate_shares(current_pos ={}, tar_pos = {}):
    data_df = pd.concat([pd.Series(current_pos).rename('current'), pd.Series(tar_pos).rename('target')],
                            axis=1).fillna(0.)

    data_df['diff'] = data_df['target'] - data_df['current']

    orders = {}

    orders['sell'] = data_df[data_df['diff']<0]['diff'].to_dict()
    orders['buy'] = data_df[data_df['diff']>0]['diff'].to_dict()

    return orders

def parse_orders(orders={},model = 'demo'):
    if len(orders):
        order_list = [
            {
                "symbol": idx,
                "amount": abs(amount),
                "action": 'SELL' if amount <0 else 'BUY',
                "type": 'market',

            } for idx, amount in orders.items()
        ]
        submit_orders(order_list)


def get_rt_data(weight_dict={}):
    symbols = list(weight_dict.keys())
    futusymbols = [swap_prefix(x) for x in symbols]
    return get_futu_rtdata(futusymbols)

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

def main():
    while(1):
        value = calculate_port_value(get_current_pos())
        tar_weight = get_target_weight()
        tar_pos = calculate_target_pos(value,tar_weight,100,0.)
        cur_pos = get_current_pos('shipane')
        orders = calculate_shares(cur_pos['stocks'],tar_pos)

        sell_orders = orders.get('sell', {})
        buy_orders = orders.get('buy', {})

        if len(sell_orders):
            parse_orders(sell_orders)
        elif len(buy_orders):
            parse_orders(buy_orders)
        else:
            break
        time.sleep(60)


    # cur_weight = get_current_weight(cur_pos)


    # shares = calculate_shares(cur_weight,tar_weight)
    # rt_price = get_rt_data(weight)
    # data = pd.concat([pd.Series(rt_price).rename('price'), pd.Series(weight).rename('weight')],axis=1)
	#
    # shares = get_buy_share_number(init_cap = 500000, opt_weight=data['weight'].values, price= data['price'].values, board_lot=100, buffer=0.)
	#
    # shares_dict = pd.Series(index = list(data.index), data=shares).to_dict()

    # initial_submit()


if __name__ == "__main__":
    main()
