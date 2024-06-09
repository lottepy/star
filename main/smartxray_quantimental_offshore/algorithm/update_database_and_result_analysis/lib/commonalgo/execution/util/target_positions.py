# -*- coding: utf-8 -*-
# @Time    : 7/22/2019 12:00 PM
# @Author  : Joseph Chen
# @Email   : joseph.chen@magnumwm.com
# @FileName: target_positions.py
# @Software: PyCharm

import pandas as pd
import numpy as np

from ..log.get_logger import get_logger
logger = get_logger(__name__)
from ..util import trading_utils


def cal_stock_position(target_value, item_price, lot_size, method='floor'):
    """
    Parameters: scalar or np.array

    Returns: same type as input
    """
    if method=='floor':
        position = np.floor(target_value / (item_price * lot_size)) * lot_size
    elif method=='round':
        position = np.round(target_value / (item_price * lot_size)) * lot_size
    elif method=='ceil':
        position = np.ceil(target_value / (item_price * lot_size)) * lot_size
    # if position nan
    return position

def calculate_positions_from_weights_fill_cash(
        total_value,
        target_weights,
        price,
        lot_size,
        cash_buffer,
        current_position,
        buy_sell_direction=None,) -> pd.Series:
    # TODO: logger
    target_value = total_value * target_weights

    est_position = cal_stock_position(target_value, price, lot_size, 'floor')
    est_value = est_position * price

    sorting_function=abs(est_value-target_value)-abs(est_value+lot_size*price-target_value)
    sorted_index = np.argsort(sorting_function)[::-1]

    cash_remaining = total_value - cash_buffer - np.sum(est_value)
    cash_remaining=_fill_positions_cash(sorted_index, cash_remaining, est_position, lot_size, price,sorting_function)
    #return est_position

    if cash_remaining>0:
        est_value = est_position * price
        sorting_function=lot_size*price-(abs(est_value+lot_size*price-target_value)-abs(est_value-target_value))
        sorted_index = np.argsort(sorting_function)[::-1]
        cash_remaining=_fill_positions_cash(sorted_index, cash_remaining, est_position, lot_size, price,sorting_function)

    return est_position

def _fill_positions_cash(sorted_idxs, cash_remaining, est_position, lot_size, price,sorting_function):
    for i in sorted_idxs:
        if cash_remaining>lot_size[i]*price[i] and sorting_function[i]>0:
            est_position[i]+=lot_size[i]
            cash_remaining-=lot_size[i]*price[i]
    return cash_remaining

    













    

def calculate_positions_from_weights_simple(
        total_value,
        target_weights,
        price,
        lot_size,
        cash_buffer,
        current_position,
        buy_sell_direction=None,) -> pd.Series:
    target_value = target_weights * total_value
    est_position = cal_stock_position(target_value, price, lot_size, 'round')
    return est_position


def calculate_positions_from_weights(
        current_asset: float,
        target_weight,
        current_position,
        price,
        multiple,  # lot size
        buy_sell_direction=None,
        strategy='simple',
        est_commission_rate=0.0,
        maximum_capital=100000.,
        minimum_capital=70000.,
        minimum_cash_buffer=50.,
        percentage_of_cash_buffer=0.005) -> pd.Series:

    """
    Calculate target position change with minimum drift, rounded by minimun value

    For futures the position represents a lot

    Parameters:
    current_asset (float): estimated total asset, cash + market capital
    target_weight (pd.dataframe):
        index - iuid,
        columns - iuid,symbol, weight,target_position
    current_price (dict or pd.Series): prices
        index - iuid
    multiple (dict or pd.Series): lot size
        index - iuid
    est_commission_rate (float): estimated commission rate [0, 1]
    strategy: is deprecated

    Returns:
    pd.Series: target positions
    """
    if strategy not in {'simple', 'min_drift'}:
        raise ValueError(f"Strategy '{strategy}' Not Implemented.")

    if est_commission_rate < 0 or est_commission_rate > 1:
        logger.warning(f"Commission Rate Abnormal: {est_commission_rate}.")
        raise ValueError(f"Commission Rate Abnormal: {est_commission_rate}.")

    if target_weight.index.name != 'iuid':
        target_weight.set_index('iuid', inplace=True)

    total_value = current_asset

    logger.info(f"Current Asset: {total_value}")

    # If current asset is less than minimum_capital(70000),
    # we use margin account to initialize capital to maximum_capital(100000.)
    # if total_value < minimum_capital:
    #     confirm = input(f"Total value {total_value} is less than minimum" \
    #                     f" required {maximum_capital}. Use margin account to reset" \
    #                     f" the total value to maximum capital {maximum_capital}" \
    #                     f" (Note: if not agree, will terminate the program)\n" \
    #                     f"1. Confirm to continue?(Y/N)\n"  \
    #                     f"2. No, I want to use only {total_value} to proceed." \
    #                     f" (N/n)\n" \
    #                     f"3. I don't want to proceed. (Hit other keys)\n"
    #            )
    #     if confirm in ('Y', 'y'):
    #         logger.info(f"Use margin account to reset total_value to {maximum_capital}")
    #         total_value = maximum_capital
    #     elif confirm in ('N', 'n'):
    #         logger.info(f"Use only total_value {total_value} to trade.")
    #     else:
    #         logger.info(f"User termination.")
    #         raise ValueError("Program is terminated.")
    # elif total_value > maximum_capital:  # TODO: allow epsilon?
    #     confirm = input("Total value {}(cash+market value of holdings) ".format(total_value)+
    #                     "is more than maximum required {}. We simply reset ".format(maximum_capital) +
    #                     "the total value to {}\n".format(maximum_capital) +
    #                     "1. Confirm to continue (Y/y)\n" +
    #                     "2. No, I want to rebalance with all {} (N/n)\n".format(total_value) +
    #                     "3. I don't want to proceed. (Hit other keys)\n"
    #                     )
    #     if confirm in ('Y', 'y'):
    #         logger.info(f"Use {maximum_capital} out of {total_value}.")
    #         total_value = maximum_capital
    #     elif confirm in ('N', 'n'):
    #         logger.info(f"Reblance with {total_value}.")
    #     else:
    #         logger.info(f"User termination.")
    #         raise ValueError("Program is terminated.")

    # If we consider commission, est_commission_rate should be larger than 0
    total_value = total_value * (1 - est_commission_rate)

    cash_buffer = max(minimum_cash_buffer,
                      total_value * percentage_of_cash_buffer)

    # merge target_weigt index with current_position
    df = pd.concat([target_weight,
                    current_position.rename('current_position')],
                   axis=1, join='outer')


    # set current to zero if not in target
    target_mask = df['target_weight'].notnull()

    current2zero = df[~target_mask]['target_position'].fillna(0)
    df = df[target_mask]
    
    futures, stocks = trading_utils.split_future_stock_df(df)
    future_pos = future_stock_positions_from_weights(futures, cash_buffer,
                                               total_value,
                                               price,
                                               multiple,
                                               current_position,
                                               buy_sell_direction,
                                                     True)
    stock_pos  = future_stock_positions_from_weights(stocks, cash_buffer,
                                               total_value,
                                               price,
                                               multiple,
                                               current_position,
                                               buy_sell_direction,
                                                     False)
    return pd.concat([future_pos, stock_pos, current2zero]).rename('holdingPosition')

def future_stock_positions_from_weights(df, cash_buffer,
                                        total_value,
                                        price,
                                        multiple,
                                        current_position,
                                        buy_sell_direction,
                                        is_futures=False,):
    if df is None or not len(df):
        return pd.Series()
    # bring out target_position
    target_pos_mask = df['target_position'].notnull()
    if target_pos_mask.all():
        # No weighting input
        return df['target_position']

    existing_positions = df['target_position'][target_pos_mask].astype(int)
    df = df[~target_pos_mask]

    # hold_out_assets = 0
    # if not is_futures:
    #     for idx, position in existing_positions.items():
    #         # stock, can also short
    #         hold_out_assets += position*price[idx]
    hold_out_assets = np.sum(existing_positions*price[existing_positions.index]
                            ) if not is_futures else 0

    weights = df['target_weight'].fillna(0)
    current_positions = df['current_position'].fillna(0)

    # reorder index, use np.array
    indices = weights.index
    weights = weights.values
    price = price[indices].values
    lot_size = multiple[indices].values.astype(int)

    # estimate position, value and drift
    target_value = weights * total_value

    if total_value < 0:
        raise ValueError(
            f"There is not sufficient money({total_value}) to trade!")

    if is_futures:
        f = calculate_positions_from_weights_simple
    else:
        f = calculate_positions_from_weights_fill_cash

    est_position = f(total_value, weights, price, lot_size, cash_buffer+hold_out_assets,
        current_position, buy_sell_direction)


    # bring back indices
    target_positions = pd.Series(est_position, index=indices, dtype=int,)

    if not is_futures:
        # TODO: re-weighting, fill cash, based on max drift
        cal_values = target_positions*lot_size
        cash_remaining = total_value - cal_values
    
    if is_futures:
        for idx, position in target_positions.items():
            target_positions[idx] = target_positions[idx]// multiple[idx]
    target_positions = pd.concat((existing_positions, target_positions))

    return target_positions


def calculate_positions_from_weights_with_backup(
    current_asset: float,
    target_weight,
    current_position,
    price,
    multiple,  # lot size
    backup=[],
    notrade=[],
    nosell=[],
    nobuy=[],
    buy_sell_direction=None,
    strategy='simple',
    est_commission_rate=0.0,
    maximum_capital=100000.,
    minimum_capital=70000.,
    minimum_cash_buffer=50.,
    percentage_of_cash_buffer=0.005) -> pd.Series:

    targetholding=pd.DataFrame(columns=['iuid', 'holdingPosition'])
    targetholding=targetholding.set_index('iuid')
    sellingblock=[]
    for iuid in current_position.index:
        if (iuid not in target_weight["target_weight"].index or target_weight["target_weight"][iuid]*current_asset<current_position[iuid]*price[iuid]) and (iuid in nosell or iuid in notrade):
            sellingblock.append(iuid)

    while sellingblock:
        df=pd.DataFrame(current_position)
        df=df[df.index.isin(sellingblock)]
        targetholding=targetholding.append(df)
        target_weight=target_weight.drop(list(set(sellingblock) & set(target_weight.index)))
        holding_value=(df['holdingPosition'] * price).fillna(0)
        current_asset -= sum(holding_value)
        target_weight['target_weight']=target_weight['target_weight']/sum(target_weight['target_weight'])

        sellingblock=[]
        for iuid in target_weight["target_weight"].index:
            if (iuid in current_position and target_weight["target_weight"][iuid]*current_asset<current_position[iuid]*price[iuid]) and (iuid in nosell or iuid in notrade):
                sellingblock.append(iuid)

    buyingblock=[]
    for iuid in target_weight.index:
        if (iuid not in current_position.index or target_weight["target_weight"][iuid]*current_asset>current_position[iuid]*price[iuid]) and (iuid in nobuy or iuid in notrade):
            buyingblock.append(iuid)
    
    temp_holding_value=0
    for iuid in buyingblock:
        if iuid not in current_position:
            current_position[iuid]=0
        targetholding.loc[iuid]=pd.Series({'holdingPosition':current_position[iuid]})
        backup_symbol=backup[0]
        backup = backup[1:]
        iuid_weight=target_weight['target_weight'][iuid]
        backup_symbol_weight = (iuid_weight * current_asset - price[iuid] * current_position[iuid]) / current_asset
        target_weight.loc[backup_symbol]=pd.Series({'target_weight':backup_symbol_weight})
        target_weight=target_weight.drop(iuid)
        temp_holding_value += price[iuid] * current_position[iuid]
        logger.info(f"backup_symbol:{backup_symbol} helps iuid:{iuid} for weight:{backup_symbol_weight}")
    current_asset -= temp_holding_value
    target_weight['target_weight'] = target_weight['target_weight']/sum(target_weight['target_weight'])

    current_position=current_position.drop(targetholding.index)
    price=price.drop(targetholding.index)
    multiple=multiple.drop(targetholding.index)
    series=calculate_positions_from_weights(current_asset,target_weight,current_position,price,multiple,
                            buy_sell_direction=buy_sell_direction,strategy=strategy,est_commission_rate=est_commission_rate,
                            maximum_capital=maximum_capital,minimum_capital=minimum_capital,minimum_cash_buffer=minimum_cash_buffer
                            ,percentage_of_cash_buffer=percentage_of_cash_buffer)
    #logger.info(f"series:{series}")

    targetholding_series=pd.Series(targetholding['holdingPosition'])
    #logger.info(f"targetholding_series:{targetholding_series}")
    series=series.append(targetholding_series)
    #logger.info(f"total_series:{series}")
    return series
