# -*- coding: utf-8 -*-
# @Time    : 7/22/2019 2:06 PM
# @Author  : Joseph Chen
# @Email   : joseph.chen@magnumwm.com
# @FileName: trading.py
# @Software: PyCharm
import os
import sys
import time

import pandas as pd

from ...data.choice_proxy_client import choice_client
from ...data.RQClient import RQClient
from ..log.get_logger import get_logger
from ..market.market import market
from ..symbol.base import symbol_converter
from ..util.trading_utils import process_ctp_arbitrage_contract

logger = get_logger(__name__)
order_status = {}
cancelled_orders = {}


def trading_callback(msg, source: str):
    global order_status
    global cancelled_orders
    # assert len(msg.keys())==1, "Each msq is supposed to contains one order only!"

    if msg['source'] != source:  # 过滤非本source的callback 使多个executor运行不互相干扰
        logger.warning(f'source not match, will ignore this callback message: {msg}')
        return

    order_status[msg['id']] = msg

    # Remove `REJECTED` & `CANCELLED` as we don't want to track orders that have been rejected or cancelled.
    # 忘了为什么要把cancelled的也删掉 为了不使逻辑改动太大 把cancelled的移动到另一个dict中
    if msg['status'] == 'REJECTED':
        logger.warning('status of oid {} is {}, see details below:'.format(msg['id'], msg['status']))
        order_status.pop(msg['id'], None)
    elif msg['status'] == 'CANCELLED':
        logger.info('status of oid {} is {}, did you just cancel this order? details:'.format(msg['id'], msg['status']))
        cancelled_orders[msg['id']] = order_status.pop(msg['id'])

    logger.debug('callback: {}'.format(msg))
    return


class Trading:
    """
        封装一些更高级的交易操作
        
        外部调用时，应import trading实例
    """

    def execute_sellbuy_cont(self, orders_sell: list, orders_buy: list, split_asset: float, trading_controller, paper,
                             timeout=60, check_status_period=3, order_type='LIMIT'):
        """ 输入一批买卖订单，分批先卖后买，使整体账户的持仓不至于突然变为空仓再满仓。
            其中每次卖出的小批订单的总金额最多为刚好超过给定的`split_asset`值，
            然后再用卖出得到的钱买入股票
        """
        remain_cash = 0
        orders = []
        orders_sell_sort = sorted(orders_sell, key=lambda order: order["price"]*order["quantity"])
        orders_buy_sort = sorted(orders_buy, key=lambda order: order["price"]*order["quantity"])

        for buy_order_index in range(len(orders_buy_sort)):
            buy_order = orders_buy_sort[buy_order_index]
            buy_order_money = buy_order["price"] * buy_order["quantity"]
            if remain_cash < buy_order_money:
                for sell_order_index in range(len(orders_sell_sort)):
                    sell_order = orders_sell_sort[sell_order_index]
                    remain_cash += sell_order["price"] * sell_order["quantity"] * 0.998  # 考虑到潜在的手续费扣除
                    if remain_cash > split_asset or sell_order_index == len(orders_sell_sort)-1:  # 卖足够的股票后停止
                        orders_sell_temp = list(orders_sell_sort[:(sell_order_index+1)])
                        orders.append(orders_sell_temp)
                        orders.append([])
                        orders_sell_sort = list(orders_sell_sort[(sell_order_index+1):])
                        break
            if not orders:  # 只有买单，没有卖单的情况
                orders.append([])
            orders[-1].append(buy_order)
            remain_cash -= buy_order_money
        if orders_sell_sort:
            orders.append(orders_sell_sort)

        trade_result = []
        for order in orders:
            result = self.execute_orders(order, trading_controller, paper, order[0]["side"], timeout, check_status_period, order_type)
            trade_result.append(result)
        return pd.concat(trade_result)

    def execute_sell_then_buy(self, orders_sell:list, orders_buy: list, trading_controller, paper,
                              timeout=60, check_status_period=3, order_type='LIMIT'):
        """ 先执行卖单，在卖单*全部*成交后，再执行买单，更适合现金账户

            - orders_sell: 卖单
            - orders_buy: 买单
            - timeout: 等待完全成交的总时间
            - check_status_period: 每次检查是否完全成交之间的间隔
        """
        sell_results = self.execute_orders(orders_sell, trading_controller, paper, 'sell', timeout, check_status_period, order_type)
        buy_results = self.execute_orders(orders_buy, trading_controller, paper, 'buy', timeout, check_status_period, order_type)
        return pd.concat([sell_results, buy_results])

    def execute_orders(self, orders_all: list, trading_controller, paper, buy_sell='all', timeout=60, check_status_period=3, order_type='LIMIT', re_submit=99):
        """ 提交订单，并检查成交状态，进行超时改单操作，直到完全成交

            - timeout: 等待完全成交的总时间
            - check_status_period: 每次检查是否完全成交之间的间隔
			- buy_sell: buy/sell/all，将影响log中的文字输出；其中all为合并提交买卖订单，适合孖展账户
        """
        assert buy_sell.lower() in ('all', 'buy', 'sell')
        global order_status
        global cancelled_orders

        all_filled = False
        results = []

        if not orders_all:
            logger.info(f'No {buy_sell} orders.')
        else:
            while not all_filled:
                # Submit orders
                order_status = {}  # to record instantaneous status of each order
                logger.info('***** SUBMITTING ORDERS: ' + str(orders_all))
                submitted_orders = trading_controller.submit_orders(baccount=paper['account'],
                                                                    brokerid=paper['broker'],
                                                                    orders=orders_all)
                logger.info('***** INIT ORDER INFO FROM SERVER: ' + str(submitted_orders))
                if submitted_orders['status']['ecode'] != 0:
                    logger.error("Submitted orders failed.\n{}".format(submitted_orders))
                    raise RuntimeError('Submitted orders failed, read log for more infomation')
                order_details = submitted_orders['data']

                # keep checking status until all orders are filled. If timeout, we have to modify orders
                toc = tic = time.time()
                while toc - tic < timeout:
                    time.sleep(check_status_period)  # sleep放在前面 使获取unfilled_orders与下面的cancel_order之间的时间间隔减小
                    unfilled_orders, filled_orders, partial_filled_orders = self.wait_and_check_unfilled_orders(order_details)  # 若未收齐callback 将可能超过timeout的时限 使这行代码耗时超过timeout
                    if len(unfilled_orders) == 0:
                        all_filled = True
                        break
                    toc = time.time()
                if unfilled_orders:
                    logger.info("UNFILLED orders AFTER TIMEOUT:\n{}".format(unfilled_orders))

                # record results, including filled and partial filled records
                results.extend(filled_orders)
                results.extend(partial_filled_orders)

                # If all orders are filled, execution is done.
                if all_filled:
                    logger.info(f"Successfully execute {buy_sell} orders.")
                    break

                while self._check_odd_lot_existence(unfilled_orders):
                    logger.warning(f'odd lot found in unfilled orders! please wait or check details: {unfilled_orders}')
                    unfilled_orders, _, _ = self.wait_and_check_unfilled_orders(order_details)
                    time.sleep(1)

                # Else we have to modify orders
                # Step 1: cancel unfilled orders
                unfilled_orderids = [o['id'] for o in unfilled_orders]
                for oid in unfilled_orderids:
                    cancel_result = trading_controller.cancel_order(oid)
                    if cancel_result.get('status').get('ecode')==0:
                        logger.info("Successfully cancelled unfilled orderid {}".format(oid))
                    else:
                        logger.error("Unfilled orderid {} unable to cancel, as: {}.".format(oid, cancel_result['status'].get('message')))
                        if 'status = FILLED' in cancel_result['status'].get('message'):  # 避免因为已成交所以撤单失败 后续又重复提交订单的情况
                            _filled_order = [order for order in unfilled_orders if order['id']==oid][0]  # 找到oid对应的order 用一行来写 不占用多行
                            unfilled_orders.remove(_filled_order)
                            logger.warning('removed order {} from unfilled orders'.format(oid))

                self.wait_until_all_cancelled_or_filled(unfilled_orderids)

                if not re_submit:
                    logger.warning('will not try to re-submit order')
                    break
                re_submit -= 1

                logger.info('***** Need to MODIFY the existing orders!')

                # Step 2: modify orders
                if unfilled_orders:
                    orders_sell, orders_buy = self.recreate_order_basket(unfilled_orders=cancelled_orders.values(),
                                                                         order_type=order_type)
                    orders_all = orders_sell + orders_buy
                else:
                    all_filled = True

                # Step 3: reset order status
                order_status = {}
                cancelled_orders = {}

        return pd.DataFrame(results)

    def _check_odd_lot_existence(self, unfilled_orders:list):
        """ 由于IB对odd lot非常不友好（出现了odd lot就close不掉），所以应尽量避免这种情况 """
        for unfilled_order in unfilled_orders:
            unfilled_quantity = unfilled_order['quantity'] - unfilled_order['filledQuantity']
            iuid = unfilled_order['iuid']
            lot_size = (self.lot_size[iuid]).item()
            if unfilled_quantity % lot_size != 0:
                return True  # 存在odd lot
        return False

    def liquidate(self, current_positions: pd.DataFrame, region=None):
        raise NotImplementedError('not tested yet')
        # if region is None:
        #     region = self.region

        # # Get real time market data
        # iuids = list(current_positions.iuid.values)
        # iuids = [i for i in iuids if region in i]
        # # rt_data = self.get_real_time_data(iuids=iuids, region=region)
        # rt_data = self.get_real_time_data()
        # rt_data = rt_data[rt_data['iuid'].isin(iuids)]
        # # To create order basket, we need to calculate order_size and delta_position
        # current_positions = pd.merge(current_positions, rt_data, on='iuid')

        # # Simply current_positions['delta_position'] = 0.0 - current_positions['holdingPosition'] is NOT enough
        # # we must handle the order lot size.
        # current_positions['delta_position'] = current_positions[['holdingPosition','ORDER_LOT_SIZE']].apply(
        #     lambda x: self._estimate_delta_position(x),
        #     axis=1,
        # )

        # current_positions['order_size'] = (current_positions['delta_position'] *
        #                                    (current_positions['BID_1'] + current_positions['ASK_1']) / 2.
        #                                    )
        # # Now we are ready to create order basket
        # orders_sell, orders_buy = self.create_order_basket(target_positions=current_positions,
        #                                                    order_type='MARKET')
        # # Execute the orders
        # self.execute_sell(orders_sell, order_type='MARKET')
        # self.execute_buy(orders_buy, order_type='MARKET')
        # return

    @staticmethod
    def cancel_all_active_orders(trading_controller, paper):
        brokerid = paper['broker']
        baccount = paper['account']
        all_orders = trading_controller.get_account_orders(brokerid=brokerid,
                                                           baccount=baccount).get('data')
        if all_orders is None:
            return
        if not isinstance(all_orders, list):
            return
        if len(all_orders)==0:
            return
        for order in all_orders:
            orderid = order['id']
            status = order['status']
            if status in ('FILLED', 'CANCELLED', 'REJECTED'):
                continue
            if status in ('FILLING', 'PENNDING', 'CREATE'):  ################################  PENNDING 拼错了?????
                logger.info(f'* orderid {orderid} ({order["iuid"]}) is {status}')
                cancel_result = trading_controller.cancel_order(orderid)
                if cancel_result['status']['ecode']!=0:
                    raise ValueError(f'Fail to cancel orderid {orderid}. '
                                     f'Check {trading_controller.endpoint}trading/order?orderId={orderid} for details. '
                                     f'cancel_result: {cancel_result}')
                logger.info("Successfully cancel orderid {}".format(orderid))
            else:
                logger.warning("! orderid {} is {}, status is weird!!!".format(orderid, status))
        return

    def get_current_cash_info(self, trading_controller, paper: dict, target_currency: str, is_subaccount=False) -> dict:
        """ 返回当前按市值计算的资产信息 对于单币种账户和多币账户 有不同的逻辑 多币种会进行货币换算 """
        t = 5
        while t:
            cash_info = trading_controller.get_cash_info(baccount=paper['account'],
                                                         brokerid=paper['broker'], is_subaccount=is_subaccount)
            logger.debug('cash info from server: ' + str(cash_info))

            if len(cash_info) == 1 and target_currency in cash_info:  # 单币种账户 可以直接返回
                return cash_info[target_currency]
            else:  # 多币种账户 可以获取到以某一货币为单位的总asset 如查询其他币种 先获取到以usd算的数值 再转换为指定的货币
                for valid_currency, valid_currency_dict in cash_info.items():
                    if valid_currency_dict['asset'] is not None:
                        break
                if target_currency == valid_currency:
                    return valid_currency_dict

                # currency_info = choice_client.csqsnapshot("USDCNH.FX,USDHKD.FX,USDCNY.EX", "Date,Time,Now")

                # rq_client = RQClient(iuid_list=['GB_40_USDHKD', 'GB_40_USDCNY', 'GB_40_HKDCNH'])
                # time.sleep(5)
                # currency_info = rq_client.orderbook_data
                # assert currency_info, 'currency info not exist, is bbg down????'
                # rq_client.disconnect_client()
                # rates = {
                #     'CNH': currency_info['GB_40_USDHKD']['b1'] * currency_info['GB_40_HKDCNH']['b1'],  # about 6~7
                #     'CNY': currency_info['GB_40_USDCNY']['b1'],  # about 6~7
                #     'HKD': currency_info['GB_40_USDHKD']['b1'],   # about 7.8
                #     'USD': 1
                # }

                import io
                from ...data.oss_client import oss_client
                csv_bytes = oss_client.download_file_to_stream('algo_it_raw_data/reuters/FX/ALGOPRO_PRICE_FX.csv')
                csv_file = io.BytesIO(csv_bytes)  # 把bytes转为一个类file object
                currency_info = pd.read_csv(csv_file).set_index('iuid').to_dict()  # 无index 有标题  支持URL格式的路径

                rates = {
                    'CNH': currency_info['price']['GB_40_USDCNH'],  # about 6~7
                    'CNY': currency_info['price']['GB_40_USDCNY'],  # about 6~7
                    'HKD': currency_info['price']['GB_40_USDHKD'],   # about 7.8
                    'USD': 1
                }
                if valid_currency == 'USD':
                    usd_asset = valid_currency_dict['asset']
                    usd_balance = cash_info['BASE']['balance'] if 'BASE' in cash_info else \
                                    valid_currency_dict['balance']
                    if usd_balance is None:
                        logger.warning('usd_balance value is missing, will retry')
                        t -= 1
                        continue
                else:
                    usd_asset = valid_currency_dict['asset'] / rates[valid_currency]  # 把当前能获取到的asset转为usd
                    usd_balance = cash_info['BASE']['balance'] / rates[valid_currency] if 'BASE' in cash_info else \
                                    valid_currency_dict['balance'] / rates[valid_currency]
                    if usd_balance is None:
                        logger.warning('usd_balance value is missing, will retry')
                        t -= 1
                        continue

                asset = usd_asset * rates[target_currency]
                balance = usd_balance * rates[target_currency]

                result = {'asset': asset, 'balance': balance}
                return result
        raise RuntimeError(f'get cash info error {cash_info}')

    def _drop_zero_holding_holdings(self, positions: pd.DataFrame) -> pd.DataFrame:
        """ 当天卖出股票后 会留下0股的持仓 T+1才会消除该记录 所以需要手动过滤掉避免多余的操作 """
        if positions.empty:
            return positions
        positions = positions[positions['holdingPosition'] != 0.0]
        return positions

    def _drop_nonspecified_region_holdings(self, positions: pd.DataFrame, region: str) -> pd.DataFrame:
        """ 返回包括self.region的行 """
        if positions.empty:
            return positions
        positions = positions[positions['iuid'].str.contains(region)]
        return positions

    def get_current_positions(self, trading_controller, paper, region, is_subaccount=False) -> pd.DataFrame:
        """ 返回当前region的非零持仓
            
            对于子账户 直接从后端拿
            对于主账户 先尝试直接从broker拿 失败则回退到老方法 """
        if is_subaccount:
            logger.debug('Get holdings of subaccount..')
            holding_info = trading_controller.get_holdings(brokerid=paper['broker'], baccount=paper['account'])
        else:
            try:
                logger.debug('Get holdings from broker..')
                holding_info = trading_controller.get_broker_holdings(brokerid=paper['broker'],
                                                                      baccount=paper['account'])
            except BaseException:
                logger.warning('Get holdings from broker failed, will try to get from backend.')
                holding_info = trading_controller.get_holdings(brokerid=paper['broker'],
                                                               baccount=paper['account'])
        holdings = process_ctp_arbitrage_contract(holding_info)
        holdings = pd.DataFrame(holdings)
        holdings = self._drop_nonspecified_region_holdings(holdings, region)
        holdings = self._drop_zero_holding_holdings(holdings)
        if len(holdings) > 0:  # 防止空仓时执行下面的语句报错
            holdings['iuid'] = holdings['iuid'].apply(symbol_converter.from_brokersymbol_to_iuid)  # 处理一些iuid的特殊情况
            holdings = holdings.sort_values(by='iuid')
        elif len(holdings) == 0:  # 空仓时 需要手动添加2列
            holdings['iuid'] = pd.Series()
            holdings['holdingPosition'] = pd.Series()
        return holdings

    def wait_and_check_unfilled_orders(self, orders:list):
        """
            主动检查order状态(阻塞主线程至全部order都收到回调)
            使用后端返回的oid 与callback的各orders状态比对 返回各orders的最新状态的列表
        """
        submitted_order_ids = [o['id'] for o in orders]  # 后端返回的order id

        global order_status
        submitted_order_iuids = set([o['iuid'] for o in orders])  # 后端返回的order iuid   在本函数运行时不变
        callback_order_iuids = set([v['iuid'] for k,v in order_status.items()])  # 在本函数运行时可能被其他线程改变
        # wait until all the submitted orders have callbacks
        while callback_order_iuids < submitted_order_iuids:  # 若callback收到的iuid是后端返回的iuid的真子集 则继续等待 按iuid来比较最准确
            logger.info(f'Waiting for "{submitted_order_iuids - callback_order_iuids}" to update fill status.. (if too slow, maybe it was rejected?)')
            callback_order_iuids = set([v['iuid'] for k,v in order_status.items()])  # 在本函数运行时可能被其他线程改变 更新
            time.sleep(1)

        unfilled_orders = []
        filled_orders = []
        partial_filled_orders = []
        for oid in submitted_order_ids:
            if oid not in order_status:
                raise ValueError("Fail in filling orders.\norder {} not in callback order_status:\n{}".format(oid, order_status))
            # 共有'FILLED', 'FILLING', 'PENNDING', 'CREATE', 'CANCELLED', 'REJECTED' 6种, 其中'CANCELLED', 'REJECTED'在callback中被过滤
            oid_status = order_status[oid]['status']
            if oid_status != 'FILLED':  # FILLING + PENNDING + CREATE
                logger.info('***** unfilled order: ' + str(order_status[oid]))
                unfilled_orders.append(order_status[oid])
                if oid_status == 'FILLING' and order_status[oid]['filledQuantity']>0.0:
                    partial_filled_orders.append(order_status[oid])
            else:  # FILLED
                # logger.debug('filled order: ' + str(order_status[oid]))
                filled_orders.append(order_status[oid])

        return unfilled_orders, filled_orders, partial_filled_orders

    def recreate_order_basket(self, unfilled_orders: list, order_type='LIMIT'):
        """
            按之前获取数据的途径 再次获取实时数据
        """
        orders_sell = []
        orders_buy = []
        iuids = [order['iuid'] for order in unfilled_orders]
        if not iuids:
            logger.warning('no iuid input, may have filled')
            return orders_sell, orders_buy

        # 重新获取unfilled_orders的实时价格
        real_time_data = market.get_real_time_data(iuids)
        logger.debug('realtime data for generating new orders\n' + str(real_time_data))

        for o in unfilled_orders:
            side = o['direction']
            iuid = o['iuid']
            quantity = o['quantity']
            filled_quantity = o['filledQuantity']
            source = o['source']
            lot_size = (self.lot_size[iuid]).item()  # 转为python原生数据类型 self.lot_size在外部通过trading.lot_size赋值
            # 处理部分成交时撤单导致碎股的问题 向下取整(int)而不是四舍五入 避免卖出超过最大可卖数量 虽然可能会残留一点碎股
            modified_quantity = int((quantity - filled_quantity) / lot_size) * lot_size
            # If for some reason, the order has already been filled, we won't bother modifying it any more.
            if modified_quantity == 0:
                continue
            if side == 'SELL':
                price = real_time_data.loc[iuid]['BID_1']
                # minchg = self.minchg_series[iuid]  TODO
                # price = round(price / minchg) * minchg
                order_sell = dict(
                    type=order_type,
                    side=side,
                    source=source,
                    symbol=iuid,
                    quantity=modified_quantity,
                    price=round(price, 3) if price < 1000 else round(price, 2)  # TODO 最终应在choice端修复
                )
                orders_sell.append(order_sell)
            elif side == 'BUY':
                price = real_time_data.loc[iuid]['ASK_1']
                # minchg = self.minchg_series[iuid]  TODO
                # price = round(price / minchg) * minchg
                order_buy = dict(
                    type=order_type,
                    side=side,
                    source=source,
                    symbol=iuid,
                    quantity=modified_quantity,
                    price=round(price, 3) if price < 1000 else round(price, 2)  # TODO 最终应在choice端修复
                )
                orders_buy.append(order_buy)

        from ..strategy.base_strategy import configs
        if configs and configs.confirm:
            input(f'RECREATE ORDERS:\nsell: {orders_sell}\nbuy: {orders_buy}\npress enter to submit')  # for debugging
        return orders_sell, orders_buy

    def wait_until_all_cancelled_or_filled(self, unfilled_orderids: list):
        """ 在发送撤单请求的过程中，可能刚好有订单变为`已成交`的状态，所以需考虑2种情况 """
        global order_status

        while True:
            oids = unfilled_orderids.copy()
            logger.debug('wait for all unfilled orders to become cancelled..')
            for oid in oids:
                # 在callback中 若cancelled会删除该项
                if oid not in order_status.keys() or order_status[oid]['status'] == 'FILLED':
                    unfilled_orderids.remove(oid)

            if not unfilled_orderids:
                break
            else:
                logger.debug(f'still not cancelled: {unfilled_orderids}')
            time.sleep(1)

    def get_subaccounts(self, trading_controller, broker_account) -> pd.DataFrame:
        """ 查询子账户的基本信息 """
        subaccounts = trading_controller.get_subaccount_list(broker_account)
        return pd.DataFrame(subaccounts)


trading = Trading()  # 供外部调用
