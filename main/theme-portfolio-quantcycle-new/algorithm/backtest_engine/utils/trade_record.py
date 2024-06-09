import numpy as np


class TradeRecord:
    def __init__(self, timestamp_list, trade_symbol_list,
                 price_list, volume_list, transaction_cost=None, uuid=None, universe=None, strategy_param=None,
                 record_class='fx'):
        self.uuid = uuid  # uuid  -int -example: 14
        self.universe = universe  # symbol series (list)  --example: ["000008.SZ","000009.SZ",]
        self.strategy_param = strategy_param  # strategy parameter series (list_list)  --example: [[0.2,10,],[0.4,5,],]
        self.timestamp_list = timestamp_list
        self.trade_symbol_list = trade_symbol_list
        self.price_list = price_list
        self.volume_list = volume_list
        self.trade_num = len(timestamp_list)
        self.record_class = record_class
        if transaction_cost is None:
            self.transaction_cost = np.zeros_like(trade_symbol_list)
        else:
            self.transaction_cost = transaction_cost

    def array_to_list(self):
        self.price_list = [array.tolist() for array in self.price_list]
        self.volume_list = [array.tolist() for array in self.volume_list]

    def get_direction(self):
        self.direction_list = np.array(["buy" if i > 0 else "sell" for i in self.volume_list])

    def toPair(self):
        self.trade_pair = TradePair(universe=self.universe, strategy_param=self.strategy_param)
        local_transaction = self.transaction_cost.copy()
        local_volume = abs(self.volume_list).copy()
        symbol_menu = np.unique(self.trade_symbol_list)
        order_num = np.arange(0, self.trade_symbol_list.size)
        long_final = []
        short_final = []
        self.get_direction()
        for symbol in symbol_menu:
            tmp_order_list = order_num[self.trade_symbol_list == symbol]
            long_position = []
            short_position = []
            for i in tmp_order_list:
                if self.direction_list[i] == "sell":
                    while local_volume[i] > 0:
                        # 若有未平仓的多单，先平仓
                        if long_position:
                            tmp = long_position[0].copy()
                            tmp["close_price"] = self.price_list[i]
                            tmp["close_time"] = self.timestamp_list[i]
                            # 若需卖的超过第一个平仓仓位，计算被平仓的pair的各项数值，关闭仓位
                            if local_volume[i] >= long_position[0]["volume"]:
                                tmp["transaction_cost"] += local_transaction[i] * long_position[0]["volume"] / \
                                                           local_volume[i]
                                tmp["status"] = "closed"
                                self.append_pair_record(tmp)
                                local_transaction[i] -= local_transaction[i] * long_position[0]["volume"] / \
                                                        local_volume[i]
                                local_volume[i] -= long_position[0]["volume"]
                                long_position.pop(0)
                            # 若需卖的未超过第一个平仓仓位，计算被平仓的pair的各项数值，保留剩余的仓位
                            else:
                                tmp["transaction_cost"] = local_transaction[i] + long_position[0]["transaction_cost"] * \
                                                          local_volume[i] / long_position[0]["volume"]
                                tmp["volume"] = local_volume[i]
                                tmp["status"] = "closed"
                                long_position[0]["transaction_cost"] -= long_position[0]["transaction_cost"] * \
                                                                        local_volume[i] / long_position[0][
                                                                            "volume"]
                                # print(long_position[0]["transaction_cost"])
                                self.append_pair_record(tmp)
                                long_position[0]["volume"] -= local_volume[i]
                                local_volume[i] = 0
                        # 若没有未平仓的多单，建立空仓
                        else:
                            new_order = ini_order_pair(symbol, direction=self.direction_list[i],
                                                       open_time=self.timestamp_list[i],
                                                       open_price=self.price_list[i], volume=local_volume[i],
                                                       transaction_cost=local_transaction[i])
                            local_volume[i] = 0
                            short_position.append(new_order)
                elif self.direction_list[i] == "buy":
                    while local_volume[i] > 0:
                        # 若有未平仓的空单，先平仓
                        if short_position:
                            tmp = short_position[0].copy()
                            tmp["close_price"] = self.price_list[i]
                            tmp["close_time"] = self.timestamp_list[i]
                            if local_volume[i] >= short_position[0]["volume"]:
                                tmp["transaction_cost"] += local_transaction[i] * short_position[0]["volume"] / \
                                                           local_volume[i]
                                tmp["status"] = "closed"
                                self.append_pair_record(tmp)
                                local_transaction[i] -= local_transaction[i] * short_position[0]["volume"] / \
                                                        local_volume[i]
                                local_volume[i] -= short_position[0]["volume"]
                                short_position.pop(0)
                            else:
                                tmp["transaction_cost"] = local_transaction[i] + short_position[0]["transaction_cost"] * \
                                                          local_volume[i] / short_position[0]["volume"]
                                tmp["volume"] = local_volume[i]
                                tmp["status"] = "closed"
                                self.append_pair_record(tmp)
                                short_position[0]["transaction_cost"] -= short_position[0]["transaction_cost"] * \
                                                                         local_volume[i] / \
                                                                         short_position[0][
                                                                             "volume"]
                                short_position[0]["volume"] -= local_volume[i]
                                local_volume[i] = 0
                        # 若无未平仓的空单，建立多仓
                        else:
                            new_order = ini_order_pair(symbol, direction=self.direction_list[i],
                                                       open_time=self.timestamp_list[i],
                                                       open_price=self.price_list[i], volume=local_volume[i],
                                                       transaction_cost=local_transaction[i])
                            local_volume[i] = 0
                            long_position.append(new_order)

                else:
                    print("There are some mistakes in order's direction. Please check it!")
            long_final += long_position
            short_final += short_position
        for i in long_final + short_final:
            self.append_pair_record(i)
        self.trade_pair.pair_num = len(self.trade_pair.open_time_list)

    def append_pair_record(self, pair_message):
        self.trade_pair.trade_symbol_list = np.append(self.trade_pair.trade_symbol_list, pair_message["symbol"])
        self.trade_pair.open_time_list = np.append(self.trade_pair.open_time_list, pair_message["open_time"])
        self.trade_pair.open_price_list = np.append(self.trade_pair.open_price_list, pair_message["open_price"])
        self.trade_pair.close_time_list = np.append(self.trade_pair.close_time_list, pair_message["close_time"])
        self.trade_pair.close_price_list = np.append(self.trade_pair.close_price_list, pair_message["close_price"])
        self.trade_pair.volume_list = np.append(self.trade_pair.volume_list, pair_message["volume"])
        self.trade_pair.direction_list = np.append(self.trade_pair.direction_list, pair_message["direction"])
        self.trade_pair.transaction_cost_list = np.append(self.trade_pair.transaction_cost_list,
                                                          pair_message["transaction_cost"])
        self.trade_pair.status = np.append(self.trade_pair.status, pair_message["status"])

    def _to_array(self):
        self.timestamp_list = np.array(self.timestamp_list)
        self.trade_symbol_list = np.array(self.trade_symbol_list)
        self.price_list = np.array(self.price_list)
        self.volume_list = np.array(self.volume_list)
        self.transaction_cost = np.array(self.transaction_cost)

    def _flatten(self):
        self.trade_symbol_flatten = np.array(list(self.trade_symbol_list) * self.trade_num)
        self.timestamp_flatten = np.array(list(self.timestamp_list) * self.trade_num)
        self.price_flatten = self.price_list.flatten()
        self.volume_flatten = self.volume_list.flatten()
        self.transaction_cost_flatten = self.transaction_cost.flatten()

        ix = np.where(self.volume_list != 0)[0]
        self.trade_symbol_list = self.trade_symbol_flatten[ix]
        self.timestamp_list = self.timestamp_flatten[ix]
        if len(self.price_flatten) > 0:  # 现在没有price数据 之后会去掉这个if
            self.price_list = self.price_flatten[ix]
        self.volume_list = self.volume_flatten[ix]
        self.transaction_cost = self.transaction_cost_flatten[ix]


def ini_order_pair(symbol, direction=None, open_time=None, open_price=None, close_time=None, close_price=None,
                   volume=None, transaction_cost=None):
    return {"symbol": symbol,
            "direction": direction,
            "open_time": open_time,
            "open_price": open_price,
            "close_time": close_time,
            "close_price": close_price,
            "volume": volume,
            "transaction_cost": transaction_cost,
            "status": "opening"}


class TradePair:
    def __init__(self, universe=[], strategy_param=[], trade_symbol_list=[], open_time_list=[],
                 open_price_list=[], close_time_list=[], close_price_list=[], volume_list=[],
                 direction_list=[], transaction_cost_list=[], status=[]):
        self.universe = universe
        self.strategy_param = strategy_param
        self.trade_symbol_list = np.array(trade_symbol_list)
        self.open_time_list = np.array(open_time_list)
        self.open_price_list = np.array(open_price_list)
        self.close_time_list = np.array(close_time_list)
        self.close_price_list = np.array(close_price_list)
        self.volume_list = np.array(volume_list)
        self.direction_list = np.array(direction_list)
        self.transaction_cost_list = np.array(transaction_cost_list)
        self.status = np.array(status)
        self.pair_num = len(open_time_list)

    def toBets(self):
        self.bets = Bets(universe=self.universe, strategy_param=self.strategy_param)
        symbol_menu = np.unique(self.trade_symbol_list)
        order_num = np.arange(0, self.trade_symbol_list.size)
        for symbol in symbol_menu:
            for status_indicators in ["closed", "opening"]:
                one_symbol_order_list = order_num[
                    (self.trade_symbol_list == symbol) & (self.status == status_indicators)]
                num = 0
                this_bet = None
                while num < one_symbol_order_list.size:
                    i = one_symbol_order_list[num]
                    signal = 1 if self.direction_list[i] == "buy" else -1
                    if this_bet:
                        if (self.direction_list[i] == this_bet["direction"]):
                            this_bet["close_time"] = max(self.close_time_list[i], this_bet["close_time"])
                            this_bet["transaction_cost"] += self.transaction_cost_list[i]
                            this_bet["profit"] += (signal * (self.close_price_list[i] - self.open_price_list[i]) *
                                                   self.volume_list[
                                                       i] - self.transaction_cost_list[i]) if self.close_price_list[
                                i] else np.nan
                        else:
                            self.bets.add_bets(symbol=this_bet["symbol"], direction=this_bet["direction"],
                                               open_time=this_bet["open_time"],
                                               close_time=this_bet["close_time"],
                                               transaction_cost=this_bet["transaction_cost"], status=self.status[i],
                                               profit=this_bet["profit"])
                            profit = (signal * (self.close_price_list[i] - self.open_price_list[i]) * self.volume_list[
                                i] - self.transaction_cost_list[i]) if self.close_price_list[i] else np.nan
                            this_bet = ini_bets(symbol, direction=self.direction_list[i],
                                                open_time=self.open_time_list[i],
                                                close_time=self.close_time_list[i],
                                                transaction_cost=self.transaction_cost_list[i],
                                                profit=profit)
                    else:
                        profit = (signal * (self.close_price_list[i] - self.open_price_list[i]) * self.volume_list[
                            i] - self.transaction_cost_list[i]) if self.close_price_list[i] else np.nan
                        this_bet = ini_bets(symbol, direction=self.direction_list[i], open_time=self.open_time_list[i],
                                            close_time=self.close_time_list[i],
                                            transaction_cost=self.transaction_cost_list[i],
                                            profit=profit)
                    num += 1
                self.bets.add_bets(symbol=this_bet["symbol"], direction=this_bet["direction"],
                                   open_time=this_bet["open_time"],
                                   close_time=this_bet["close_time"],
                                   transaction_cost=this_bet["transaction_cost"], status=self.status[i],
                                   profit=this_bet["profit"])


def ini_bets(symbol, direction=None, open_time=None, close_time=None, transaction_cost=None, profit=None):
    return {"symbol": symbol,
            "direction": direction,
            "open_time": open_time,
            "close_time": close_time,
            "transaction_cost": transaction_cost,
            "status": "opening",
            "profit": profit}


class Bets:
    def __init__(self, universe=[], strategy_param=[], trade_symbol_list=[], direction_list=[], open_time_list=[],
                 close_time_list=[], transaction_cost_list=[], status_list=[], profit_list=[]):
        self.universe = universe
        self.strategy_param = strategy_param
        self.trade_symbol_list = trade_symbol_list
        self.direction_list = direction_list
        self.open_time_list = open_time_list
        self.close_time_list = close_time_list
        self.transaction_cost_list = transaction_cost_list
        self.status_list = status_list
        self.profit_list = profit_list

    def add_bets(self, symbol, direction, open_time, close_time, transaction_cost, status, profit):
        self.trade_symbol_list = np.append(self.trade_symbol_list, symbol)
        self.direction_list = np.append(self.direction_list, direction)
        self.open_time_list = np.append(self.open_time_list, open_time)
        self.close_time_list = np.append(self.close_time_list, close_time)
        self.transaction_cost_list = np.append(self.transaction_cost_list, transaction_cost)
        self.status_list = np.append(self.status_list, status)
        self.profit_list = np.append(self.profit_list, profit)


if __name__ == "__main__":
    import pandas as pd

    df = pd.read_csv("result/order.csv")
    # print(df)
    a = TradeRecord(timestamp_list=np.array(df["time"]),
                    trade_symbol_list=np.array(df["name"]), price_list=np.array(df["price"]),
                    volume_list=np.array(df["amount"]),
                    transaction_cost=np.array(df["transaction_cost"]))
    a.toPair()
    tmp_df1 = pd.DataFrame([a.trade_pair.trade_symbol_list, a.trade_pair.open_time_list, a.trade_pair.open_price_list,
                            a.trade_pair.close_time_list, a.trade_pair.close_price_list, a.trade_pair.volume_list,
                            a.trade_pair.direction_list, a.trade_pair.transaction_cost_list, a.trade_pair.status])
    print(tmp_df1.T)

    a.trade_pair.toBets()
    print("\nBets")
    tmp_df2 = pd.DataFrame([a.trade_pair.bets.trade_symbol_list,
                            a.trade_pair.bets.direction_list, a.trade_pair.bets.open_time_list,
                            a.trade_pair.bets.close_time_list,
                            a.trade_pair.bets.transaction_cost_list, a.trade_pair.bets.status_list,
                            a.trade_pair.bets.profit_list])
    print(tmp_df2.T)
