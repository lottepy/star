


def parse_tradable_info(symbols, rslt, EXCHANGE_CALENDAR_MAPPING, EXCHANGE_TRADE_TIME_MAPPING) -> None:
    null_ex_sym = []
    null_ex_time_sym = []
    for sym in symbols:
        if sym not in rslt:
            rslt[sym] = {"symbol":sym}
        one = rslt[sym]
        ####### exchange info ######
        if ('symboltype' in one and one['symboltype'] == 'FX') or 'list_exchange' not in one or not one['list_exchange']:
            one['list_exchange'] = 'ALL_TRADABLE'
            time_info = EXCHANGE_TRADE_TIME_MAPPING['ALL_TRADABLE']
            one["trade_time"] = time_info.split(',')[:-1]
            one["timezone"] = time_info.split(',')[-1]
            null_ex_sym.append(one["symbol"])

        if EXCHANGE_CALENDAR_MAPPING.get(one['list_exchange'], one['list_exchange']) not in EXCHANGE_TRADE_TIME_MAPPING:
            one["real_list_exchange"] = one["list_exchange"]
            one["list_exchange"] = 'ALL_TRADABLE'
            time_info = EXCHANGE_TRADE_TIME_MAPPING['ALL_TRADABLE']
            one["trade_time"] = time_info.split(',')[:-1]
            one["timezone"] = time_info.split(',')[-1]
            null_ex_time_sym.append(one["symbol"])
        else:
            time_info = EXCHANGE_TRADE_TIME_MAPPING[EXCHANGE_CALENDAR_MAPPING.get(one['list_exchange'], one['list_exchange'])]
            one["trade_time"] = time_info.split(',')[:-1]
            one["timezone"] = time_info.split(',')[-1]
    return null_ex_sym, null_ex_time_sym