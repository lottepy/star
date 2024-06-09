#!/usr/bin/env python

CASH_IUID = 'CN_00_CNY'

region_currency_map = {
    "HK": "HKD",
    "US": "USD",
    "CN": "CNY",
    "HC": "CNH"
}

order_status_code = ['CREATE', 'PENNDING', 'FILLING', 'CANCELLED', 'REJECTED',
                     'FILLED', ]

symbol_prefix_mapping = {'CN': 'choice', 'HK': 'factset', 'US': ''}  # 不同区域用到的symbol类型 HK/US为普通symbol

region_data_src_map = {'CN': 'choice', 'HK': 'choice', 'US': 'factset'}

region = 'HK'

true_str = {'True', 'T', 'true', '1', 't', 'y', 'yes', 'Y', 'Yes', 'YES',
            'yeah', 'yup', 'certainly', 'uh-huh'}


class OrderStatus:
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"
    CREATE = "CREATE"
    PENNDING = "PENNDING"   # I know, this is a typ0
    FILLING = "FILLING"
    SUBMITTING = "SUBMITTING"  # we add this status to indecate the status before any trading message returned
    FAILED = "FAILED"  # we add this status to indecate the order is failed during submission

OrderCompletionStatus = {"FILLED", "CANCELLED", "REJECTED"}

class TradingDirection:
    BUY = "BUY"
    SELL = "SELL"

class MessageType:
    ORDERBOOK = "orderbook"
    TICK = "tick"
    METADATA = "metadata"

class UpdateType:
    REFRESH = "Refresh"
    UPDATE = "Update"
