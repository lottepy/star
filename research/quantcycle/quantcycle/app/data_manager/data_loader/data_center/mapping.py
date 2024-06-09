import json
import os
import pathlib

from ...utils.update_nested_dict import update_dict
from .utils.json_utils import read_json, write_json

_DIR_PATH = os.path.join(pathlib.Path(__file__).parent.absolute(), 'data')

# symbol mapping
# ==============================================
# FX related symbol mapping
_FX2DM_SYMBOL = read_json(os.path.join(_DIR_PATH, 'fx2dm_symbol.json'))

# Final large symbol maps
MNEMONIC2SYMBOL = {}
update_dict(MNEMONIC2SYMBOL, _FX2DM_SYMBOL)

# ==============================================


# intrument type mapping
# ==============================================
INSTRUMENT_TYPE_MAPPING = read_json(
    os.path.join(_DIR_PATH, 'instrument_type_map.json'))
# ==============================================

# trading currency mapping
# ==============================================
TRADING_CURRENCY_MAPPING = read_json(
    os.path.join(_DIR_PATH, 'trading_currency_map.json'))
# ==============================================

# stock currency mapping
# ==============================================
STOCK_CCY_MAPPING = read_json(
    os.path.join(_DIR_PATH, 'stock_ccy_map.json'))
# ==============================================

# ETF currency mapping
# ==============================================
ETF_CCY_MAPPING = read_json(
    os.path.join(_DIR_PATH, 'etf_ccy_map.json'))
# ==============================================

# ccy mapping to ccy/usd or usd/ccy
# ==============================================
CCY2USDCCP = read_json(os.path.join(_DIR_PATH, 'ccy2fxrate.json'))
# ==============================================

# ccp tomnex forward scale mapping
# ==============================================
FWD_SCALE = read_json(os.path.join(_DIR_PATH, 'fwd_scale.json'))
# ==============================================

# NDF list
IS_NDF = read_json(os.path.join(_DIR_PATH, 'ndf.json'))

# exchange calendar mapping
# ==============================================
EXCHANGE_CALENDAR_MAPPING = read_json(
    os.path.join(_DIR_PATH, 'exchange_calendar_map.json'))
# ==============================================

# exchange trade time mapping
# ==============================================
EXCHANGE_TRADE_TIME_MAPPING = read_json(
    os.path.join(_DIR_PATH, 'exchange_trade_time_map.json'))
# ==============================================
