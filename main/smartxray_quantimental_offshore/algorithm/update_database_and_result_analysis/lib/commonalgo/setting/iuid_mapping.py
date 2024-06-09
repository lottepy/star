import re
from .constants import ASSET_TYPE_MAP,REGION_CURRENCY_MAP,INDEX_IUID_MAP,ASSET_PATTERN_MAP

# REGION = [US,CN,HK,WW,SG,XX,GB,CC]
def get_iuid_from_symbol(symbol):
    aqm_iuid = ''
    asset_type = ''
    sym_region = ''

    if symbol[-2]=='-':
        return 'US_10_' + symbol.split('-')[0]
    elif symbol == 'UX1' or symbol == 'UX2':
        return "US_10_"+symbol

    elif symbol in INDEX_IUID_MAP:
        return INDEX_IUID_MAP[symbol]

    elif re.match(ASSET_PATTERN_MAP['INDEX'], symbol):
        sym_type = asset_type = 'INDEX'
        sym_region = 'CN'

    elif re.match(ASSET_PATTERN_MAP['CN_XRAY_MF'], symbol):
        sym_type = 'MUTUAL FUND'
        sym_region = 'CN'
        asset_type = 'CN_XRAY_MF'

    elif re.match(ASSET_PATTERN_MAP['CN_STOCK'], symbol):
        sym_type = 'EQUITY'
        sym_region = 'CN'
        asset_type = 'CN_STOCK'

    elif re.match(ASSET_PATTERN_MAP['HK_STOCK'], symbol):
        sym_type = 'EQUITY'
        sym_region = 'HK'
        asset_type = 'HK_STOCK'

    elif re.match(ASSET_PATTERN_MAP['HK_ETF'], symbol):
        sym_type = 'EQUITY'
        sym_region = 'HK'
        asset_type = 'HK_ETF'

    elif re.match(ASSET_PATTERN_MAP['HK_FUTURE'], symbol):
        sym_type = 'FUTURE'
        sym_region = 'HK'
        asset_type = 'HK_FUTURE'

    elif re.match(ASSET_PATTERN_MAP['GB_STOCK'], symbol):
        sym_type = 'EQUITY'
        sym_region = 'GB'
        asset_type = 'GB_STOCK'

    elif re.match(ASSET_PATTERN_MAP['US_STOCK'], symbol):
        sym_type = 'EQUITY'
        sym_region = 'US'
        asset_type = 'US_STOCK'

    else:
        sym_type = 'EQUITY'
        sym_region = 'US'
        sym_market = symbol

    if asset_type:
        if asset_type == 'HK_ETF':
            sym_market = int([x for x in re.match(ASSET_PATTERN_MAP[asset_type], symbol).groups() if x][0])
        else:
            sym_market = [x for x in re.match(ASSET_PATTERN_MAP[asset_type], symbol).groups() if x][0]

    if sym_market and sym_region and sym_type:
        return '_'.join([sym_region, ASSET_TYPE_MAP[sym_type],str(sym_market)])