from lib.commonalgo.execution.trade.trading_controller import TradingController
from lib.commonalgo.execution.trade.trading import trading


# 同步持仓 没有返回值 出错会抛出exception
trading_controller = TradingController(endpoint='http://172.31.86.12:5565/api/v3/')
trading_controller.sync_holdings(brokerid='1', baccount='U3245801')


# get positions
# 先尝试从broker获取 失败后会提示 并改从数据库获取
# region会影响过滤的IUID 例如region=HK 输出只包含HK的持仓
# 返回dataframe
positions = trading.get_current_positions(
    trading_controller, paper={'broker': '1', 'account': 'U3245801', 'subaccount': None}, region='US'
)
print(positions.to_string())


# 查asset
# 返回float 货币为target_currency
asset_usd = trading.get_current_cash_info(
    trading_controller, paper={'broker': '1', 'account': 'U3245801', 'subaccount': None}, target_currency='USD'
)
asset_hkd = trading.get_current_cash_info(
    trading_controller, paper={'broker': '1', 'account': 'U3245801', 'subaccount': None}, target_currency='HKD'
)
print(f'usd:{asset_usd}, hkd:{asset_hkd}')
