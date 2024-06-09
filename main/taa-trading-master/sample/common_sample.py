# coding=utf-8
"""
A sample of algo execution (HK/US)
"""
from lib.commonalgo.execution.trading_executor import TradingExecutor

__author__="Joseph Chen"
__email__="joseph.chen@magnumwm.com"


if __name__=="__main__":

	# paper1 = {
	#     'broker': '2',
	#     'account': 'a28e4a93-dc43-479b-807f-89642e53fce0',
	# }

	# Live account only, be careful
	paper1 = {
		'broker': '1',
		'account': 'U9556979',
		'subaccount': 'Internalalgotest1',
	}

	target_weight_csv = 'data/target_weight_US.csv'
	region = 'US'

	trading_exe = TradingExecutor(paper=paper1,
								  target_weight_csv=target_weight_csv,
								  region=region,
								  activate_futu=False,        # HK/US/CN
								  activate_factset=False,     # HK/US
								  activate_choice=False,      # CN
								  activate_msq=False,         # HK/US/CN
								  activate_intrinio=True,     # US
								  channel_priority=['intrinio'],
								  disable_msq=False,
								  )

	# Cancel all existing orders
	trading_exe.cancel_all_active_orders()

	# Test single order
	order = dict(
		type='MARKET',  # Market
		side='BUY',
		symbol='US_10_ASHR',
		quantity=1,
	)
	trading_exe.place_order(order)

	# Save info before trading
	cash = trading_exe.get_current_cash()
	positions = trading_exe.get_current_positions()
	cash.to_csv("tmp/before_trading_cash.csv")
	positions.to_csv("tmp/before_trading_positions.csv")

	# Optional: liquidate your current positions
	if (not positions.empty):
		trading_exe.liquidate(current_positions=positions)

	# Rebalance
	target_positions = trading_exe.calculate_target_positions()
	target_positions.to_csv("tmp/target_positions.csv")
	orders_sell, orders_buy = trading_exe.create_order_basket(target_positions)
	# Note: If you want to terminate the execution manually, you can set breakpoint
	# in the while loop, and save 'sell_results' or 'buy_results' to local
	# drive. This will give you a record of filled orders and partial filled orders.
	sell_results = trading_exe.execute_sell(orders_sell)
	buy_results = trading_exe.execute_buy(orders_buy)

	# Save info after trading
	cash = trading_exe.get_current_cash()
	positions = trading_exe.get_current_positions()
	cash.to_csv("tmp/after_trading_cash.csv")
	positions.to_csv("tmp/after_trading_positions.csv")

	# Finished
	print("All done.")
	res = trading_exe.msq_client.disconnect_client()
	print(res)
