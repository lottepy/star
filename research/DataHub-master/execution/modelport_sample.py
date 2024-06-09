# coding=utf-8
"""
A sample of IB model portfolio execution (HK/US)
"""
from lib.commonalgo.execution.trading_executor import TradingExecutor
from lib.commonalgo.execution.trading_controller import TradingController

__author__="Joseph Chen"
__email__="joseph.chen@magnumwm.com"


if __name__=="__main__":

	# Live account only, be careful
    account_dict = {
        'broker': '1',
        'account': 'U9556979',
        'model': 'Internalalgotest1',
    }

    target_weight_csv = 'data/target_weight_HK.csv'
    region = 'HK'


    trading_ctrl = TradingController()

    cash_info = trading_ctrl.get_cash_info(brokerid=account_dict.get('broker'), baccount=account_dict.get('account'),
                                           subaccount=account_dict.get('model'))
    holdings_info = trading_ctrl.get_holdings(brokerid=account_dict.get('broker'), baccount=account_dict.get('account'),
                                              subaccount=account_dict.get('model'))



    # order_dict = dict(
	 #    type='LIMIT',  # Market
	 #    side='BUY',
	 #    symbol='GB_10_ERNA',
	 #    quantity=1,
	 #    price=5.0
    # )

    order_dict = dict(
	    type='LIMIT',  # Market
	    side='BUY',
	    symbol='US_10_ASHR',
	    quantity=1,
	    price=5.0
    )
    # orderid = trading_ctrl.submit_order(brokerid=account_dict.get('broker'),
    #                                     baccount=account_dict.get('account'),
    #                                     modelcode=account_dict.get('model'),
    #                                     order=order_dict)

    print('test')

