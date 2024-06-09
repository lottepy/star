import os
MR_PARAMS = ['alpha', 'rho', 'W', 'N']
MOM_PARAMS = ['rank', 'sample']

STRATEGY_NAME_MAP = {
	'mean_reversion': MR_PARAMS,
	'momentum': MOM_PARAMS,
	'var': ['weight']
}

ALL_CCY = ['NZDJPY', 'NOKSEK', 'GBPJPY', 'GBPCHF', 'GBPCAD',
		   'EURJPY', 'EURGBP', 'EURCHF', 'EURCAD', 'CADJPY',
		   'CADCHF', 'AUDNZD', 'AUDJPY', 'AUDCAD', 'USDSEK',
		   'USDNOK', 'USDJPY', 'USDCHF', 'USDCAD', 'NZDUSD',
		   'GBPUSD', 'EURUSD', 'AUDUSD', 'USDSGD', 'USDTHB',
		   'USDKRW', 'USDTWD', 'USDINR', 'USDIDR']
HIGH_VOL_CCY = ['NZDJPY', 'GBPJPY', 'GBPCHF', 'EURJPY', 'CADJPY',
				'CADCHF', 'AUDJPY', 'USDNOK', 'USDCHF', 'NZDUSD',
				'AUDUSD', 'USDKRW', 'USDTWD', 'USDINR', 'USDIDR']

ALL_CCY_NO_CROSS = ['USDSEK', 'USDNOK', 'USDJPY', 'USDCHF', 'USDCAD',
					'NZDUSD', 'GBPUSD', 'EURUSD', 'AUDUSD', 'USDSGD',
					'USDTHB', 'USDKRW', 'USDTWD', 'USDINR', 'USDIDR']
HIGH_VOL_CCY_NO_CROSS = ['USDNOK', 'USDCHF', 'NZDUSD', 'AUDUSD', 'USDKRW',
						 'USDTWD', 'USDINR', 'USDIDR']

# PARSE_PATH = '//192.168.9.170/share/alioss/0_DymonFx/parse_data'

ENV = os.getenv('DYMON_FX_ENV', 'dev_algo')
PARSE_PATH = {
	'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/parse_data/",
	'dev_jeff': '../0_DymonFx/parse_data/',
	'live': '../0_DymonFx/parse_data/'
}[ENV]
WORKING_PATH = 'D:/Magnum/Research/FX'

