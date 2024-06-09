import os

IMM_DATES = [
	{'FixingDate': '16/12/2019', 'SettlementDate': '18/12/2019'},
	{'FixingDate': '16/03/2020', 'SettlementDate': '18/03/2020'},
	{'FixingDate': '15/06/2020', 'SettlementDate': '17/06/2020'},
	{'FixingDate': '14/09/2020', 'SettlementDate': '16/09/2020'}
]

TARGETRATIO_HEADER = ['CCY1', 'CCY2', 'TargetRatio', 'Portfolio']
#ORDER_HEADER = ['CCY1', 'CCY2', 'FixingDate', 'SettlementDate', 'Amount1', 'Amount2', 'Portfolio']
ORDER_HEADER = ['CCY1', 'CCY2', 'FixingDate', 'SettlementDate', 'Amount', 'Portfolio']

IMM_FIX_DATE = '15/06/2020'
IMM_SETTLE_DATE = '17/06/2020'

NDF_CCP = ['USDINR', 'USDIDR', 'USDKRW', 'USDTWD']

CURRENT_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_PATH = os.path.join(CURRENT_PATH, 'log')
DATA_PATH = os.path.join(CURRENT_PATH, 'data')
FX_RECORD_PATH = os.path.join(DATA_PATH, 'fx_record')
BACKTEST_PATH = os.path.join(CURRENT_PATH, 'fx_daily')
BT_CONFIG_PATH = os.path.join(BACKTEST_PATH, 'data/config')

ALL_CCY_NO_CROSS = ['USDSEK', 'USDNOK', 'USDJPY', 'USDCHF', 'USDCAD',
					'NZDUSD', 'GBPUSD', 'EURUSD', 'AUDUSD', 'USDSGD',
					'USDTHB', 'USDKRW', 'USDTWD', 'USDINR', 'USDIDR']
ALL_CCY = ['AUDCAD','AUDJPY','AUDNZD','AUDUSD','CADCHF','CADJPY','EURCAD','EURCHF','EURGBP','EURJPY',
 		   'EURUSD','GBPCAD','GBPCHF','GBPJPY','GBPUSD','NOKSEK','NZDJPY','NZDUSD','USDCAD','USDCHF','USDIDR',
           'USDINR','USDJPY','USDKRW','USDNOK','USDSEK','USDSGD','USDTHB','USDTWD']

HIGH_VOL_CCY_NO_CROSS = ['USDNOK', 'USDCHF', 'NZDUSD', 'AUDUSD', 'USDKRW',
						 'USDTWD', 'USDINR', 'USDIDR']

VAR_LIMIT = 0.01
USD_LIMIT = 3
NON_USD_LIMIT = 1