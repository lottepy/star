import pandas as pd
import numpy as np
from datetime import datetime
import math

CALENDAR_DAYS = 365.
BUSINESS_DAYS = 252.
# RF = 2.37563 / 100
RF = 0. / 100


class FinancialIndicator():
	def __init__(self, data):
		self.data = data
		self.report = None
		self.pv = data + 1.
		self.daily_return = pv.pct_change().dropna()

	def _compute_total_return(self):
		return data.iloc[-1, :]

	def _compute_annual_return(self):
		return np.power(report['Total Return'] + 1., CALENDAR_DAYS / period) - 1

	def _compute_volatility(self):
		return self.daily_return.std() * math.sqrt(BUSINESS_DAYS)

	def _compute_sharpe(self):
		return


path = 'C:\Users\lance\PycharmProjects\magnum-risk-management\data/pv_ts_sg.csv'

data = pd.read_csv(path, index_col=0)
report = pd.DataFrame(None, index=data.columns.values)
start = datetime.strptime(data.index.values[0], '%Y/%m/%d')
end = datetime.strptime(data.index.values[-1], '%Y/%m/%d')
period = (end - start).days

if data.iloc[1, 1] > 0.5:
	data = data - 1.
report['Total Return'] = data.iloc[-1, :]
report['Return p.a.'] = np.power(report['Total Return'] + 1., 365. / period) - 1

pv = data + 1.
daily_return = pv.pct_change().dropna()
report['Volatility'] = daily_return.std() * math.sqrt(252)
report['Sharpe Ratio'] = (report['Return p.a.'] - RF) / report['Volatility']
report['Max Drawdown'] = (pv.div(pv.cummax()) - 1.).min()
report['Calmar Ratio'] = report['Return p.a.'] / abs(report['Max Drawdown'])
report['99% VaR'] = daily_return.mean() - daily_return.std() * 2.32

print report.to_string()
report.to_csv(path.replace('.csv', '_stats.csv'))

rolling_q = daily_return.rolling(500).quantile(0.01)
rolling_q.to_csv(path.replace('.csv', '_rolling_quantile.csv'))
