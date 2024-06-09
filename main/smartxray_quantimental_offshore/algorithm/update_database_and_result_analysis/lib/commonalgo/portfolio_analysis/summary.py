import math
from tabulate import tabulate
import matplotlib.pyplot as plt


def portfolio_summary(perf_manual, T):
	annual_return = math.pow(perf_manual.algorithm_period_return.ix[-1] + 1, 1 / T) - 1
	financial_indicator = [
		['Portfolio annualized return', annual_return],
		['Portfolio cumulative return', perf_manual.algorithm_period_return.ix[-1]],
		['Sharpe ratio', perf_manual.sharpe.ix[-1]],
		['Max Draw Down', perf_manual.max_drawdown.ix[-1]],
		['Beta', perf_manual.beta.ix[-1]],
		['Alpha', perf_manual.alpha.ix[-1]],
		['Volatility', perf_manual.algo_volatility.ix[-1]]
	]
	print(tabulate(
		financial_indicator,
		headers=[
			'Indicator',
			'Value'
		],
		tablefmt='orgtbl'
	))


def analyze(context=None, results=None):
	"""
	Plot the portfolio and the benchmark return
	"""
	ax = plt.subplot(111)
	results.algorithm_period_return.plot()
	results.benchmark_period_return.plot()
	plt.grid(True)
	plt.title('Back-testing result')
	plt.ylabel('Cumulative Return')

	# Shrink current axis's height by 10% on the bottom
	box = ax.get_position()
	ax.set_position([
		box.x0,
		box.y0 + box.height * 0.1,
		box.width,
		box.height * 0.9
	])
	ax.legend(
		loc='upper center',
		bbox_to_anchor=(0.5, -0.2),
		fancybox=True,
		shadow=True,
		ncol=5
	)
	plt.show()
