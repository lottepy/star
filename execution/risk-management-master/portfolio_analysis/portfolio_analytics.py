# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np


class PortfolioAnalytics:
	def __init__(self, _portfolio, _benchmark):
		self.portfolio = _portfolio
		self.benchmark = _benchmark
		self.pv = pd.DataFrame(None)
		self.simple_report = pd.DataFrame(None)
		self.weight_table = pd.DataFrame(None)
		self.return_contribution_table = pd.DataFrame(None)
		self.segment_return_table = pd.DataFrame(None)
		self.attribution_model_table = pd.DataFrame(None)
		self.rebalance_date = self._get_rebalance_dt()
		self._simple_analysis()
		self.segment = self._get_segment()

	def link_underlying(self, path):
		self.asset_map = pd.read_csv(path, index_col=0)

	def return_attribution(self, method='Parilux'):
		# 获得组合和基准中各个资产的权重，按调仓日划分
		# 权重需要下移一格，以保证每个时间段对应的权重都是该时间段组合/基准期初的权重
		port_segment_weight = self._get_portfolio_segment_weight()
		port_segment_weight[~port_segment_weight.index.isin(self.rebalance_date)] = np.nan
		port_segment_weight = port_segment_weight.ffill().shift(1).fillna(0.)
		bmrk_segment_weight = self.benchmark.weight.copy().shift(1).fillna(0.)
		bmrk_segment_weight.columns = [c + '-B' for c in bmrk_segment_weight.columns]

		# 计算Active weight
		self.active_weight = pd.np.subtract(port_segment_weight, bmrk_segment_weight)
		self.active_weight.columns = [c + '-Active' for c in self.active_weight.columns]
		self.weight_table = pd.concat([
			port_segment_weight,
			bmrk_segment_weight,
			self.active_weight
		], axis=1, join='outer')

		# 分解组合收益
		port_modify_price = self.portfolio.price.copy()
		port_modify_price[~port_modify_price.index.isin(self.rebalance_date)] = np.nan
		port_modify_price = port_modify_price.ffill()

		port_asset_return = self.portfolio.weight.shift(1).fillna(0.) \
							* port_modify_price.pct_change().fillna(0.)
		port_segment_contribution = port_asset_return.dot(self.asset_map)
		port_segment_contribution['Total Return'] = port_segment_contribution.sum(axis=1)

		# 分解基准收益
		bmrk_modify_price = self.benchmark.price.copy()
		bmrk_modify_price[~bmrk_modify_price.index.isin(self.rebalance_date)] = np.nan
		bmrk_modify_price = bmrk_modify_price.ffill()

		bmrk_asset_return = self.benchmark.weight.shift(1).fillna(0.) \
							* bmrk_modify_price.pct_change().fillna(0.)
		bmrk_segment_contribution = bmrk_asset_return
		bmrk_segment_contribution['Total Return'] = bmrk_segment_contribution.sum(axis=1)

		# 计算超额收益
		active_segment_contribution = port_segment_contribution - bmrk_segment_contribution
		bmrk_segment_contribution.columns = [c + '-B' for c in bmrk_segment_contribution.columns]
		active_segment_contribution.columns = [c + '-Excess' for c in active_segment_contribution.columns]

		self.return_contribution_table = pd.concat([
			port_segment_contribution,
			bmrk_segment_contribution,
			active_segment_contribution
		], axis=1, join='outer')

		# 计算attribution analysis必要的w_p, r_p, w_b, r_b
		effective_columns = np.append(self.segment, self.segment + '-B')
		self.segment_return_table = (self.return_contribution_table[effective_columns] \
									/ self.weight_table[effective_columns]).fillna(0.)
		port_segment_return = self.segment_return_table[self.segment]
		bmrk_segment_return = self.segment_return_table[self.segment + '-B']
		active_segment_return = port_segment_return - bmrk_segment_return


		# 实现Parilux和Brinson两种分解方式
		# Parilux: Allocation Effect, Selection Effect
		# Brinson: Allocation Effect, Selection Effect, Interaction
		if method == 'Parilux':
			self.allocation_effect = pd.np.multiply(
				bmrk_segment_return,
				pd.np.subtract(port_segment_weight, bmrk_segment_weight)
			)
			self.selection_effect = pd.np.multiply(
				port_segment_weight,
				pd.np.subtract(port_segment_return, bmrk_segment_return)
			)

			self.attribution_model_table['Allocation Effect'] = self.allocation_effect.sum(axis=1)
			self.attribution_model_table['Selection Effect'] = self.selection_effect.sum(axis=1)

		elif method == 'Brinson':
			self.allocation_effect = pd.np.multiply(
				bmrk_segment_return,
				pd.np.subtract(port_segment_weight, bmrk_segment_weight)
			)
			self.selection_effect = pd.np.multiply(
				bmrk_segment_weight,
				pd.np.subtract(port_segment_return, bmrk_segment_return)
			)
			self.interaction = pd.np.multiply(
				pd.np.subtract(port_segment_weight, bmrk_segment_weight),
				pd.np.subtract(port_segment_return, bmrk_segment_return)
			)

			self.attribution_model_table['Allocation Effect'] = self.allocation_effect.sum(axis=1)
			self.attribution_model_table['Selection Effect'] = self.selection_effect.sum(axis=1)
			self.attribution_model_table['Interaction'] = self.interaction.sum(axis=1)

		return


	def risk_attribution(self):
		pass

	def _get_portfolio_segment_weight(self):
		return self.portfolio.weight.dot(self.asset_map)

	def _simple_analysis(self):
		self.portfolio.simple_analysis()
		self.benchmark.simple_analysis()
		self.pv = pd.concat(
			[self.portfolio.pv, self.benchmark.pv],
			axis=1,
			join='outer'
		)

		daily_pv = self.pv.copy()
		daily_pv.index = pd.to_datetime(daily_pv.index, format='%Y-%m-%d')
		daily_pv['Year'] = daily_pv.index.year
		daily_pv['Month'] = daily_pv.index.month

		def total_return(prices):
			return prices.iloc[-1] / prices.iloc[0] - 1

		self.monthly_pv = daily_pv.groupby(['Year', 'Month'], ).apply(total_return).iloc[:, :2]


	def _get_rebalance_dt(self):
		cash_change = self.portfolio.position['Cash'].pct_change().fillna(0.)
		return np.append(
			np.append(
				cash_change.index.values[0],
				cash_change[cash_change != 0].index.values
			),
			cash_change.index.values[-1]
		)

	def _get_segment(self):
		return self.benchmark.position.columns.values

	def describe(self):
		self.simple_report = pd.concat([self.portfolio.report, self.benchmark.report])
		return self.simple_report

	def to_excel(self, file_name):
		writer = pd.ExcelWriter(file_name, engine='xlsxwriter')

		# 输出数据
		# self.simple_report.T.applymap(lambda x: ('%.2f' % (x * 100)) + '%').to_excel(writer, sheet_name='simple_analysis')
		self.simple_report.T.to_excel(writer, sheet_name='simple_analysis')
		self.pv.to_excel(writer, sheet_name='portfolio_value')
		self.monthly_pv.to_excel(writer, sheet_name='monthly_return')
		self.weight_table.loc[self.rebalance_date]\
			.to_excel(writer, sheet_name='historical_weight')
		self.return_contribution_table.loc[self.rebalance_date]\
			.to_excel(writer, sheet_name='return_breakdown')
		self.segment_return_table.loc[self.rebalance_date, sorted(self.segment_return_table.columns.values)]\
			.to_excel(writer, sheet_name='source_return')
		self.attribution_model_table.loc[self.rebalance_date]\
			.to_excel(writer, sheet_name='return_attribution_summary')

		# 画图直观展示
		workbook = writer.book

		# 组合净值曲线
		pv_sheet = writer.sheets['portfolio_value']
		chart = workbook.add_chart({'type': 'line'})
		max_rol = len(self.pv.index.values)
		for i in xrange(2):
			col = i + 1
			chart.add_series({
				'name': ['portfolio_value', 0, col],
				'categories': ['portfolio_value', 1, 0, max_rol, 0],
				'values': ['portfolio_value', 1, col, max_rol, col],
				'line': {'width': 1.00}
			})
		chart.set_x_axis({'name': 'Date', 'date_axis': True})
		chart.set_y_axis({'name': 'PV', 'major_gridlines': {'visible': False}})
		chart.set_legend({'position': 'bottom'})
		pv_sheet.insert_chart('H2', chart)

		# 收益源收益比较
		source_sheet = writer.sheets['source_return']
		max_rol = len(self.segment_return_table.loc[self.rebalance_date].index.values)
		init_plot_row = 2
		for seg in self.segment:
			idx = list(self.segment).index(seg)
			chart = workbook.add_chart({'type': 'column'})
			for i in xrange(2):
				pos = idx * 2 + 1 + i
				chart.add_series({
					'name': ['source_return', 0, pos],
					'categories': ['source_return', 1, 0, max_rol, 0],
					'values': ['source_return', 1, pos, max_rol, pos],
					'gap': 300
				})
			chart.set_x_axis({'name': 'Date', 'date_axis': True})
			chart.set_y_axis({'name': 'Return', 'major_gridlines': {'visible': False}})
			source_sheet.insert_chart('K' + str(init_plot_row), chart)
			init_plot_row += 20

		# 归因模型比较
		model_sheet = writer.sheets['return_attribution_summary']
		max_rol = len(self.attribution_model_table.loc[self.rebalance_date].index.values)
		chart = workbook.add_chart({'type': 'column'})
		for i in xrange(3):
			col = i + 1
			chart.add_series({
				'name': ['return_attribution_summary', 0, col],
				'categories': ['return_attribution_summary', 1, 0, max_rol, 0],
				'values': ['return_attribution_summary', 1, col, max_rol, col],
				'gap': 300
			})
		chart.set_x_axis({'name': 'Date', 'date_axis': True})
		chart.set_y_axis({'name': 'Return', 'major_gridlines': {'visible': False}})
		model_sheet.insert_chart('H2', chart)

		writer.save()
