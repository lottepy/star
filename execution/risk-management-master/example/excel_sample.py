# -*- coding: utf-8 -*-
import pandas as pd
import pandas_datareader.data as web
import datetime


if __name__ == '__main__':
	start = datetime.datetime(2010, 1, 1)
	end = datetime.datetime(2013, 1, 27)
	cpi = web.DataReader('CPIAUCSL', 'fred', start, end)

	# 创建excel writer对象，将df传入excel
	writer = pd.ExcelWriter('excel_sample.xlsx', engine='xlsxwriter')
	cpi.to_excel(writer, sheet_name='us_cpi')

	cpi_stats = cpi.describe()
	cpi_stats.to_excel(writer, sheet_name='cpi_stats')

	# 画图直观展示
	workbook = writer.book
	cpi_sheet = writer.sheets['us_cpi']
	chart = workbook.add_chart({'type': 'line'})
	max_rol = len(cpi.index.values)
	chart.add_series({
		'name': ['us_cpi', 0, 1],
		'categories': ['us_cpi', 1, 0, max_rol, 0],
		'values': ['us_cpi', 1, 1, max_rol, 1],
		'line': {'width': 1.00}
	})
	chart.set_x_axis({'name': 'Date', 'date_axis': True})
	chart.set_y_axis({'name': 'CPI', 'major_gridlines': {'visible': False}})
	chart.set_legend({'position': 'bottom'})
	cpi_sheet.insert_chart('H2', chart)

	writer.save()


