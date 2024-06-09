import pandas as pd
import numpy as np


if __name__ == '__main__':
	bear_prob = pd.read_csv('bear_prob_sim.csv', index_col=0)

	sector_select = pd.read_csv('sector_select.csv', index_col=0)
	sector_select['Score'] = np.maximum(
		0.2,
		50. * sector_select['Mean Reversion Intensity']
	)

	region_select = pd.read_csv('region_select.csv', index_col=0)
	region_select['Score'] = np.maximum(
		0.2,
		50. * region_select['Mean Reversion Intensity']
	)

	acas_list = pd.read_csv(
		'acas_list.csv',
		encoding='utf_8_sig',
		index_col=0
	)[['Name', 'Flag']]

	acas_factor = pd.read_csv(
		'acas_factor.csv',
	)['Factor'].to_frame()

	ahas_list = pd.read_csv(
		'ahas_list.csv',
		encoding='utf_8_sig',
		index_col=0
	)[['Name', 'Flag']]

	ahas_factor = pd.read_csv(
		'ahas_factor.csv',
	)['Factor'].to_frame()

	# Export to Excel
	writer = pd.ExcelWriter('report_data.xlsx', engine='xlsxwriter')
	bear_prob.to_excel(writer, sheet_name='algo_view')
	sector_select.iloc[:3].to_excel(writer, sheet_name='algo_view', startrow=5)
	region_select.iloc[:3].to_excel(writer, sheet_name='algo_view', startrow=5, startcol=5)
	acas_list.iloc[:].to_excel(writer, sheet_name='algo_view', startrow=10)
	acas_factor.iloc[:3].to_excel(writer, sheet_name='algo_view', startrow=10, startcol=5, index=False)
	ahas_list.iloc[:].to_excel(writer, sheet_name='algo_view', startrow=35)
	ahas_factor.iloc[:3].to_excel(writer, sheet_name='algo_view', startrow=35, startcol=5, index=False)

	writer.save()
