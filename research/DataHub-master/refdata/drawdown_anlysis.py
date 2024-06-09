import pandas as pd
import numpy as np
import csv
import os

# shcomp = pd.read_csv('global_indices.csv', index_col=0)['SHCOMP INDEX']
# shcomp = shcomp[shcomp.first_valid_index():]
# shcomp_nd = shcomp.as_matrix()
# max_value = np.maximum.accumulate(shcomp_nd)[-1]
# max_key = shcomp[shcomp == max_value].index[0]
# max_loc = shcomp.index.get_loc(max_key)
# print shcomp

# i = (np.maximum.accumulate(data) - data).astype('float').idxmax()  # end of the period
#         j = (data[:i]).astype('float').idxmax()
#         value = (data[j] - data[i]) / data[j]
#         maxdd_series = maxdd_series.append(pd.Series(value,index = [k]))

data = pd.read_csv('global_indices.csv', index_col=0)['HSI INDEX']
rawdata = data = data[data.first_valid_index():]

class CSVWrite(object):
	def __init__(self,filename = None):
		if filename:
			self.filename = filename
		else:
			self.filename = 'bull_bear.csv'
		self.headers = [
			'sinal',
			'date',
			'value',
			'percentage',
		]
		self.writer = None
		self.init_writer()

	def init_writer(self):
		if not os.path.isfile(self.filename):
			with open(self.filename, 'a') as f:
				writer = csv.writer(f)
				writer.writerow(self.headers)

	def write_data(self, data):
		with open(self.filename, 'a') as f:
			writer = csv.writer(f)
			writer.writerows(data)

file_name = 'HSI_20.csv'
csvwriter = CSVWrite(file_name)
file_data =[]
threoshold = 0.2

while True:
	max_value = np.maximum.accumulate(data)
	max_series = (max_value - data)/max_value
	if len(max_series[max_series>threoshold])>0:
		bear_signal = max_series[max_series>threoshold].index[0]
	else:
		bear_signal = '2018-09-14'
	min_value = np.minimum.accumulate(data)
	min_series = (data - min_value)/min_value
	if len(min_series[min_series > threoshold]) > 0:
		bull_signal = min_series[min_series>threoshold].index[0]
	else:
		bull_signal = '2018-09-14'
	if pd.to_datetime(bear_signal) < pd.to_datetime(bull_signal):
		print('bear signal: {}, value: {}'.format(bear_signal, data[bear_signal]))
		row = [
			'{}'.format('bear signal'),
			"{}".format(bear_signal),
			"{0:.3f}".format(data[bear_signal]),
			"{0:.3f}".format(threoshold),
		]
		file_data.append(row)

		bear_bottom = max_series[:bull_signal].idxmax()
		bear_value = max_series[bear_bottom]
		print('bear bottom: {}, value:{}, down to {:.2%}'.format(bear_bottom, data[bear_bottom], bear_value))
		data = data[bear_bottom:]
		row = [
			'{}'.format('bear bottom'),
			"{}".format(bear_bottom),
			"{0:.3f}".format(data[bear_bottom]),
			"{0:.3f}".format(bear_value),
		]
		file_data.append(row)

	elif pd.to_datetime(bear_signal) > pd.to_datetime(bull_signal):
		print('bull signal: {}, value: {}'.format(bull_signal, data[bull_signal]))
		row = [
			'{}'.format('bull signal'),
			"{}".format(bull_signal),
			"{0:.3f}".format(data[bull_signal]),
			"{0:.3f}".format(threoshold),
		]

		file_data.append(row)
		bull_peak = min_series[:bear_signal].idxmax()
		bull_value = min_series[bull_peak]
		print('bull peak: {}, value: {}, up to {:.2%}'.format(bull_peak, data[bull_peak], bull_value))
		data = data[bull_peak:]
		row = [
			'{}'.format('bull peak'),
			"{}".format(bull_peak),
			"{0:.3f}".format(data[bull_peak]),
			"{0:.3f}".format(bull_value),
		]
		file_data.append(row)
	else:
		csvwriter.write_data(file_data)
		break

	# peak_date = (data[:valley_date]).astype('float').idxmax()
	# dd_value = (data[peak_date] - data[valley_date])/data[peak_date]
	# if dd_value > 0.05:
	# 	print 'from {} to {}: drawon down  is {}'.format(peak_date,valley_date,dd_value)
	# 	if pd.to_datetime(peak_date) < pd.to_datetime(last_valley):
	# 		data = rawdata[valley_date:]
	# 	elif pd.to_datetime(peak_date) > pd.to_datetime(last_valley):
	# 		data = rawdata[last_valley:peak_date]
	# 	last_valley = valley_date
	# 	last_peak = peak_date
	# else:
	# 	break