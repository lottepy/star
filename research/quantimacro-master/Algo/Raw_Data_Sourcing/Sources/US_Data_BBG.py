import datetime
import os

import pandas as pd

from Constants import *


# Input a dataset from BBG macro indicator download
# Output a dataset at monthly level with a timestamp indicating the release date

def us_bbg_process(datain, variable_name, last_date):
	work_in_process = datain.loc[:, ['date', 'ECO_RELEASE_DT', 'ACTUAL_RELEASE']]
	work_in_process.dropna(inplace=True)
	work_in_process['ECO_RELEASE_DT'] = work_in_process['ECO_RELEASE_DT'].map(lambda x: int(x))
	work_in_process["tsp_" + variable_name] = work_in_process['ECO_RELEASE_DT'].map(lambda x: datetime.datetime(int(x / 10000), int((x % 10000)/100), int(x % 100)))
	work_in_process[variable_name] = work_in_process['ACTUAL_RELEASE']
	work_in_process["date_" + variable_name] = work_in_process['date']
	work_in_process = work_in_process[work_in_process["tsp_" + variable_name] <= last_date]
	dataout = work_in_process.loc[:, [variable_name, "date_" + variable_name, "tsp_" + variable_name]]
	return dataout


def us_dt_consolidate(datapath, last_date):
	us_data_files = os.listdir(datapath)
	us_data_files_list = []
	for us_data_file in us_data_files:
		file_path = datapath + "\\" + us_data_file
		if os.path.isfile(file_path):
			(name, exts) = os.path.splitext(us_data_file)
			if exts == '.csv':
				temp_in = pd.read_csv(file_path, parse_dates=['date'])
				variable_name = name[14: -6]
				temp_out = us_bbg_process(temp_in, variable_name, last_date)
				if temp_out.shape[0] > 0:
					us_data_files_list.append(temp_out)

	us_data = pd.concat(us_data_files_list, axis=1, join='outer')
	return us_data


if __name__ == "__main__":
	us_raw_data = us_dt_consolidate(USDATAPATH, last_date)
	us_raw_data.to_csv(RAWDATAPATH + "\\US_Raw_Data.csv")

	GDP_Growth = pd.read_csv(USDQTRATAPATH + '\\United States_GDP CQOQ Index.csv', parse_dates=['date'])
	GDP_QOQ = GDP_Growth.loc[:, ['date', 'ECO_RELEASE_DT', 'ACTUAL_RELEASE']]
	GDP_QOQ.dropna(inplace=True)
	GDP_QOQ['Release_date'] = GDP_QOQ['date'].map(lambda x: x + datetime.timedelta(days=10))
	GDP_QOQ['GDP_QOQ'] = GDP_QOQ['ACTUAL_RELEASE']
	GDP_QOQ.set_index('Release_date', inplace=True)
	GDP_QOQ = GDP_QOQ.resample('1M').last()
	GDP_QOQ.drop(columns=['ECO_RELEASE_DT','ACTUAL_RELEASE'], inplace=True)
	GDP_QOQ.to_csv(RAWDATAPATH + "\\US_GDP_Data.csv")
