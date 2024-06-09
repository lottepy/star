# -- coding: utf-8 --
import pandas as pd
import numpy as np
import math
import json
import os
import csv
import logging as log
import requests
from ast import literal_eval
import time

_session = requests.session()
IUID_BATCH_SIZE = 8

class FactorAPI(object):
	def __init__(self, endpoint = 'https://market.aqumon.com/v2/market/factors/fund/offshore'):
		if not endpoint:
			self.endpoint = 'https://market.aqumon.com/v2/market/factors/fund/offshore'
		else:
			self.endpoint = endpoint


	def get_single_factor(self, iuid = 'HK_20_F00000027I', factor_field = 'aum_factor_24M', start_date = '2018-01-01',end_date = '2018-12-31'):
		params = {
			'iuids': iuid,
			'factor_fields': factor_field,
			'start_date': start_date,
			'end_date': end_date,
			# 'version': '1',
			# "access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ"
		}
		result = _session.get(url=self.endpoint, params=params).json()

		if not result.get('status').get("ecode"):
			return result.get('data')
		else:
			return result
	#多iuids和factor_field的输入
	# iuids 是一个包含多个iuid的list
	# factor_filed 是包含多个field的list
	def get_muti_factor(self, iuids = ['HK_20_F00000027I','HK_20_F00000081N'], factor_field = ['aum_factor_1M','aum_factor_3M','aum_factor_6M','aum_factor_18M','aum_factor_24M'], start_date = '2013-01-01',end_date = '2018-12-31'):
		params = {
			'iuids': ','.join(iuids),
			'factor_fields': ','.join(factor_field),
			'start_date': start_date,
			'end_date': end_date,
			# 'version': '1',
			# "access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ"
		}
		result = _session.get(url=self.endpoint, params=params).json()

		if not result.get('status').get("ecode"):
			return result.get('data')
		else:
			return result

class Factor_Client(object):
	def __init__(self, ass_type = 'stock', region = 'HK'):
		#境外基金
		if ass_type =='fund' and (region == 'HK' or region == 'US'):
			self.endpoint = ass_type + '/' + 'offshore'
		#境内基金
		elif ass_type == 'fund' and region == 'CN':
			self.endpoint = ass_type + '/' + 'onshore'
		#stock
		elif ass_type == 'stock':
			self.endpoint = ass_type + '/' + region
		self.endpoint = 'https://test-market.aqumon.com/v2/market/factors/' + self.endpoint

		self.error_iuids=[]

	def get_batch_size(self,n):
		if n <= 4:
			return n
		num_of_batches = 2
		while math.ceil(n / num_of_batches) > IUID_BATCH_SIZE:
			num_of_batches += 1
		return math.ceil(n / num_of_batches)

	#iuids的csv的读取格式为：列名为'iuids'的列存储iuid
	#iuid组成的list保存在self.iuids中
	def load_iuids(self,path = '/Users/hefan/Desktop/factor_info/total_active/HK_20_active.csv'):
		iuids = pd.read_csv(path)
		iuids = np.array(iuids['iuids']).tolist()
		self.iuids= iuids
	#field的csv的读取格式为：列名为'field'的列存储field
	#field组成的list保存在self.fields中
	def load_field(self,path = '/Users/hefan/Desktop/factor_info/fields.csv'):
		fields = pd.read_csv(path)
		fields = np.array(fields['field']).tolist()
		self.fields= fields



	def getdata(self, iuid_path= '', field_path='', start_date = '2015-01-31', end_date = '2019-07-01',save_path=None):
		#初始化FactorAPI
		if save_path == None:
			save_path = ''
		# 如果路径不存在，则在当前目录下创建
		elif not os.path.exists(save_path):
			os.makedirs(save_path)
		factor = FactorAPI(self.endpoint)
		#load csv中的iuid和field
		self.load_iuids(iuid_path)
		self.load_field(field_path)
		#此处要加一个有关字段和iuid的判断
		total = len(self.iuids)
		batch_size = self.get_batch_size(total)
		iuid_batch = [self.iuids[i:i + batch_size] for i in range(0, total, batch_size)]
		dataframe_total = pd.DataFrame()
		for batch in iuid_batch:
			print(batch)
			get_factor = factor.get_muti_factor(iuids = batch, factor_field = self.fields, start_date = start_date, end_date = end_date)
			dataframe_batch = factor_client.dict_to_dataframe(get_factor)
			#如果save_path为空的话 直接写在当前目录
			dataframe_batch = dataframe_batch.sort_index()
			for iuid in batch:
				try:
					dataframe_batch[iuid][self.fields].to_csv(save_path+iuid+'.csv')
				except:
					self.error_iuids.append(iuid)
			pd.DataFrame({'error_iuid':self.error_iuids}).to_csv('error_iuids.csv')





			dataframe_total = pd.concat([dataframe_total,dataframe_batch],axis=1)
		return dataframe_total

	#data_dict:
	def dict_to_dataframe(self,data_dict):
		#index为第一层字典的key
		date_index = list(data_dict.keys())
		# 第二层字典的key 是 iuids
		try:
			iuid_index = list(data_dict[date_index[0]].keys())
		except:
			return pd.DataFrame()
		try:
			#第三层字典的key 是 fields
			field_index = list(data_dict[date_index[0]][iuid_index[0]].keys())
		except:
			return pd.DataFrame()
		#dataframe_total:总的dataframe
		dataframe_total=pd.DataFrame()
		#第一层循环 key为日期，value为记录每个iuid 当前日期数据的字典
		for date, iuid_table in data_dict.items():

			#每一个时间的dataframe
			dataframe_date = pd.DataFrame()
			#第二层字典 key为iuid value为记录该iuid的所有filed的字典
			for iuid, field_table in iuid_table.items():
				# 除去iuid和asof_date的field_dict
				field_dict2 = dict((key, value) for key, value in field_table.items() if key in field_index[2:])
				#创建双重columns的dataframe
				#dataframe_iuid = pd.DataFrame(columns=pd.MultiIndex.from_product([[iuid], list(field_dict2.keys())]))
				#添加因子数据
				dataframe_iuid = pd.DataFrame(field_dict2,columns=list(field_dict2.keys()),index = [field_table['asof_date']])
				dataframe_iuid.columns = pd.MultiIndex.from_product([[iuid],dataframe_iuid.columns])
				#dataframe_iuid[iuid].append(field_dict2, )
				#合并iuid的dataframe
				dataframe_date = pd.concat([dataframe_date,dataframe_iuid],axis=1)
			dataframe_total = pd.concat([dataframe_total,dataframe_date],axis=0)
		return dataframe_total

	def df_to_csv(self,df,csv_dir_path,name):
		df.to_csv(csv_dir_path+name+'.csv')

	def df_total_to_csv(self, df, csv_dir_path):
		for i in list(df.columns.levels[0]):
			df[i].to_csv(csv_dir_path+i+'.csv')







	# def merge_data(self, data1, data2):



#iuid的csv的路径
# iuid_path = 'CN_20_iuids.csv'
# iuid_path = 'iuid_sample.csv'
iuid_path = 'HK_10_csv.csv'

#field的csv路径
field_path = 'HK_10_field.csv'
# field_path = 'field_sample.csv'
#初始化Factor_Client类  ass_type为 'fund'或者'stock'
#region可以设置为'HK', 'US' 'CN'
factor_client = Factor_Client(ass_type = 'stock', region = 'HK')

#从数据库中调用因子的数据
#iuid_path: 存储iuid的csv的路径
#field_path: 存储要查询的因子field的csv路径
#start_date: 起始时间
#end_date: 结束时间
# data1=factor_client.getdata(iuid_path=iuid_path,field_path=field_path,start_date = '2010-12-31', end_date = '2019-06-30')
data1=factor_client.getdata(iuid_path=iuid_path,field_path=field_path,start_date = '2000-12-31', end_date = '2019-07-31',save_path='/Users/hefan/Desktop/factor_info/HK_10_test/')

#写入csv
# csv_path = '/Users/hefan/Desktop/factor_info/csv_dir/CN_20/'
#将每个iuid写成一个csv
#csv中index为date，columns为field
#factor_client.df_total_to_csv(data1, csv_path)

# print(data1.shape)

