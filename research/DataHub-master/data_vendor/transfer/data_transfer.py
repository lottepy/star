import pymysql
import pandas as pd
import pandas.io.sql as pdsql
import time
import json
import dataset
import datetime

from lib.commonalgo.setting.constants import DBsetting
from lib.commonalgo.data.zipline_parser import ZiplineResultConverter, ZiplineResultLogger
from lib.commonalgo.database.algo_marketapi import post_localcsv,MarketAlgoAPI

class Data_Transer(object):
	# update test2_big_account
	def __init__(self,read_db = {}, write_db = {} ):
		self.fromdb = dataset.connect(
			"mysql+pymysql://{}:{}@{}:{}/{}".format(
				read_db['DB_USER'],
				read_db['DB_PASS'],
				read_db['DB_HOST'],
				read_db['DB_PORT'],
				read_db['DB_NAME']))
		self.todb = dataset.connect(
			"mysql+pymysql://{}:{}@{}:{}/{}".format(
				write_db['DB_USER'],
				write_db['DB_PASS'],
				write_db['DB_HOST'],
				write_db['DB_PORT'],
				write_db['DB_NAME']))
		self.weight_precision = 4
		self.position_precision = 6
		self.bt_precision = 4

	def read_modelid(self, algo_type_id = 0):
		fromdb = self.fromdb
		modelid = list(fromdb['algo_model'].find(algo_type_id=algo_type_id))
		return pd.DataFrame(modelid)


# db_transfer = Data_Transer(read_db=DBsetting.test_algo_db, write_db=DBsetting.omni_algo_db)
#
# modelid_data = db_transfer.read_modelid(8)
#
# for row in modelid_data.iterrows():
# 	version = row[1].get('version')
#
# 	print (version)
file_log = ZiplineResultLogger('post2database_unhedge.csv', initialize=False)
post_localcsv(file_log.filename, endpoint='http://47.91.157.91:6666/v2/algo/')
