import pandas as pd
import numpy as np
from influxdb import InfluxDBClient
import six
import abc
import time

@six.add_metaclass(abc.ABCMeta)
class DB(object):

	@abc.abstractmethod
	def marketData(self, iuids, fields, startTimestamp, endTimestamp, frequency, toDataFrame, convertTime):
		"""
		Fetch market data
		:param iuid: list of str, example ["CN_10_600000"]
		:param fields: list of str, example ["price", "volume"]
		:param startTimestamp: str, example "YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"
		:param endTimestamp: str, example "YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"
		:param frequency: str, example "1ms", default None
		:param toDataFrame: bool, example True
		:param convertTime: str, example 'Asia/Hong_Kong'/"UTC", default None
		:return: dict of Generator, or dict of DataFrame
		"""
		raise NotImplementedError

	@abc.abstractmethod
	def orderbookData(self, iuids, fields, startTimestamp, endTimestamp, frequency, toDataFrame, convertTime):
		"""
		Fetch orderbook data
		:param iuid: list of str, example ["CN_10_600000"]
		:param fields: list of str, example ["price", "volume"]
		:param startTimestamp: str, example "YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"
		:param endTimestamp: str, example "YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"
		:param frequency: str, example "1ms", default None
		:param toDataFrame: bool, example True
		:param convertTime: str, example 'Asia/Hong_Kong'/"UTC", default None
		:return: dict of Generator, or dict of DataFrame
		"""
		raise NotImplementedError


class InfluxDB(DB):
	__influx = InfluxDBClient(
		host='47.75.164.89',
		port='8086',
		database='market_data'
	)

	__VALID_MARKET_FIELDS = ['price', 'open', 'high', 'low', 'close', 'volume']
	__VALID_ORDERBOOK_FIELDS = ['b1', 'b2', 'b3', 'b4', 'b5',
								'a1', 'a2', 'a3', 'a4', 'a5',
								'bv1', 'bv2', 'bv3', 'bv4', 'bv5',
								'av1', 'av2', 'av3', 'av4', 'av5']
	__INVALID_IUIDS_TYPE_ERROR = "Invalid type of `iuids` (should be passed as list of strings)"
	__INVALID_FIELDS_TYPE_ERROR = "Invalid type of `fields` (should be passed as list of strings)"
	__INVALID_FIELDS_ERROR = "Invalid input of `fields`"

	def _convertAggregateQueries(self, fields):
		aggQueryList = []
		for field in fields:
			if "price" in field:
				aggQueryList.append(f"mean({field}) as {field}")
			elif "open" in field:
				aggQueryList.append(f"first(price) as {field}")
			elif "close" in field:
				aggQueryList.append(f"last(price) as {field}")
			elif "high" in field:
				aggQueryList.append(f"max(price) as {field}")
			elif "low" in field:
				aggQueryList.append(f"min(price) as {field}")
			elif "volume" in field:
				aggQueryList.append(f"sum({field}) as {field}")
			elif ("av" in field) or ("bv" in field):
				aggQueryList.append(f"sum({field}) as {field}")
			elif ("a" in field) or ("b" in field):
				aggQueryList.append(f"mean({field}) as {field}")
		return ",".join(aggQueryList)

	def _parseQueryResultSet(self, rs, toDataFrame=False, convertTime=None):
		res = {}
		for item in rs.items():
			iuid = item[0][1].get('iuid')
			rsIter = item[1]
			if toDataFrame:
				df = pd.DataFrame(list(rsIter))
				if convertTime is not None:
					df['timeReadable'] = pd.DatetimeIndex(
						pd.to_datetime(df['time'], unit='ms', utc=True)).tz_convert(convertTime)
				res[iuid] = df
			else:
				res[iuid] = rsIter
		return res

	def marketData(self, iuids, fields, startTimestamp, endTimestamp, frequency=None, toDataFrame=False, convertTime=None):
		assert isinstance(iuids, list), self.__INVALID_IUIDS_TYPE_ERROR
		assert isinstance(fields, list), self.__INVALID_FIELDS_TYPE_ERROR
		assert set(fields).issubset(set(self.__VALID_MARKET_FIELDS)), self.__INVALID_FIELDS_ERROR

		iuidReg = "|".join(iuids)
		queryFields = ",".join([f for f in fields if f in ['price','volume']])

		groupbyConditionList = ["iuid"]
		if frequency is not None:
			queryFields = self._convertAggregateQueries(fields) # "mean(price) as price, sum(volume) as volume"
			groupbyConditionList.append(f"time({frequency}) fill(none)")
		groupbyCondition = ",".join(groupbyConditionList)

		query = f"select {queryFields} from \"executions.cn.10\" " \
				f"where iuid=~ /{iuidReg}/ and time>='{startTimestamp}' and time<='{endTimestamp}' " \
				f"group by {groupbyCondition} order by time;"

		rs = self.__influx.query(query, epoch="ms")

		return self._parseQueryResultSet(rs, toDataFrame=toDataFrame, convertTime=convertTime)

	def orderbookData(self, iuids, fields, startTimestamp, endTimestamp, frequency=None, toDataFrame=False, convertTime=None):
		assert isinstance(iuids, list), self.__INVALID_IUIDS_TYPE_ERROR
		assert isinstance(fields, list), self.__INVALID_FIELDS_TYPE_ERROR
		assert set(fields).issubset(set(self.__VALID_ORDERBOOK_FIELDS)), self.__INVALID_FIELDS_ERROR

		iuidReg = "|".join(iuids)
		queryFields = ",".join(fields)

		groupbyConditionList = ["iuid"]
		if frequency is not None:
			queryFields = self._convertAggregateQueries(fields)  # "mean(a1) as a1, sum(av1) as av1"
			groupbyConditionList.append(f"time({frequency}) fill(none)")
		groupbyCondition = ",".join(groupbyConditionList)

		query = f"select {queryFields} from \"orderbook.cn.10\" " \
				f"where iuid=~ /{iuidReg}/ and time>='{startTimestamp}' and time<='{endTimestamp}' " \
				f"group by {groupbyCondition} order by time;"

		rs = self.__influx.query(query, epoch="ms")
		return self._parseQueryResultSet(rs, toDataFrame=toDataFrame, convertTime=convertTime)

class InfluxDBFutures(InfluxDB):
	"""This db query module is specific for futures.
	`marketData` and `orderbookData` are disabled; they are replaced by a single wrapped method `tickData`.
	"""
	def __new__(cls):
		if not hasattr(cls, 'instance'):
			cls.instance = super(InfluxDBFutures, cls).__new__(cls)
			cls.instance.__VALID_TICK_FIELDS = cls._InfluxDB__VALID_MARKET_FIELDS + cls._InfluxDB__VALID_ORDERBOOK_FIELDS
			cls.instance.__influx = cls._InfluxDB__influx
		return cls.instance

	def _convertAggregateQueries(self, fields):
		# For futures data, volume is aggregated, so instead of sum(volume), we want last(volume)
		aggQueryList = []
		for field in fields:
			if "price" in field:
				aggQueryList.append(f"mean({field}) as {field}")
			elif "open" in field:
				aggQueryList.append(f"first(price) as {field}")
			elif "close" in field:
				aggQueryList.append(f"last(price) as {field}")
			elif "high" in field:
				aggQueryList.append(f"max(price) as {field}")
			elif "low" in field:
				aggQueryList.append(f"min(price) as {field}")
			elif "volume" in field:
				aggQueryList.append(f"last({field}) as {field}")
			elif ("av" in field) or ("bv" in field):
				aggQueryList.append(f"sum({field}) as {field}")
			elif ("a" in field) or ("b" in field):
				aggQueryList.append(f"mean({field}) as {field}")
		return ",".join(aggQueryList)

	def marketData(self, iuids, fields, startTimestamp, endTimestamp, frequency=None, toDataFrame=False, convertTime=None):
		raise NotImplementedError

	def orderbookData(self, iuids, fields, startTimestamp, endTimestamp, frequency=None, toDataFrame=False, convertTime=None):
		raise NotImplementedError

	def tickData(self, iuids, fields, startTimestamp, endTimestamp, frequency=None, toDataFrame=False, convertTime=None):
		assert isinstance(iuids, list), self.__INVALID_IUIDS_TYPE_ERROR
		assert isinstance(fields, list), self.__INVALID_FIELDS_TYPE_ERROR
		assert set(fields).issubset(set(self.__VALID_TICK_FIELDS)), self.__INVALID_FIELDS_ERROR

		iuidReg = "|".join(iuids)
		queryFields = ",".join(fields)

		groupbyConditionList = ["iuid"]
		if frequency is not None:
			queryFields = self._convertAggregateQueries(fields)  # "mean(a1) as a1, sum(av1) as av1"
			groupbyConditionList.append(f"time({frequency},-8h) fill(none)")
		groupbyCondition = ",".join(groupbyConditionList)

		query = f"select {queryFields} from \"orderbook.cn.60\" " \
				f"where iuid=~ /{iuidReg}/ and time>='{startTimestamp}' and time<='{endTimestamp}' " \
				f"group by {groupbyCondition} order by time;"

		rs = self.__influx.query(query, epoch="ms")
		return self._parseQueryResultSet(rs, toDataFrame=toDataFrame, convertTime=convertTime)

	def cumVolumeByTradeDay(self, mainSymbol, tradeday, toDataFrame=True,convertTime='Asia/Hong_Kong'):
		query = f"select last(volume) as volume from \"orderbook.cn.60\" " \
				f"where iuid=~ /CN_60_{mainSymbol}[\d+]/ and tradeday={tradeday} " \
				f"group by iuid;"
		rs = self.__influx.query(query, epoch="ms")
		rs_dict = self._parseQueryResultSet(rs, toDataFrame=toDataFrame, convertTime=convertTime)
		return rs_dict

if __name__=="__main__":

	# """For futures"""
	# influxdb = InfluxDBFutures()
	# iuids = ["CN_10_000054", ]
	# fields = ["open","high","low","close", "volume"]
	# fields = fields + (
	# 	['b1', 'b2', 'b3', 'b4', 'b5',
	# 	 'a1', 'a2', 'a3', 'a4', 'a5',
	# 	 'bv1', 'bv2', 'bv3', 'bv4', 'bv5',
	# 	 'av1', 'av2', 'av3', 'av4', 'av5']
	# )
	# startTimestamp = "2019-02-25T09:24:00.001000000+08:00"
	# endTimestamp = "2019-02-28T15:00:01.001000000+08:00"
	#
	# # tick data (include both market data and orderbook data)
	# tic = time.time()
	# res3 = influxdb.tickData(iuids, fields, startTimestamp, endTimestamp, frequency="1ms",
	# 						 toDataFrame=True, convertTime='Asia/Hong_Kong')
	# toc = time.time()
	# print("Elapsed time: {:.2f}".format(toc - tic))

	# # query holdings
	# mainSymbol = 'a'
	# tic = time.time()
	# res4 = influxdb.holdingsByDate(mainSymbol, startTimestamp, endTimestamp,
	# 							   toDataFrame=True, convertTime='Asia/Hong_Kong')
	# toc = time.time()
	# print("Elapsed time: {:.2f}".format(toc - tic))
	#
	# print("Test Futures Done.")

	# """For stocks"""
	# influxdb = InfluxDB()
	# iuids = ["CN_10_600000", ]  # "CN_10_000054"]
	# fields = ["open", "high", "low", "close", "volume"]
	# startTimestamp = "2018-12-25T09:24:00.001000000+08:00"
	# endTimestamp = "2018-12-28T15:00:01.001000000+08:00"
	#
	# # Market data
	# tic = time.time()
	# res1 = influxdb.marketData(iuids, fields, startTimestamp, endTimestamp, frequency="1ms",
	# 						   toDataFrame=True, convertTime='Asia/Hong_Kong')
	# toc = time.time()
	# print("Elapsed time: {:.2f}".format(toc - tic))

	# Orderbook data
	influxdb = InfluxDB()
	iuids = ["CN_10_300230" ]  # "CN_10_000054"]
	fields = ['b1', 'b2', 'b3', 'b4', 'b5',
			 'a1', 'a2', 'a3', 'a4', 'a5',
			 'bv1', 'bv2', 'bv3', 'bv4', 'bv5',
			 'av1', 'av2', 'av3', 'av4', 'av5']
	startTimestamp = "2018-01-01T09:24:00.001000000+08:00"
	endTimestamp = "2018-02-03T15:00:01.001000000+08:00"
	tic = time.time()
	res2 = influxdb.orderbookData(iuids, fields, startTimestamp, endTimestamp, frequency="1ms",
							      toDataFrame=False, convertTime='Asia/Hong_Kong')
	toc = time.time()
	print("Elapsed time: {:.2f}".format(toc-tic))

	print("Test Stocks Done.")

