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




if __name__=="__main__":
	influxdb = InfluxDB()
	iuids = ["CN_10_600016", ]#"CN_10_600016"]
	fields = ["price","volume"]
	# startTimestamp = "2017-12-31T09:24:00.001000000Z"
	startTimestamp = "2018-12-25T09:24:00.001000000Z"
	endTimestamp = "2018-12-28T15:00:01.001000000Z"
	frequency = "1ms"

	tic = time.time()
	res1 = influxdb.marketData(iuids, fields, startTimestamp, endTimestamp, frequency="1ms",
							  toDataFrame=False, convertTime='Asia/Hong_Kong')
	toc = time.time()
	print("Elapsed time: {:.2f}".format(toc - tic))

	fields = ['b1', 'b2', 'b3', 'b4', 'b5',
			 'a1', 'a2', 'a3', 'a4', 'a5',
			 'bv1', 'bv2', 'bv3', 'bv4', 'bv5',
			 'av1', 'av2', 'av3', 'av4', 'av5']
	startTimestamp = "2016-02-01T09:24:00.001000000Z"
	endTimestamp = "2016-04-01T15:00:01.001000000Z"

	tic = time.time()
	res2 = influxdb.orderbookData(iuids, fields, startTimestamp, endTimestamp, frequency="1ms",
							  toDataFrame=False, convertTime='Asia/Hong_Kong')
	toc = time.time()
	print("Elapsed time: {:.2f}".format(toc-tic))

	print("Done.")


