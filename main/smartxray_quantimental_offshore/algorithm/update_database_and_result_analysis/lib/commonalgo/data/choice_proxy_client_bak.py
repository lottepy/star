import json
import platform
import socket
import struct
import pandas as pd

HOST = '47.91.157.91'
PORT = 8394
# if reach data limit, change port to 8294 (IT use account)


class GTcpClient(object):

	def __init__(self, address, port, timeout=60, retry=True):
		self._address = address
		self._port = port
		self._socket = None
		self._timeout = timeout
		self._retry = retry

	def close(self):
		if self._socket is not None:
			self._socket.close()
			self._socket = None

	def send(self, request):
		if self._socket is None:
			if not self._connect():
				return False
		packet = struct.pack('<I%ds' % len(request), len(request), request)
		try:
			self._socket.sendall(packet)
			return True
		except Exception as ex:
			return False

	def receive(self):
		if self._socket is None:
			if not self._connect():
				return None
		try:
			length_data = self._recv(4)
			length = struct.unpack('<I', length_data)[0]
			return self._recv(length)
		except Exception as ex:
			self.close()
			return None

	def request(self, request):
		if self._socket is None:
			if not self._connect():
				return None
		packet = struct.pack('<I%ds' % len(request), len(request), request.encode('utf8'))
		try:
			self._socket.sendall(packet)
			length_data = self._recv(4)
		except Exception as ex:
			self.close()
			if not self._retry:
				return None
			if isinstance(ex, socket.timeout):
				return None
			if not self._connect():
				return None
			try:
				self._socket.sendall(packet)
				length_data = self._recv(4)
			except Exception as ex:
				self.close()
				return None
		try:
			length = struct.unpack('<I', length_data)[0]
			return self._recv(length)
		except Exception as ex:
			self.close()
			return None

	def _connect(self):
		try:
			self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			if self._timeout > 0:
				self._socket.settimeout(self._timeout)
			self._set_keep_alive()
			self._socket.connect((self._address, self._port))
			return True
		except Exception as ex:
			self.close()
			return False

	def _recv(self, length):
		data = b''
		while length > 0:
			recv_data = self._socket.recv(length)
			recv_length = len(recv_data)
			if recv_length <= 0:
				raise Exception('socket_closed')
			data += recv_data
			length -= recv_length
		return data

	def _set_keep_alive(self):
		self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
		if platform.system().lower() == 'linux':
			self._socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 30)
			self._socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 5)
			self._socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 5)


class Choice(object):
	def __init__(self):
		self.client = GTcpClient(HOST, PORT)

	def request(self, payload):
		response = self.client.request(json.dumps(payload))
		return json.loads(response)

	def __getattr__(self, item):
		if item not in self.__dict__:
			def func(*args, **kwargs):
				result = self.request({
					'cmd': item,
					'args': args,
					'kwargs': kwargs
				})
				series = {}
				# if not result['Dates']:
				# 	df = pd.DataFrame.from_dict(result['Data']).T
				# 	df.columns = result['Indicators']
				# 	return df
				indicators = result['Indicators']
				if result['Dates'] and isinstance(result['Data'],dict):
					for code, data in result['Data'].items():
						for i, v in enumerate(data):
							s = pd.Series(v, index=result['Dates'])
							series[(code, indicators[i])] = s
					return pd.DataFrame.from_dict(series)
				elif isinstance(result['Data'],list):
					df = pd.DataFrame(result['Data'])
					if len(indicators) == len(df):
						return pd.DataFrame(data=df.T.values, index=result['Dates'],columns=indicators)
					elif len(indicators) == df.shape[1]:
						return pd.DataFrame(data=df.values, columns=indicators)
					else:
						# parse c.sector result
						name_list = [x for x in result['Data'] if x not in result['Codes']]
						return pd.DataFrame(data=[result['Codes'], name_list]).T
				elif not result['Dates']:
					df = pd.DataFrame(result['Data'])
					return pd.DataFrame(data=df.T.values,columns=indicators)

			self.__setattr__(item, func)
			return func


choice_client = Choice()
choice_client.start()

"""
Usage: all APIs are identical to choice native api
from client import c
df = c.csd("300059.SZ,600425.SH", "open,close", "2016-07-01", "2016-07-06", "RowIndex=1,period=1,adjustflag=1,curtype=1,pricetype=1,year=2016,Ispandas=0")
print(df.to_string())
"""