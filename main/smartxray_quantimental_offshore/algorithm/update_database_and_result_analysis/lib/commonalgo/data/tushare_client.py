import tushare as ts

class TushareClient(object):
	def __init__(self,api_token = '384ac2601c024bc0dd3d564daa2210975ed96a41cfce52a5ef0d361e'):
		self.API = api_token

	def DataApi(self):
		return ts.pro_api(self.API)