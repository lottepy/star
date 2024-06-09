# -*- coding:utf-8 -*-
# from futuquant import *
# futuquant has been replaced by futu, please pip install first

from futu import OpenQuoteContext
import sys
# API_PORT = 22222
# TELNET_PORT = 22223
class FutuClient(object):
	def __init__(self, server='public', host = None, port = 22222):
		self.PORT = port
		if server == 'public':
			self.HOST = 'rabbitmq.aqumon.com'

		elif server == 'internal':
			self.HOST = '192.168.11.62'
		else:
			self.HOST = host

	@property
	def connect(self):
		try:
			quote_ctx = OpenQuoteContext(host=self.HOST, port=self.PORT)
		except:
			print ('server error: {}：{}'.format(self.HOST, self.PORT))
			quote_ctx = None
		return quote_ctx
