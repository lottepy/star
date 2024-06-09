import random
import time

from django.apps import AppConfig


class CryptoServerConfig(AppConfig):
	name = 'crypto_server'

	def ready(self):
		random.seed(int(time.time()))
