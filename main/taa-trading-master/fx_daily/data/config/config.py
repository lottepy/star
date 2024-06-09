import configparser
import urllib
import os

class ConfigManager(object):
	configs = {}

	def __init__(self, config_file_path: str):
		parser = configparser.ConfigParser()
		parser.read(config_file_path)

		self.configs['cash'] = parser.getint('account', 'cash')
		self.configs['cash0'] = parser.getint('account', 'cash_cny')
		self.configs['commission'] = parser.getfloat('account', 'commission')

		self.configs['window_size'] = parser.getint('algo', 'window_size')
		self.configs['strategy_module'] = parser.get('algo', 'strategy_module')
		self.configs['strategy_name'] = parser.get('algo', 'strategy_name')

		self.configs['root_path'] = parser.get('path', 'ROOT_PATH')
		self.configs['save_path'] = os.path.join(self.configs['root_path'], parser.get('path', 'SAVE_PATH'))
		self.configs['weight_path'] = os.path.join(self.configs['root_path'], parser.get('path', 'WEIGHT_PATH'))

		self.configs['display'] = parser.getint('settings', 'display')

	def __str__(self):
		return str(self.configs)

	def __getattr__(self, attr: str):  # 调用不存在的实例属性时
		return self.configs.get(attr)


if __name__ == '__main__':
	cp = ConfigManager('../data/config/config.ini')
	print(cp)
