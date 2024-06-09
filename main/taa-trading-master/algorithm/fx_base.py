import pandas as pd
import numpy as np
from datetime import datetime
import pysftp
import logging
import requests
import time
import json
import os
from utils.fx_client_lib.fx_client import get_oss_data
from utils.md5_api import md5_verification
from utils.constants import *
from fx_daily.data.data_prepare.fillna import process_fill_ndf
from fx_daily.data.data_prepare.quote_in_usd_daily import process_quote_in_usd
from fx_daily.data.data_prepare.spot_ndf_together import process_spot_ndf_data
from fx_daily.data.data_prepare.update_rate import process_rate_update
from fx_daily.core.engine import ForexEngine

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
HOST = 'ftp.dymonasia.com'
USER = 'magnumwm'
PSWD = 'D7yu#3iQr!'

ENV = os.getenv('DYMON_FX_ENV', 'dev_algo')

class ForexLive(object):
	def __init__(self, start, end, symbols, algo_list, compound_method='EW'):
		self.start = start
		self.end = end
		self.today = datetime.today().strftime('%Y%m%d')
		self.symbols = symbols
		self.strategy_list = algo_list
		self.compound_method = compound_method
		self._init_logger()
		self.traded_symbols = self._get_traded_symbols()
		self.ccy_matrix_df = self._get_ccy_matrix()

		self.data = None
		self.time = None

		self.positions = None
		self.portfolio_value = 0.
		self.aum = 5e+6

		self.sub_weight = []
		self.final_weight = np.zeros(len(self.traded_symbols))

	def __str__(self):
		return f'时间：{self.start} ~ {self.end} | 标的：{self.symbols} | 策略：{[s.strategy_name for s in self.strategy_list]}'

	def _get_traded_symbols(self):
		return [s for s in self.symbols if 'USD' in s]

	def _get_ccy_matrix(self):
		base_ccy_list = [sym[:3] for sym in self.symbols]
		quoted_ccy_list = [sym[3:] for sym in self.symbols]
		all_ccy = list(set(base_ccy_list + quoted_ccy_list))
		ccy_matrix_df = pd.DataFrame(np.zeros((len(all_ccy), len(self.symbols))), index=all_ccy, columns=self.symbols)
		for sym in self.symbols:
			base_ccy = sym[:3]
			quoted_ccy = sym[3:]
			ccy_matrix_df.loc[base_ccy, sym] = 1
			ccy_matrix_df.loc[quoted_ccy, sym] = -1
		return ccy_matrix_df

	def _get_ccy_exposure(self, weight):
		ccy_exposure = np.dot(self.ccy_matrix_df.values, weight)
		return pd.Series(ccy_exposure, index=self.ccy_matrix_df.index)

	def _connect_sftp(self):
		return pysftp.Connection(host=HOST, username=USER, password=PSWD, cnopts=cnopts)

	def _close_sftp(self, sftp):
		return sftp.close()

	def _init_logger(self):
		self.logger = logging.getLogger('FX-Dymon')
		self.logger.setLevel(logging.DEBUG)

		formatter = logging.Formatter('%(asctime)s — %(name)s — %(levelname)s — %(filename)s:%(lineno)d — %(message)s')
		sh = logging.StreamHandler()
		sh.setLevel(logging.DEBUG)
		sh.setFormatter(formatter)
		self.logger.addHandler(sh)

		fh = logging.FileHandler(f'{LOG_PATH}/smart_fx/fx_{self.today}.log', encoding='utf-8')
		fh.setLevel(logging.DEBUG)
		fh.setFormatter(formatter)
		self.logger.addHandler(fh)

		self.logger.info('Logger初始化完成')
		self.logger.info(self.__str__())

	def update_backtest_data(self):
		process_spot_ndf_data()
		self.logger.info(f'[更新回测数据] spot和NDF数据更新完成')
		process_fill_ndf()
		self.logger.info(f'[更新回测数据] NDF缺失数据补充完成')
		process_quote_in_usd()
		self.logger.info(f'[更新回测数据] 以USD计价的price数据更新完成')
		process_rate_update()
		self.logger.info(f'[更新回测数据] 利率数据更新完成')
		self.logger.info(f'[更新回测数据] 回测依赖数据全部更新完成')

		test_weight = np.random.randn(len(ALL_CCY_NO_CROSS))
		try:
			fx_bt = ForexEngine(
				config_file=os.path.join(BT_CONFIG_PATH, 'config_var.ini'),
				start=datetime(2011, 12, 31),
				end=datetime.strptime(self.end, '%Y-%m-%d'),
				symbols=ALL_CCY_NO_CROSS,
				params=test_weight,
				mode='var_once'
			)
			var = fx_bt.run()
			dd = DingDingMsg(data='FX回测数据更新完成，并已通过回测检验')
			dd.send_msg()
			self.logger.info(f'var_once回测测试通过, 测试var={var}')
		except Exception as ee:
			print(ee)
			dd = DingDingMsg(data='FX回测数据更新异常，无法进行回测')
			dd.send_msg()
			self.logger.info(f'var_once回测测试失败, 无法测试var')

	def load_market_data(self):
		self.data, self.time = get_oss_data(self.symbols, self.start, self.end)
		self.logger.info('数据获取成功')
		self.last_prices = self.data[-1, :, 0]
		self.last_time = self.time[-1]
		self.logger.info(f'最近一期数据：{dict(zip(self.traded_symbols, self.last_prices))}')
		self.logger.info(f'最近一期时间：{self.last_time}')

	def get_holdings(self):
		sftp = self._connect_sftp()
		sftp.cwd('/Live/')
		directory_files = sftp.listdir()
		for f in directory_files:
			if f'Dymon2Aqumon_Position_{self.today}' in f:
				remote_filename = f
				local_filename = 'position.csv'
				oss_path = {
					'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/Dymon_FTP/",
					'dev_jeff': '../0_DymonFx/Dymon_FTP/',
					'live': '../0_DymonFx/Dymon_FTP/'
				}[ENV]

				oss_filename = oss_path + remote_filename
				sftp.get(remote_filename, local_filename)
				sftp.get(remote_filename, oss_filename)
				self._close_sftp(sftp)
				self.logger.info(f'FTP下载持仓文件成功，文件名：{f}')

				# Check MD5 code
				md5 = md5_verification()
				md5.verify_md5_file(source_filepath=remote_filename)

				holdings = pd.read_csv('position.csv')
				holdings['ccp'] = holdings['CCY1'] + holdings['CCY2']
				holdings['usd_amt'] = np.where(holdings['CCY1'] == 'USD', holdings['Amount1'].abs(), holdings['Amount2'].abs())\
									  * np.sign(holdings['Amount1'])
				holdings = holdings.set_index('ccp')
				portfolio_value = holdings['usd_amt'].abs().sum()
				holdings['weight'] = holdings['usd_amt'] / portfolio_value
				self.logger.info(f'当前持仓：\n{holdings.to_string()}')
				self.logger.info(f'当前组合价值：{portfolio_value} USD')
				return holdings, portfolio_value

		raise NotImplementedError

	def generate_weight(self):
		for strategy in self.strategy_list:
			strategy.set_logger(self.logger)
			self.sub_weight.append(strategy.generate_weight(self.data, self.time, self.traded_symbols))
		self.final_weight = self._compound_weight(method=self.compound_method)
		self.logger.info(dict(zip(self.traded_symbols, self.final_weight)))

	def _compound_weight(self, method='EW'):
		self.logger.info(f'合成权重，合成方法为：{method}')
		if method == 'EW':
			return np.array(self.sub_weight).mean(axis=0)
		elif method == 'VaR':
			equal_weight = self._compound_weight(method='EW')
			self.currency_exposure = self._get_ccy_exposure(equal_weight)
			usd_exposure = abs(self.currency_exposure['USD'])
			max_non_usd_exposure = self.currency_exposure[self.currency_exposure.index != 'USD'].abs().max()
			self.logger.info(f'USD暴露度：{usd_exposure}, 非USD最大暴露度：{max_non_usd_exposure}')
			self.logger.info(f'每个币种暴露度：\n{self.currency_exposure}')

			fx_bt = ForexEngine(
				config_file=os.path.join(BT_CONFIG_PATH, 'config_var.ini'),
				start=datetime(2011, 12, 31),
				end=datetime.strptime(self.end, '%Y-%m-%d'),
				symbols=ALL_CCY_NO_CROSS,
				params=equal_weight,
				mode='var_once'
			)
			var = fx_bt.run()
			var_scale = VAR_LIMIT * 0.9 / abs(var)
			max_usd_scale = USD_LIMIT / usd_exposure
			max_non_usd_scale = NON_USD_LIMIT / max_non_usd_exposure
			leverage_ratio = min(var_scale, max_usd_scale, max_non_usd_scale)
			self.logger.info(f'[VaR方法合成] VaR = {var}, leverage_ratio = {leverage_ratio}')
			return equal_weight * leverage_ratio

	def send_orders(self):
		self.final_weight_df = self._prepare_weight_csv()
		self.final_orders_df = self._prepare_orders_csv()

		# path = '//192.168.9.170/share/alioss/0_DymonFx/Dymon_FTP/'  # web disk
		path = {
			'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/Dymon_FTP/",
			'dev_jeff': '../0_DymonFx/Dymon_FTP/',
			'live': '../0_DymonFx/Dymon_FTP/'
		}[ENV]

		TargetRatio_filename = f'Aqumon2Dymon_TargetRatio_{self.today}.csv'
		Order_filename = f'Aqumon2Dymon_Order_{self.today}.csv'
		self.final_weight_df.to_csv(path + TargetRatio_filename, index=False)
		self.final_orders_df.to_csv(path + Order_filename, index=False)
		self.logger.info(f'权重CSV：\n{self.final_weight_df.to_string()}')
		self.logger.info(f'订单CSV：\n{self.final_orders_df.to_string()}')

		# check oss files
		from lib.commonalgo.data.oss_client import oss_client
		retry_count = 0
		while (True):
			retry_count += 1
			if retry_count > 10:
				dd = DingDingMsg('FX:oss delay [TargetRatio][Order][csv]')
				dd.send_msg()
			if oss_client.exists(os.path.join('0_DymonFx/Dymon_FTP/', TargetRatio_filename)) and oss_client.exists(os.path.join('0_DymonFx/Dymon_FTP/', Order_filename)):
				break
			time.sleep(1)
			print(f'retry {retry_count} times uploading file to oss.')

		# generate MD5 code
		md5 = md5_verification()
		md5.create_md5_code_file(source_filepath=TargetRatio_filename)
		md5.create_md5_code_file(source_filepath=Order_filename)

		# check oss files
		TargetRatio_filename_md5 = TargetRatio_filename.replace('.csv', '.md5')
		Order_filename_md5 = Order_filename.replace('.csv', '.md5')
		retry_count = 0
		while (True):
			retry_count += 1
			if retry_count > 10:
				dd = DingDingMsg('FX:oss delay [TargetRatio][Order][MD5]')
				dd.send_msg()
			if oss_client.exists(os.path.join('0_DymonFx/Dymon_FTP/', TargetRatio_filename_md5)) and oss_client.exists(os.path.join('0_DymonFx/Dymon_FTP/', Order_filename_md5)):
				break
			time.sleep(1)
			print(f'retry {retry_count} times uploading file to oss.')

		sftp = self._connect_sftp()
		sftp.put(path + TargetRatio_filename, f'Live/{TargetRatio_filename}')
		sftp.put(path + TargetRatio_filename, f'Live/{Order_filename}')
		sftp.put(path + TargetRatio_filename, f'Live/{TargetRatio_filename_md5}')
		sftp.put(path + TargetRatio_filename, f'Live/{Order_filename_md5}')


		self._close_sftp(sftp)
		dd = DingDingMsg(data='FX每日更新完成，权重和订单文件上传完毕')
		dd.send_msg()
		self.logger.info('权重和订单文件上传完毕')

	def _prepare_weight_csv(self):
		weight_info = np.vstack((self.traded_symbols, self.final_weight)).T
		weight_df = pd.DataFrame(weight_info, columns=['ccp', 'TargetRatio'])
		weight_df['CCY1'] = weight_df['ccp'].str[:3]
		weight_df['CCY2'] = weight_df['ccp'].str[3:]
		weight_df['Portfolio'] = 'AQM'
		self.logger.info(f'目标权重：\n{weight_df.to_string()}')
		return weight_df[TARGETRATIO_HEADER]

	def _prepare_orders_csv(self):
		imm_fix_date, imm_settle_date = self.get_IMM_date()

		orders_df = self.final_weight_df.copy()
		orders_df['ccp'] = orders_df['CCY1'] + orders_df['CCY2']
		orders_df['FixingDate'] = np.where(orders_df['ccp'].isin(NDF_CCP), imm_fix_date, 'SPOT')
		orders_df['SettlementDate'] = np.where(orders_df['ccp'].isin(NDF_CCP), imm_settle_date, 'SPOT')
		orders_df['target_usd_amt'] = orders_df['TargetRatio'].astype(float) * self.aum
		orders_df['current_usd_amt'] = self.positions['usd_amt'].reindex(orders_df['ccp'].values).fillna(0.).values
		orders_df['order_usd_amt'] = orders_df['target_usd_amt'] - orders_df['current_usd_amt']
		orders_df['order_other_amt'] = np.where(
			orders_df['CCY1'] == 'USD',
			orders_df['order_usd_amt'] * self.last_prices,
			orders_df['order_usd_amt'] / self.last_prices
		).astype(np.int64) // 1000 * 1000
		orders_df['Amount1'] = np.where(orders_df['CCY1'] == 'USD', '', orders_df['order_other_amt'])
		orders_df['Amount2'] = np.where(orders_df['CCY2'] == 'USD', '', -orders_df['order_other_amt'])
		self.logger.info(f'目标订单：\n{orders_df.to_string()}')
		return orders_df[ORDER_HEADER]

	def get_IMM_date(self):
		return IMM_FIX_DATE, IMM_SETTLE_DATE

	def run(self):
		self.load_market_data()
		self.positions, self.portfolio_value = self.get_holdings()

		self.generate_weight()
		self.send_orders()


class ForexStrategyBase(object):
	def __init__(self, **kwargs):
		self.strategy_name = self.__class__.__name__
		for k, v in kwargs.items():
			setattr(self, k, v)

	def set_logger(self, logger):
		self.logger = logger

	def generate_weight(self, window_data, time_data, traded_ccp):
		raise NotImplementedError


class EqualWeight(ForexStrategyBase):
	def __init__(self, **kwargs):
		super().__init__(**kwargs)

	def generate_weight(self, window_data, time_data, traded_ccp):
		today = datetime.strptime('/'.join(time_data[-1][-3:].astype(int).astype(str)), '%Y/%m/%d')
		n_traded_ccp = len(traded_ccp)

		target_weight = np.ones(n_traded_ccp) / n_traded_ccp

		self.target_weight_df = pd.DataFrame(target_weight, columns=[today], index=traded_ccp)
		self.logger.info(f'[{self.strategy_name}] | 目标权重：\n{self.target_weight_df}')
		return target_weight


class DingDingMsg(object):
	def __init__(self, data):
		# self.url = 'https://oapi.dingtalk.com/robot/' \
		# 		   'send?access_token=673b8f53d8c5e8e4800528a6add7bf745f6452feda7edbb81a4a57078af57003'  # FX
		# self.url = 'https://oapi.dingtalk.com/robot/' \
		# 		   'send?access_token=1f940ef402891ada76d0c6ac1aad8f9d5bcbf77f517b038600845b4404a7bd52' # test
		self._set_url()
		self.headers = {"content-type": "application/json"}
		self.time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
		self.data_to_post = self._convert_data(data)

	def _set_url(self):
		if os.getenv('DYMON_FX_ENV','dev_algo') == 'dev_jeff': # jeff added
			self.url = 'https://oapi.dingtalk.com/robot/send?access_token=cc73f1d9b98d3921cec03d308d83f285577830611a1e89132833f920d6f440ec'
		elif os.getenv('DYMON_FX_ENV','dev_algo') == 'dev_algo': # junxin added
			self.url = 'https://oapi.dingtalk.com/robot/send?access_token=1f940ef402891ada76d0c6ac1aad8f9d5bcbf77f517b038600845b4404a7bd52'
		elif os.getenv('DYMON_FX_ENV','dev_algo') == 'live': # junxin added
			self.url = 'https://oapi.dingtalk.com/robot/send?access_token=1f940ef402891ada76d0c6ac1aad8f9d5bcbf77f517b038600845b4404a7bd52'

	def _convert_data(self, data):
		temp = {
			'msgtype': 'text',
			'text': {
				'content': f'{[self.time]} {data}'
			}
		}
		return json.dumps(temp).encode('utf-8')

	def send_msg(self):
		requests.post(self.url, headers=self.headers, data=self.data_to_post)


if __name__ == '__main__':
	start_date = '2019-01-01'
	end_date = '2019-12-03'
	symbols = ['USDINR', 'USDKRW', 'EURUSD']
	algo_list = [EqualWeight]

	fx = ForexLive(
		start=start_date,
		end=end_date,
		symbols=symbols,
		algo_list=algo_list,
		compound_method='VaR'
	)
	fx.run()