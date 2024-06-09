import pysftp
import logging
import requests
import time
import json
import numpy as np
from utils.fx_client_lib.fx_client import get_oss_data
from utils.md5_api import md5_verification_local, md5_verification
from utils.constants import *
from utils.fx_convertor import *
from lib.commonalgo.data.oss_client import oss_client

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
HOST = 'ftp.dymonasia.com'
USER = 'magnumwm'
PSWD = 'D7yu#3iQr!'

ENV = os.getenv('DYMON_FX_ENV', 'dev_algo')

class ForexLive(object):

	def __init__(self, today, symbols):

		self.today = today
		self.yesterday = get_fx_yesterday(datetime.strptime(self.today,'%Y%m%d')).strftime('%Y%m%d')
		self.symbols = symbols
		self._init_logger()
		self.traded_symbols = self._get_traded_symbols()
		self.ccy_matrix_df = self._get_ccy_matrix()
		self.md5_local = md5_verification_local()

	def __str__(self):
		return f'时间：{self.today} | 标的：{self.symbols}'

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

	def load_market_data(self):
		self.last_price_df = self.T150_price_df.rename(columns={'T150_price':'last_price'})

	def load_T150_data(self):
		self.data, self.time = get_oss_data(self.symbols, self.yesterday, self.today)
		self.logger.info('数据获取成功')
		self.T150_price = self.data[-1, :, 0]
		self.last_time = self.time[-1]
		self.T150_price_df = pd.DataFrame(dict(zip(self.symbols, self.T150_price)).items(),columns=['ccp','T150_price'])
		self.logger.info(f'最近一期数据：{dict(zip(self.traded_symbols, self.T150_price))}')
		self.logger.info(f'最近一期时间：{self.last_time}')

	def get_holdings(self):
		ts = datetime.now()
		sftp = self._connect_sftp()
		sftp.cwd('/Live/')
		directory_files = sftp.listdir()
		for f in directory_files:
			# 需要改成昨天的
			if f'Dymon2Aqumon_Position_{self.yesterday}' in f:
				remote_filename = f
				local_filename = f
				sftp.get(remote_filename, local_filename)
				self.logger.info(f'FTP下载持仓文件成功，文件名：{f}')
		self._close_sftp(sftp)

		# Check MD5 code
		if os.path.exists(f'Dymon2Aqumon_Position_{self.yesterday}.csv'):
			self.md5_local.verify_md5_file(source_filepath=f'Dymon2Aqumon_Position_{self.yesterday}.csv')
			holdings = pd.read_csv(f'Dymon2Aqumon_Position_{self.yesterday}.csv')
			holdings['ccp'] = holdings['CCY1'] + holdings['CCY2']
			te = datetime.now()
			self.logger.info(f"读取目标仓位时间:{te-ts}")
			return holdings

		raise NotImplementedError


	def send_orders(self):

		ts = datetime.now()

		self.final_weight_df = self._prepare_weight_csv()
		self.final_orders_df = self._prepare_orders_csv()

		TargetRatio_filename = f'Aqumon2Dymon_TargetRatio_{self.today}.csv'
		Order_filename = f'Aqumon2Dymon_Order_{self.today}.csv'
		self.final_weight_df.to_csv(TargetRatio_filename, index=False)
		self.final_orders_df.to_csv(Order_filename, index=False)
		self.logger.info(f'权重CSV：\n{self.final_weight_df.to_string()}')
		self.logger.info(f'订单CSV：\n{self.final_orders_df.to_string()}')

		# generate MD5 code
		self.md5_local.create_md5_code_file(source_filepath=TargetRatio_filename)
		self.md5_local.create_md5_code_file(source_filepath=Order_filename)

		# check oss files
		TargetRatio_filename_md5 = TargetRatio_filename.replace('.csv', '.md5')
		Order_filename_md5 = Order_filename.replace('.csv', '.md5')

		sftp = self._connect_sftp()

		sftp.put(TargetRatio_filename, f'Live/{TargetRatio_filename}')
		sftp.put(Order_filename, f'Live/{Order_filename}')
		sftp.put(TargetRatio_filename_md5, f'Live/{TargetRatio_filename_md5}')
		sftp.put(Order_filename_md5, f'Live/{Order_filename_md5}')

		self._close_sftp(sftp)

		dd = DingDingMsg(data='FX每日更新完成，权重和订单文件上传完毕')
		dd.send_msg()
		self.logger.info('权重和订单文件上传完毕')

		te = datetime.now()
		self.logger.info(f"上传order用时:{te - ts}")

	def _prepare_weight_csv(self):
		weight_df = self.target_df.copy(deep=True)
		weight_df['CCY1'] = weight_df['ccp'].str[:3]
		weight_df['CCY2'] = weight_df['ccp'].str[3:]
		weight_df['Portfolio'] = 'AQM'
		return weight_df[TARGETRATIO_HEADER]

	def _prepare_orders_csv(self):
		imm_fix_date, imm_settle_date = self.get_IMM_date()

		orders_df = self.target_df.copy()
		orders_df = weight_to_holding(orders_df, self.last_price_df, self.portfolio_value)
		orders_df['current_holding'] = self.positions['Amount'].reindex(orders_df['ccp'].values).fillna(0.).values
		orders_df['Amount'] = orders_df['target_holding'] - orders_df['current_holding']

		orders_df['CCY1'] = orders_df['ccp'].str[:3]
		orders_df['CCY2'] = orders_df['ccp'].str[3:]
		orders_df['Portfolio'] = 'AQM'
		orders_df['ccp'] = orders_df['CCY1'] + orders_df['CCY2']
		orders_df['FixingDate'] = np.where(orders_df['ccp'].isin(NDF_CCP), imm_fix_date, 'SPOT')
		orders_df['SettlementDate'] = np.where(orders_df['ccp'].isin(NDF_CCP), imm_settle_date, 'SPOT')

		self.logger.info(f'目标订单：\n{orders_df.to_string()}')

		return orders_df[ORDER_HEADER]

	def get_IMM_date(self):
		return IMM_FIX_DATE, IMM_SETTLE_DATE

	def load_target(self):
		#读取目标仓位
		ts = datetime.now()
		TARGET_PATH = r"\\192.168.9.170\share\alioss\0_DymonFx\target_holding"
		directory_files = os.listdir(TARGET_PATH)

		for f in directory_files:
			if f'backtest_u_0_strategy_id_global_{self.today}' in f:
				# 读取cross pair的目标持仓，转成major pair的目标持仓
				result_df = pd.read_csv(os.path.join(TARGET_PATH,f))

				cross_pair_target = get_holding_from_csv(result_df)

				self.target_df, self.portfolio_value = convert_cross_target_to_major(cross_pair_target, self.T150_price_df)
		te = datetime.now()
		self.logger.info(f"网盘读取目标仓位时间:{te-ts}")

	def verify_executed(self):

		ts = datetime.now()
		sftp = self._connect_sftp()
		sftp.cwd('/Live/')
		directory_files = sftp.listdir()

		#检查executed文件是否存在
		ts_executed = datetime.now()
		while f'Dymon2Aqumon_Executed_{self.today}.csv' not in directory_files:
			time.sleep(1)
			directory_files = sftp.listdir()
			te_executed = datetime.now()
			self.logger.info(f'等待成交订单：已等待{te_executed-ts_executed}')

		for f in directory_files:
			# 需要改成昨天的
			if f'Dymon2Aqumon_Executed_{self.today}' in f:
				remote_filename = f
				local_filename = f

				sftp.get(remote_filename, local_filename)
				self.logger.info(f'FTP下载持仓文件成功，文件名：{f}')
		self._close_sftp(sftp)
		# Check MD5 code
		self.md5_local.verify_md5_file(source_filepath=f'Dymon2Aqumon_Executed_{self.today}.csv')

		orders_df = pd.read_csv(f"Aqumon2Dymon_Order_{self.today}.csv")
		executed_df = pd.read_csv(f"Dymon2Aqumon_Executed_{self.today}.csv")
		if (orders_df == executed_df).all().all():
			self.logger.info(f'目标订单完全成交')
		else:
			self.logger.info(f'目标订单未完全成交')
			self.logger.info(f'目标订单：\n{orders_df.to_string()}')
			self.logger.info(f'成交订单：\n{executed_df.to_string()}')
		te = datetime.now()
		self.logger.info(f"网盘读取目标仓位时间:{te - ts}")

	def put_file_to_oss(self):
		ts = datetime.now()
		path = {
			'dev_algo': "//192.168.9.170/share/alioss/0_DymonFx/Dymon_FTP/",
			'dev_jeff': '../0_DymonFx/Dymon_FTP/',
			'live': '../0_DymonFx/Dymon_FTP/'
		}[ENV]

		df_files = [f"Aqumon2Dymon_Order_{self.today}.csv",\
					f"Aqumon2Dymon_TargetRatio_{self.today}.csv",\
					f"Dymon2Aqumon_Position_{self.yesterday}.csv",\
					f"Dymon2Aqumon_Executed_{self.today}.csv"]
		md5_files = [f"Aqumon2Dymon_Order_{self.today}.md5",\
					f"Aqumon2Dymon_TargetRatio_{self.today}.md5",\
					f"Dymon2Aqumon_Position_{self.yesterday}.md5",\
					f"Dymon2Aqumon_Executed_{self.today}.md5"]

		for file in df_files:
			df = pd.read_csv(file)
			df.to_csv(os.path.join(path, file), index=False)
		for file in md5_files:
			origin_file = open(file, "r")
			md5_data =  ''.join(origin_file.readlines())
			target_file = open(os.path.join(path, file), "w")
			target_file.write(md5_data)
			origin_file.close()
			target_file.close()

		retry_count = 0
		while (True):
			retry_count += 1
			if retry_count > 10:
				dd = DingDingMsg('FX:oss delay [TargetRatio][Order][MD5]')
				dd.send_msg()
			df_existence = [oss_client.exists(os.path.join("0_DymonFx/Dymon_FTP/", file)) for file in df_files]
			md5_existence = [oss_client.exists(os.path.join("0_DymonFx/Dymon_FTP/", file)) for file in md5_files]
			if np.all(df_existence) & np.all(md5_existence):
				break
			time.sleep(1)
			self.logger.info(f'retry {retry_count} times uploading file to oss.')
		te = datetime.now()
		self.logger.info(f"文件放到网盘用时:{te - ts}")

	def run(self):
		ts = datetime.now()
		self.load_T150_data()
		self.load_target()
		self.positions = self.get_holdings()
		self.load_market_data()
		self.send_orders()
		self.verify_executed()
		self.put_file_to_oss()
		te = datetime.now()
		self.logger.info(f"ftp总时间：{te-ts}")

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

	fx = ForexLive(
		start=start_date,
		end=end_date,
		symbols=symbols,
	)
	fx.run()