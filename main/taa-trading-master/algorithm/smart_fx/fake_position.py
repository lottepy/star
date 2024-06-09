from datetime import datetime
import hashlib
import os
import oss2
from utils.md5_api import md5_verification
import pysftp
import time
import json
import requests

auth = oss2.Auth('LTAIdXteU42OrXqy', 'Cggf8jCyJ8KTAKaizDQGJOcFZDGojM')
bucket = oss2.Bucket(auth, 'http://oss-cn-hongkong.aliyuncs.com', 'aqm-algo')

cnopts = pysftp.CnOpts()
cnopts.hostkeys = None
HOST = 'ftp.dymonasia.com'
USER = 'magnumwm'
PSWD = 'D7yu#3iQr!'


def _connect_sftp():
	return pysftp.Connection(host=HOST, username=USER, password=PSWD, cnopts=cnopts)

def _close_sftp(sftp):
	return sftp.close()

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
	today = datetime.today().strftime('%Y%m%d')
	filename = f'Dymon2Aqumon_Position_{today}.csv'
	dummy_file = 'Dymon2Aqumon_Position_20200106.csv'

	# download from oss
	bucket.get_object_to_file(os.path.join('0_DymonFx/Dymon_FTP/',dummy_file), filename)

	# upload to oss
	bucket.put_object_from_file(os.path.join('0_DymonFx/Dymon_FTP/', filename), filename)

	# add MD5 in oss
	md5 = md5_verification()
	md5.create_md5_code_file(source_filepath=filename)

	# download MD5 from oss
	filename_md5 = filename.replace('.csv', '.md5')
	bucket.get_object_to_file(os.path.join('0_DymonFx/Dymon_FTP/',filename_md5), filename_md5)
	bucket.get_object_to_file(os.path.join('0_DymonFx/Dymon_FTP/',filename), filename)

	# upload to FTP
	sftp = _connect_sftp()
	sftp.put(filename_md5, f'Live/{filename_md5}')
	sftp.put(filename, f'Live/{filename}')
	_close_sftp(sftp)
	dd = DingDingMsg(data='FX模拟仓位更新完毕')
	dd.send_msg()



