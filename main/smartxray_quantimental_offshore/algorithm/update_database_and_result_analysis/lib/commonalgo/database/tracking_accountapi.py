import hashlib
import time
import requests
from ..setting.network_utils import MARKET_EP



# BASE_API_URI = 'http://localhost:8000/v2/account/'  # 本地测试
# BASE_API_URI = 'https://market.aqumon.com/v2/account/' # 上线链接
#
# ACCESS_TOKEN = 'M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ'  # 弘量公司 access token

_session = requests.session()
ACCOUNT_EP = f'{MARKET_EP}/v2/account/'

class TrackingAccountAPI(object):
	def __init__(self, endpoint = ACCOUNT_EP):
		if not endpoint:
			self.endpoint = ACCOUNT_EP
		else:
			self.endpoint = endpoint

	def create_init_position(self, data_dict):
		endpoint = self.endpoint + 'algo_model'
		params = {
			'algo_type_id': data_dict.get('algo_type_id'),
			'risk_ratio': data_dict.get('risk_ratio'),
			'region': data_dict.get('region'),
			'sector': data_dict.get('sector'),
			# 'version': '1',
			"access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ"
		}
		result = _session.get(url=endpoint, params=params).json()

		if not result.get('status').get("ecode"):
			return result.get('data')
		else:
			return result


def update_transactions(self, data_dict):
    endpoint = self.endpoint + 'algo_model'
    params = {
        'algo_type_id': data_dict.get('algo_type_id'),
        'risk_ratio': data_dict.get('risk_ratio'),
        'region': data_dict.get('region'),
        'sector': data_dict.get('sector'),
        # 'version': '1',
        "access_token": "M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ"
    }
    result = _session.get(url=endpoint, params=params).json()

    if not result.get('status').get("ecode"):
        return result.get('data')
    else:
        return result

# account_id = 65  # name: Test_Account # not a real account, only for test
#
# _session = requests.session()
#
#
# # test create_init_position
# # list of dict
# init_position = [
#     {"date": "2018-11-01", "iuid": "CN_00_CNH", "quantity": 13186},
#     ]
# payload = {"init_position": init_position}
#
# resp = _session.post(BASE_API_URI + str(account_id) + '/create_init_position?access_token=M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ', json=payload)
#
# print(resp.json())
#
# # test update_trades
# # 可能产生重复的 trades（如果在上传过程中发生错误），那么需要算法组 step1. 在管理页面，删除错误的account history, step2. 在管理页面，rollback and delete txns, step3. 在管理页面，sync_trades
# # list of dict
# trades = [
#     {"date": " 2018-11-01", "iuid": "CN_10_000166", "action": "buy", "price": 11.06, "quantity": 8000, "fee": 0},
#     ]
# payload = {"trades": trades}
# resp = _session.post(BASE_API_URI + str(account_id) + '/update_trades?access_token=M3za0PTfaNZg59FVwg0jVJo2uwirzMwZ', json=payload)
#
# print(resp.json())
