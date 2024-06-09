import requests
import time
import json


if __name__ == '__main__':
	update_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
	data = {
		'msgtype': 'text',
		'text': {
			'content': f'{[update_time]} FX回测数据更新完成，并已通过回测检验'
		}
	}
	data_to_post = json.dumps(data).encode('utf-8')

	# url = 'https://oapi.dingtalk.com/robot/send?access_token=1f940ef402891ada76d0c6ac1aad8f9d5bcbf77f517b038600845b4404a7bd52'
	url = 'https://oapi.dingtalk.com/robot/send?access_token=673b8f53d8c5e8e4800528a6add7bf745f6452feda7edbb81a4a57078af57003'
	result = requests.post(
		url,
		headers={"content-type": "application/json"},
		data=data_to_post
	)
	print(result.text)