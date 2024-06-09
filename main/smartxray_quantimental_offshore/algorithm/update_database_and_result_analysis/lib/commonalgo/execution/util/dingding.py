# -*- coding: utf-8 -*-

import requests


def send_dingding_msg(access_token: str, message: str):

    proxies = {
        'http': 'http://127.0.0.1:8888',
        'https': 'http://127.0.0.1:8888'
    }

    def _send(access_token: str, message: str, proxies={}):
        try:
            requests.post('https://oapi.dingtalk.com/robot/send',
                headers = {'Content-Type': 'application/json'},
                params = {'access_token': access_token},
                proxies=proxies,
                json = {
                    'msgtype': 'text',
                    'text': {'content': message}
                }
            )
        except:
            return False
        return True

    success = _send(access_token, message)
    if not success:
        print('send dingding msg failed, will use proxy and retry')
        success = _send(access_token, message, proxies=proxies)
        if success:
            print('send dingding msg via proxy success')
        else:
            print('send dingding msg via proxy failed')
