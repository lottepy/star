
import uuid
import pandas as pd
import requests


req_url = "http://172.29.39.140:1234/api/strategy_subscription"


if __name__ == "__main__":
    
    config_path_1 = r'strategy/FX/technical_indicator/oscillator/RSI/config/RSI_lv1.json'
    config_1 = json.load(open(config_path_1))
    request_id = uuid.uuid1().hex
    params = { 'config_json': json.dumps(config_1) , "strategy_df":(pd.read_csv(r"strategy\FX\technical_indicator\oscillator\RSI\strategy_pool\RSI_lv1_strategy_pool.csv")).to_json()}
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, data = params)
    print(res.text) 

    config_path_2 = r'strategy/FX/technical_indicator/oscillator/RSI/config/RSI_lv2.json'
    config_2 = json.load(open(config_path_2))
    request_id = uuid.uuid1().hex
    params = { 'config_json': json.dumps(config_2)  , "strategy_df":(pd.read_csv(r"strategy\FX\technical_indicator\oscillator\RSI\strategy_pool\RSI_lv2_strategy_pool.csv")).to_json()}
    res = requests.post(url=req_url,headers={'x-request-id': request_id}, data = params)
    print(res.text) 

