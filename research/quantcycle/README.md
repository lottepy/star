# QuantCycle V2.1.2

A quantitative platform for managing whole cycle from strategy backtest and development to production run.
 - Strategy Backtest
 - Production Run

## Environment

- Windows, MacOS, Linux, Python 3.7 x64

## 已支持

- FX, stocks, futures
- Daily, hourly, minutely
- Datamaster, local CSV


## Strategy Backtest

### 目录结构

  ```bash
  your folder
    ├── backtest_lv1_start.py
    │
    ├── venv
    │
    ├── .numba_config.yaml
    │
    └── strategy: gitlab package
        ├── ......
  ```

### How to use

 1. Create a folder(your folder)
 
 2. Clone strategy from gitlab
 
      ```bash
      cd your folder
      git clone https://gitlab.aqumon.com/pub/strategy.git
      ```

 3. Site-package install quantcycle
 
      ```bash
      virtualenv venv
      # after activating venv
      venv\Script\activate
      python -m pip install --upgrade pip
      pip install git+https://gitlab.aqumon.com/algo/quantcycle.git --no-cache-dir
      ``` 
 4. Copy .numba_config.yaml file from strategy folder to your folder
      ```bash
      your folder
        ├── backtest_lv1_start.py
        │
        ├── venv
        │
        ├── .numba_config.yaml
        │
        └── strategy: gitlab package
            ├── .numba_config.yaml
      ```
 5. Write your backtest start py
 
      ```bash
      from quantcycle.engine.backtest_engine import BacktestEngine
      from datetime import datetime
      import pandas as pd
      import json

      if __name__ == "__main__":
        json_path = r'strategy\FX\technical_indicator\oscillator\KD\config\KD_lv1.json'
        strategy_pool_df = pd.read_csv(r'strategy\FX\technical_indicator\oscillator\KD\strategy_pool\KD_lv1_strategy_pool.csv')
        json_dict = json.load(open(json_path))
        backtest_engine = BacktestEngine()
        backtest_engine.load_config(json_dict, strategy_pool_df)
        backtest_engine.prepare()
        backtest_engine.start_backtest()
        backtest_engine.export_results()
      ``` 
 6. Push your strategy and config to gitlab:strategy 

## Production Run

### 目录结构
  ```bash
  your folder
    ├── fire_request_main.py
    │
    └── strategy: gitlab package
        ├── ......
  ```

### How to use

 0. Url
     ```bash
      base_url = "https://172.29.39.140:1234/"(dev)
      base_url = "https://172.29.39.140:1235/"(dev)
      base_url = "https://172.29.37.30:1234/" (uat)
      base_url = "https://172.29.38.123:1234/"(production)
      ```
     
 1. Create a folder(your folder)
 
 2. Clone strategy from gitlab
 
      ```bash
      git clone https://gitlab.aqumon.com/pub/strategy.git
      ```
    
 3. Fire strategy and config to production engine
 
      ```bash
      import uuid
      import pandas as pd
      import requests
      import json
      
      if __name__ == "__main__":
        req_url = base_url + "strategy_subscription"

        config_json_rsi_level1 = json.load(open(r'strategy/FX/technical_indicator/oscillator/RSI/config/RSI_lv1.json'))
        request_id = uuid.uuid1().hex
        params = { 'config_json': json.dumps(config_json_rsi_level1) , "strategy_df":(pd.read_csv(r"strategy\FX\technical_indicator\oscillator\RSI\strategy_pool\RSI_lv1_strategy_pool.csv")).to_json()}
        res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
        print(res.text) 

        request_id = uuid.uuid1().hex
        config_json_rsi_level2 = json.load(open(r'strategy/FX/technical_indicator/oscillator/RSI/config/RSI_lv2.json'))
        params = { 'config_json': json.dumps(config_json_rsi_level2) , "strategy_df":(pd.read_csv(r"strategy\FX\technical_indicator\oscillator\RSI\strategy_pool\RSI_lv2_strategy_pool.csv")).to_json()}
        res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
        print(res.text) 

        #with no strategy storage
        request_id = uuid.uuid1().hex
        df = pd.read_csv(r"strategy\STOCKS\technical_indicator\oscillator\strategy_pool\EW_lv1_strategy_pool.csv")
        df["strategy_module"][0] = "quantcycle_tmp.EW_lv1"
        STOCK_config = json.load(open(r'strategy\STOCKS\technical_indicator\oscillator\config\EW_stock_lv1_oms.json'))
        params = { 'config_json': json.dumps(STOCK_config)   , "strategy_df":df.to_json(),"is_override":True }
        f = open('strategy/STOCKS/technical_indicator/oscillator/algorithm/EW_lv1.py', 'rb')
        res = requests.post(url=req_url,
                         headers={'x-request-id': request_id},
                         data = params,
                         files = {'EW_lv1.py': f, # file name same as the one in strategy_pool
                                                  # no need to care about the dir path
                                   }) 
        f.close()
        print(res.text) 
      ```
    
 4. Start startegy in production engine 
 
      ```bash
      import uuid
      import pandas as pd
      import requests
    
      if __name__ == "__main__":
        req_url = base_url + "engine_manager_control"
        request_id = uuid.uuid1().hex
        params = { "task_type":"start_engine"}
        res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
        print(res.text) 
      ```
    
 5. Set up task for diff data process
 
      ```bash
      import uuid
      import pandas as pd
      import requests
    
      if __name__ == "__main__":
        req_url = base_url + "engine_manager_control"
        request_id = uuid.uuid1().hex
        dt = '20121231'
        engine_name = "lv3" # can be None
        params = { "task_type":"handle_current_data","timepoint":dt,"engine_name":engine_name}
        res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
        print(res.text) 
    
        req_url = base_url + "engine_manager_control"
        request_id = uuid.uuid1().hex
        dt = '20121231'
        engine_name = "lv3" # can be None
        params = { "task_type":"handle_current_fx_data","timepoint":dt,"engine_name":engine_name}
        res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
        print(res.text) 
    
        req_url = base_url + "engine_manager_control"
        request_id = uuid.uuid1().hex
        dt = '20121231'
        engine_name = "lv3" # can be None
        params = { "task_type":"handle_current_rate_data","timepoint":dt,"engine_name":engine_name}
        res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
        print(res.text) 
      ``` 
    
 6. Remove strategy
 
      ```bash
      import uuid
      import pandas as pd
      import requests
    
      if __name__ == "__main__":
        req_url = base_url + "engine_manager_control"
        request_id = uuid.uuid1().hex
        engine_name = "lv3" # can be None
        params = { "task_type":"kill_engine","engine_name":engine_name}
        res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
        print(res.text) 
      ```  

 7. Check result
 
      ```bash
      import json
      import uuid
      import pandas as pd
      import requests

      if __name__ == "__main__":
          request_id = uuid.uuid1().hex
          req_url = base_url + "result"
          engine_name = "lv3" # can be None
          phase = "order_feedback" # can be None [order_feedback,sync_holding]
          params = { "engine_name":"lv3","id_list":[0],"fields":["pnl","position"],"engine_name":engine_name,"phase":phase}
          res = requests.get(url=req_url,headers={'x-request-id': request_id}, json = params)
          print(res.text) 
          df = pd.read_json(json.loads(res.text)['data']['0'][0])
          print(df) 
      ```  
 8. reload current data
     ```bash
      import json
      import uuid
      import pandas as pd
      import requests

      if __name__ == "__main__":
          req_url = base_url + "engine_manager_control"
          request_id = uuid.uuid1().hex
          dt = '20121231'
          engine_name = "lv3"
          params = { "task_type":"reload_current_data","timepoint":dt,"engine_name":engine_name}
          res = requests.post(url=req_url,headers={'x-request-id': request_id}, json = params)
          print(res.text) 
      ```  

### How to view strategy information in production engine 

 - http://172.29.39.140:1234/api/strategy_subscription (dev)
 - http://172.29.39.140:1235/api/strategy_subscription (dev)
 - http://172.29.37.30:1234/api/strategy_subscription  (uat)
 - http://172.29.38.123:1234/api/strategy_subscription (production)


## More

Refer to the *[manual for strategy developers](docs/manual_v2.md)* for more details.

