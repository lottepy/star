import json
import os
import shutil
import time
import traceback
from datetime import datetime

import pandas as pd
import pip
from tabulate import tabulate
from tornado.web import RequestHandler

from engine_manager import EngineManager
from quantcycle.app.result_exporter.result_reader import ResultReader
from quantcycle.utils import get_logger
from quantcycle.utils.production_helper import MessageBox

engine_manager = None
abspath = os.path.dirname(__file__)
temp_strategy_dir = "quantcycle_tmp"
absolate_temp_strategy_dir = os.path.join(abspath, temp_strategy_dir)

def init():
    if os.path.exists(absolate_temp_strategy_dir):
        shutil.rmtree(absolate_temp_strategy_dir)
    os.makedirs(absolate_temp_strategy_dir)

def start_engine_manager():
    global engine_manager
    engine_manager = EngineManager()
    engine_manager.run()
    if not os.path.exists(absolate_temp_strategy_dir): 
        os.makedirs(absolate_temp_strategy_dir)

def read_json(data):
    try: 
        return json.loads(data)
    except: 
        return {}


class Base_Handler(RequestHandler):
    
    def _get(self,data):
        pass

    def _post(self,data):
        pass

    def get(self):
        """
        Description end-point
        ---
        tags:
        - Example
        summary: Create user
        description: This can only be done by the logged in user.
        operationId: examples.api.api.createUser
        parameters:
        -   name: task_type
            required: false
            type: string
        -   name: timepoint
            required: false
            type: string
        -   name: engine_name
            required: false
            type: string
        -   name: start_timepoint
            required: false
            type: string
        -   name: end_timepoint
            required: false
            type: string
        responses:
            "201":
                description: successful operation
        """
        self.write(self.task_handler(read_json(self.request.body),self._get))

    def post(self):
        """
        Description end-point
        ---
        tags:
        - Example
        summary: Create user
        description: This can only be done by the logged in user.
        operationId: examples.api.api.createUser
        parameters:
        -   name: task_type
            required: false
            type: string
        -   name: timepoint
            required: false
            type: string
        -   name: engine_name
            required: false
            type: string
        -   name: start_timepoint
            required: false
            type: string
        -   name: end_timepoint
            required: false
            type: string
        responses:
            "201":
                description: successful operation
        """
        self.write(self.task_handler(read_json(self.request.body),self._post))

    def task_handler(self,data, f, *args, **kwargs):
        msg = None
        try:
            result = f(data, *args, **kwargs)
            msg = {"status": "success"}
            if result:
                msg = result
        except Exception as e:
            msg = { 
                    "status": "fail",
                    "reason": str(e),
                    "traceback": str(traceback.format_exc())
                }
        get_logger.get_logger().info(f"data:{data} msg:{msg} f:{f.__name__} class:{self.__class__.__name__}")
        return msg 



class Initiation_View(Base_Handler):

    def _get(self,data):
        return "wellcome to prod engine"

class Strategy_View(Base_Handler):

    def _get(self,data):
        list_dir_arr = os.listdir(absolate_temp_strategy_dir)
        return str(list_dir_arr)

    def _post(self,data):
        task_type = data["task_type"]
        if task_type == "delete_all":
            init()



class Subscription_View(Base_Handler):

    def _get(self,data):
        keys = list(engine_manager.id2engine_info.keys())
        res = list(map(lambda x: [x,engine_manager.id2engine_info[x].config_json,engine_manager.id2engine_info[x].strategy_df.to_json(),
                                    engine_manager.id2engine_info[x].is_started,engine_manager.id2engine[x].is_backtest_completed,
                                    engine_manager.id2engine[x].return_waiting_order_list(),
                                    engine_manager.id2engine[x].return_fail_order_list()
                                    ],keys))
        return tabulate(res, tablefmt='html',headers=["engine_name","config_json","strategies","is_started","is_backtest_completed","pending_orders","fail_orders"])
    
    def _post(self,data, *args, **kwargs):
        config_json = json.loads(data["config_json"][0])
        strategy_df = pd.read_json(data["strategy_df"][0])
        is_override = False
        if "is_override" in data:
            is_override = bool(data["is_override"][0])
        install_packages = []
        if "install_packages" in data:
            install_packages = json.loads(data["install_packages"][0])  
        for package in install_packages:
            pip.main(['install', package])



        if 'files' in kwargs:
            files = kwargs['files']
            for file_name, v in files.items():
                file_path = os.path.join(absolate_temp_strategy_dir, file_name)
                if os.path.exists(file_path) and not is_override:
                    raise Exception("already added")
            for file_name, v in files.items():
                file = v[0]['body']
                with open(os.path.join(absolate_temp_strategy_dir, file_name), 'wb') as f:
                    f.write(file)

        # end handle possible files
        global engine_manager
        engine_manager.add_engine(config_json,strategy_df)
        #return {"engine_name":config_json["engine"]["engine_name"]}

    def post(self):
        if 'Content-Type' in self.request.headers.keys() and self.request.headers['Content-Type'].startswith('multipart/form-data'):
            try:
                params = self.request.arguments
            except:
                params = {}
            self.write(self.task_handler(params,self._post,files=self.request.files))
        else:
            try:
                params = self.request.arguments
            except:
                params = {}
            self.write(self.task_handler(params,self._post))

class Engine_manager_View(Base_Handler):

    def _post(self,data):
        MessageBox.send_msg(f"{self.__class__.__name__} data:{data} start")

        task_type = data["task_type"]
        timepoint = data.get("timepoint",None)
        engine_name = data.get("engine_name",None)
        start_timepoint = data.get("start_timepoint",None)
        end_timepoint = data.get("end_timepoint",None)

        global engine_manager
        error_list = {}
        if task_type == "start_engine":
            engine_manager.start_engine()
            load_data_event = engine_manager.load_engine_data()
            load_data_event.wait()
            error_list = engine_manager.load_cash(timepoint="19710101") 
            error_list1 = engine_manager.load_holding(timepoint="19710101",reduce_from_cash = True) 
            #error_list.extend(error_list1)
        elif task_type == "handle_current_data":
            handle_current_data_event , error_list = engine_manager.handle_current_data(timepoint,engine_name = engine_name)
        elif task_type == "reload_current_data":
            error_list = engine_manager.reload_current_data(timepoint,engine_name = engine_name)
        elif task_type == "handle_current_fx_data":
            error_list = engine_manager.handle_current_fx_data(timepoint,engine_name = engine_name)
        elif task_type == "handle_current_rate_data":
            error_list = engine_manager.handle_current_rate_data(timepoint,engine_name = engine_name)
        elif task_type == "handle_ref_data":
            error_list = engine_manager.handle_ref_data(start_timepoint = start_timepoint,end_timepoint = end_timepoint,engine_name = engine_name) 
        elif task_type == "kill_engine":
            engine_manager.kill_engine(engine_name = engine_name)
            #engine_manager.wait_engine()
            if len(engine_manager.id2engine_info.keys()) == 0:
                engine_manager.kill()
                engine_manager.wait()
                start_engine_manager()
                MessageBox.send_msg(f"engine_manager reboot")
                get_logger.get_logger().info(f"engine_manager reboot")
            else:
                get_logger.get_logger().info(f"there are still other engine running engines:{engine_manager.id2engine_info.keys()}")
        elif task_type == "load_holding":
            error_list = engine_manager.load_holding(timepoint,engine_name = engine_name,is_init = False) 
        elif task_type == "load_cash":
            error_list = engine_manager.load_cash(timepoint,engine_name = engine_name) 
        elif task_type == "load_settle":
            error_list = engine_manager.load_settle(timepoint,engine_name = engine_name) 
            
        MessageBox.send_msg(f"{self.__class__.__name__} data:{data} end")
        if len(error_list) > 0:
            raise Exception(error_list)

        #return {}

class Result_View(Base_Handler):
    def _get(self,data):
        engine_name = data["engine_name"]
        id_list = data.get("id_list",None)
        fields = data.get("fields",None)
        start_end_time = data.get("start_end_time",None)
        phase = data.get("phase",None)
        df_disable = data.get("df_disable",False)
        if engine_name not in engine_manager.id2engine_info:
            raise Exception(f"engine_name:{engine_name} does exist")
        engine_info = engine_manager.id2engine_info[engine_name]
        result_reader = ResultReader(engine_info.hdf5_path)
        res_dict = result_reader.get_strategy(id_list = id_list, fields = fields, start_end_time=start_end_time, phase=phase, df_disable=df_disable)
        res_dict = dict([ (key,list([ value1.reset_index().to_json() for value1 in value]))  for key,value in res_dict.items()])
        msg = {"status": "success","data":res_dict}
        return msg
