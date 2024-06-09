import os

import numpy as np

from quantcycle.app.pms_manager.super_portfolio_manager import \
    SuperPorfolioManager
from quantcycle.app.result_exporter.result_export import ResultExport
from quantcycle.app.signal_generator.super_strategy_manager import \
    SuperStrategyManager
from quantcycle.utils import get_logger
from quantcycle.utils.production_constant import (PMS_RETRY_NO, RISK_RETRY_NO,
                                                  OrderFeedback, PmsStatus,
                                                  Time)
from quantcycle.utils.production_helper import (WindowSecDataGenerator,
                                                WindowDataGenerator, 
                                                init_user_save_dict)
                                                



# control order_router ,super_porfolio_manager ,super_strategy_manager and risk manager
# receive current data,fx data and rate data from master
class Runner:
    def __init__(self,config : dict ,super_porfolio_manager : SuperPorfolioManager,id2risk_manager,
                        super_strategy_manager : SuperStrategyManager):
        self.window_current_data_generator = WindowDataGenerator(config["algo"]['window_size']["main"])
        self.window_current_time_generator = WindowDataGenerator(config["algo"]['window_size']["main"])
        self.window_sec_data_generator = WindowSecDataGenerator(config["algo"]['window_size'])

        self.super_porfolio_manager = super_porfolio_manager
        self.super_strategy_manager = super_strategy_manager
        self.id2risk_manager = id2risk_manager
        self.config = config
        self.flatten_setting= self.config["result_output"].get("flatten",False)
        self.date = None
        self.ts = None
        self.ready_save={}
        self.ready_save["order_feedback"]=0
        self.result_export = ResultExport(self.config["result_output"]["save_dir"])
        self.result_dir=self.config["result_output"]["save_dir"]
        self.save_name=self.config["result_output"]["save_name"]
        self.save=False
        
        # init user_save_dict
        self.remark_save_dir = config.get('signal_remark', {}).get('save_dir', None)
        self.remark_save_name = config.get('signal_remark', {}).get('save_name', None)
        self.user_save_dict = init_user_save_dict(self.super_strategy_manager, self.remark_save_dir, self.remark_save_name)

    def save_status(self):
        status_path = self.config.get("status_path",None)
        if status_path is not None and os.path.exists(status_path):
            status_path = None
        if status_path is not None:
            self.super_strategy_manager.save_status(status_path)

    def update_time(self,date,ts):
        # read time from master and update fields 
        if self.date!= date:
            self.super_porfolio_manager.reset_field_rollover_day()
        for id,risk in self.id2risk_manager.items():
            risk.update_field(date,ts)
        self.date = date
        self.ts = ts

    def reset_trade_status(self, tradable_status):
        self.super_porfolio_manager.reset_trade_status(tradable_status)

    def update_spot(self,current_data,current_fx_data):
        temp_current_data=current_data.copy()
        temp_current_fx_data = current_fx_data.copy()
        #temp_current_data[np.isnan(temp_current_data)] = 0
        #temp_current_fx_data[np.isnan(temp_current_fx_data)] = 1
        self.super_porfolio_manager.calculate_spot(temp_current_data,temp_current_fx_data)

    def update_dividends(self,current_data,current_fx_data,current_rate_data):
        temp_current_data = current_data.copy()
        temp_current_fx_data = current_fx_data.copy()
        temp_current_rate_data = current_rate_data.copy()
        #temp_current_data[np.isnan(temp_current_data)] = 0
        #temp_current_fx_data[np.isnan(temp_current_fx_data)] = 1
        #temp_current_rate_data[np.isnan(temp_current_rate_data)] = 0
        self.super_porfolio_manager.calculate_rate(temp_current_rate_data,temp_current_data,temp_current_fx_data)
    
    def sync_holding(self,id,holding):
        self.super_porfolio_manager.sync_holding(id,holding)
    
    def sync_current_data(self,id,current_data):
        self.super_porfolio_manager.sync_current_data(id,current_data)

    def sync_cash(self,id,cash):
        cash_residual = self.super_porfolio_manager.sync_cash(id,cash)
        return cash_residual


    #def handle_current_data(self,current_data,current_fx_data,ts,secondary_data,window_ref_data_dict,window_ref_time_dict,need_secondary_data,order_router_ready):
    def handle_current_data(self, window_current_data, window_time_array, current_fx_data, window_fx_data,
                            window_fx_time, window_rate_data, window_rate_time, window_tradable_data,
                            window_tradable_time, secondary_data, window_ref_data_dict,
                            window_ref_time_dict, need_secondary_data, order_router_ready, info_dict,order_feedback = None):
        #
        current_data = window_current_data[-1]
        ts = window_time_array[-1]
        #current_fx_data = window_fx_data[-1]
        id_order = {}
        window_secondary_data = None

        if window_current_data is None: #TODO：与之前production_helper if 判断重复
            get_logger.get_logger().info(f"runner can't handle_current_data with window_current_data:{window_current_data} dt:{ts[Time.YEAR.value:Time.HOUR.value]}")
            return id_order,[]
        if not secondary_data and need_secondary_data ==True:
            get_logger.get_logger().info(f"runner can't handle_current_data with secondary_data:{secondary_data} need_secondary_data:{need_secondary_data} dt:{ts[Time.YEAR.value:Time.HOUR.value]}")
            return id_order,[]
        window_secondary_data = self.window_sec_data_generator.receive_secondary_data(secondary_data) if secondary_data else {}



        if current_fx_data is not None:
            self.super_strategy_manager.save_current_data(current_fx_data,current_data[:,3],ts[Time.TIMESTAMP.value] ,np.array([True for i in range(len(current_data))]) )
        if order_router_ready:
            id2order,backup_symbols_dict = self.super_strategy_manager.on_data(window_current_data, window_time_array, window_fx_data, window_fx_time,
                                                                               window_rate_data, window_rate_time, window_tradable_data, window_tradable_time,
                                                                               window_ref_data_dict, window_ref_time_dict, window_secondary_data, info_dict,
                                                                               order_feedback=order_feedback)
            
            for id,order in id2order.items():
                # check order with risk manager, retry RISK_RETRY_NO times, fire zeros array order to router if all failed 
                get_logger.get_logger().info(f"id:{id} order:{order}")
                risk_checked_order = np.copy(order)
                for i in range(RISK_RETRY_NO):
                    risk_checked_order, risk_array = self.id2risk_manager[id].check_order(risk_checked_order,current_data[:,3]*current_fx_data)
                    if not (np.sum(risk_array,axis=1) == 0).all():
                        risk_checked_order = self.super_strategy_manager.on_risk_feedback(id,risk_checked_order,risk_array)
                    else:
                        break

                # check order with pms manager, retry PMS_RETRY_NO times, fire zeros array order to router if all failed  
                get_logger.get_logger().info(f"id:{id} risk_checked_order:{risk_checked_order}")
                pms_checked_order = np.copy(risk_checked_order)
                for i in range(PMS_RETRY_NO):
                    pms_checked_order,pms_status = self.super_porfolio_manager.check_order(id,risk_checked_order)
                    if not (pms_status==PmsStatus.LEGAL.value).all():
                        self.super_strategy_manager.on_pms_feedback(id,pms_checked_order,pms_status)
                    else:
                        break
                get_logger.get_logger().info(f"id:{id} pms_checked_order:{pms_checked_order}")
                id_order[id] = pms_checked_order
        id_order_dict = dict([(id,dict(zip(self.super_strategy_manager.id2universe[id], order))) for id,order in id_order.items()])        
        #return id_order
        return id_order_dict,backup_symbols_dict

    
    def handle_order_feedback(self,id,order_feedback):
        get_logger.get_logger().info(f"order_feedback: id:{id} {str({'transaction':list(order_feedback[OrderFeedback.transaction.value]),'order_status':list(order_feedback[OrderFeedback.order_status.value]) })}")
        temp_order_feedback=order_feedback.copy()
        #temp_order_feedback[np.isnan(temp_order_feedback)]=0
        self.super_porfolio_manager.receive_order_feedback(id,temp_order_feedback)


    def capture(self,ts,task_type):
        self.super_porfolio_manager.capture(ts)
        if task_type=="order_feedback":
            self.ready_save[task_type] += 1
            if self.ready_save[task_type] == len(self.super_strategy_manager.strategies):
                self.ready_save[task_type] = 0
                self.save_real_result(ts, task_type)
        else:
            self.save_real_result(ts, task_type)
        # self.save_real_result(ts, task_type)

    def end_task(self):
        pass



    def save_real_result(self,ts,task_type):

        if not self.save:
            # 存储lv1结果
            # 如果结果存储路径不存在，创建路径
            if not os.path.exists(self.result_dir):
                print("warning: 结果存储路径不存在，创建路径")
                os.makedirs(self.result_dir)
            # 如果目标文件存在，就先删除, 相当于新结果覆盖原结果
            if os.path.exists(os.path.join(self.result_dir, self.save_name.split(".")[0] + ".hdf5")):
                print("warning: 结果文件已经存在，新结果将覆盖原结果")
                os.remove(os.path.join(self.result_dir,self.save_name.split(".")[0] + ".hdf5"))
            if os.path.exists(os.path.join(self.result_dir, self.save_name.split(".")[0] + ".keys")):
                print("warning: 结果文件已经存在KEY，新结果将覆盖原结果")
                os.remove(os.path.join(self.result_dir, self.save_name.split(".")[0] + ".keys"))

        self.save = True
        self.result_export.save_realtime_data(self.save_name,
                                              *[(self.super_porfolio_manager.id2pms[id],
                                                 list(self.super_strategy_manager.id2universe[id]),
                                                 self.super_strategy_manager.strategies[
                                                     id].strategy_param, ts) for id in
                                                range(len(self.super_strategy_manager.strategies))], flatten= self.flatten_setting,
                                              phase=task_type, reset_id=0)
        # self.result_export.save_realtime_data('#close#')

    def update_signal_remark(self, strategy_id):
        strategy = self.super_strategy_manager.strategies[strategy_id]
        if len(strategy.signal_remark) != 0:
            self.user_save_dict[strategy_id].update_signal_remark(strategy.signal_remark)
            strategy.clear_signal_remark()