from datetime import datetime
import numpy as np
from quantcycle.app.risk_manager.constant import Risk_types as Risk , RISK_TYPES_NUM


class RiskManager:
    def __init__(self, N_symbols,dt,ts,industry_info):

        self.N_symbols = N_symbols
        self.active = False

        self.Order_flow_limit = 0
        self.Order_size_limit = 0
        self.Trade_limit = 0
        self.Order_cancel_limit = 0
        self.Active_order_limit = 0
        self.time_interval = 0
        self.start_time = ts
        self.industry_info=industry_info


        self.security_weight_limit=np.zeros(N_symbols)
        self.industry_weight_limit = np.zeros(N_symbols)
        self.order_flow_count=np.zeros(N_symbols)
        self.date = dt
        self.cancelled_order= np.zeros(N_symbols)
        self.finished_order_vol = np.zeros(N_symbols)



    def update_field(self,dt,ts):
        if self.date != dt:
            self.cancelled_order[:] = 0
            self.finished_order_vol[:] = 0
            self.date = dt
        if self.time_interval !=None:
            if ts - self.start_time >= self.time_interval:
                self.order_flow_count[self.order_flow_count!=0] = 0
                self.start_time = ts


    def update_setting(self, active=False, Order_flow_limit=None, Order_size_limit=None, Trade_limit=None, Order_cancel_limit=None,time_interval=None,security_weight_limit=None,industry_limit=None, PMS=None, black_list=None):
        self.active = active
        self.Order_flow_limit = Order_flow_limit
        self.Order_size_limit = Order_size_limit
        self.Trade_limit = Trade_limit
        self.Order_cancel_limit = Order_cancel_limit
        self.time_interval = time_interval
        self.security_weight_limit=security_weight_limit
        self.industry_weight_limit=industry_limit
        if industry_limit!=None:
           self.industry_num = len(industry_limit)
        self.pms=PMS
        # 0,1 array , 1 means blocked
        self.black_list=black_list


    def process_order_event(self,cancel_array):
        # 用于计算每天 CANCELLED ORDER
        
        self.cancelled_order += np.where(cancel_array != 0, 1, 0)

    def process_trade_event(self,finished_array):
        # 已经成交单累加
        
        self.finished_order_vol += abs(finished_array)




    def check_order(self,temp_order_array,price_array):
        order_array = np.copy(temp_order_array)
        risk_array = np.zeros((RISK_TYPES_NUM, len(order_array)))

        #检查风控是否开启
        if not self.active:
            return order_array, risk_array


        #check_order_volume
        if self.Order_size_limit != None:
            risk_array[Risk.order_volume.value] = np.where(abs(order_array) > self.Order_size_limit,1,0)
            order_array[risk_array[Risk.order_volume.value] == 1] = 0



        # Check 已成交VOL
        if self.Trade_limit != None:
            risk_array[Risk.traded.value] = np.where(self.finished_order_vol > self.Trade_limit, 1, 0)
            order_array[risk_array[Risk.traded.value]==1] = 0



        # Check order cancel counts
        if self.Order_cancel_limit != None:
            risk_array[Risk.cancelled.value] = np.where(self.cancelled_order > self.Order_cancel_limit, 1, 0)
            order_array[risk_array[Risk.cancelled.value]==1] = 0


        # Check all active orders ?


        # Check flow count
        if self.Order_flow_limit != None:
            risk_array[Risk.flow_count.value] = np.where(self.order_flow_count > (self.Order_flow_limit -1), 1, 0)
            order_array[risk_array[Risk.flow_count.value] == 1] = 0


        #check 股票WEIGHT上限
        if (self.security_weight_limit != None).all() :
            weight= (order_array+self.pms.current_holding)*price_array/self.pms.pv
            risk_array[Risk.security_weight.value] = np.where(weight > (self.security_weight_limit), 1, 0)

            order_array[risk_array[Risk.security_weight.value] == 1] = 0

        #check 行业股票上限
        if (self.industry_weight_limit != None).all():


            industry_weight = np.array([sum((order_array[self.industry_info==i]+self.pms.current_holding[self.industry_info==i])*price_array[self.industry_info==i]) for i in range(self.industry_num)])

            industry_weight=industry_weight/self.pms.pv
            industry_risk = np.where( industry_weight > self.industry_weight_limit, 1, 0)



            order_array[np.isin(self.industry_info, np.where(industry_risk==1)[0])]=0
            risk_array[Risk.industry_limit.value][np.isin(self.industry_info, np.where(industry_risk==1)[0])] = 1

        #check 黑名单
        if (self.black_list != None).all():
            risk_array[Risk.black_list.value][ np.where(self.black_list == 1)] = 1
            order_array[self.black_list==1]=0



        #flow_count 计数
        self.order_flow_count += np.where(order_array != 0, 1, 0)


        return order_array, risk_array





