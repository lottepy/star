#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
import numpy as np
import tqdm
from datetime import datetime
import os


# # V21 daily 测试用例

# In[3]:


# datamaster 准备数据
ROOT_PATH = r"/Users/liuxichen/Desktop/quantcycle/tests/test_case/fx_daily_weight_strategy/v21_daily"
#1. daily data
daily_data_path = os.path.join(ROOT_PATH, "forex_daily_BGNL(OHLC).csv")
daily_data = pd.read_csv(daily_data_path, index_col=0, parse_dates=True)
daily_data.index = daily_data.index.tz_localize(None)
close_columns = [col for col in daily_data.columns if 'close' in col]
daily_close = daily_data[close_columns]
daily_close.columns = [col.replace("close_","") for col in daily_close.columns]
ccp_list = daily_close.columns

#2. daily usd data
daily_usd_path = os.path.join(ROOT_PATH, "forex_daily_in_usd_BGNL.csv")
daily_usd = pd.read_csv(daily_usd_path, index_col=0, parse_dates=True)
daily_usd.index = daily_usd.index.tz_localize(None)

#3. daily rate
daily_rate_path = os.path.join(ROOT_PATH, "FX_ON_RATE.csv")
daily_rate = pd.read_csv(daily_rate_path, index_col=0, parse_dates=True)
last_columns = [col for col in daily_rate.columns if '_LAST' in col]
daily_rate = daily_rate[last_columns]
daily_rate.columns = [col.replace("_LAST","") for col in daily_rate.columns]
daily_rate.index = daily_rate.index.tz_localize(None)

TARGET_PATH = r"/Users/liuxichen/Desktop/quantcycle/tests/test_case/fx_daily_weight_strategy/v21_daily"
commission_file = pd.read_csv(os.path.join(TARGET_PATH, "aqm_turnover_union_fx.csv"), index_col=0)


# In[5]:


# 开始回测
# 回测需要有数据
# daily_close—> dataframe  ccp BGNL close price
# index: 时间
# columns: ccp
# daily_usd—> dataframe  ccy against USD BGNL close price
# index: 时间
# columns: ccy
# daily_rate—> dataframe  ccy implied interest rate
# index: 时间
# columns: ccy
# commission_file --> dataframe commission for different ccp
# index: ccp
# 回测需要指定变量
# ccp: 回测的pair
# ref_aum
# back_start: 回测起始日期
# back_end: 回测终止日期
weight1 = pd.read_csv(r"/Users/liuxichen/Desktop/quantcycle/tests/test_case/fx_daily_weight_strategy/v21_daily/fx_daily_weight_v21.csv", index_col=0,parse_dates=True)

class context:
    def __init__(self, ccp, ref_aum):
        self.ccp = ccp
        self.left_ccp = [ccp[:3] for ccp in self.ccp]
        self.right_ccp = [ccp[3:] for ccp in self.ccp]
        self.n_ccp = len(self.ccp)
        self.ref_aum = ref_aum
        self.commission = commission_file.loc[ccp,'COMMISSION'].values
        self.ccp_holding = np.zeros(self.n_ccp)
        self.ccp_pnl = np.zeros(self.n_ccp)
        self.ccp_cost = np.zeros(self.n_ccp)

context = context(ccp=ccp_list,                  ref_aum=2900000)
                  

datelist = daily_close.index.tolist()
back_start = datetime(2012,1,1)
back_end = datetime(2020,2,28)
back_list = [date for date in datelist if (date>=back_start)&(date<back_end)]
back_start_index = datelist.index(back_list[0])
back_end_index = datelist.index(back_list[-1])


holding_columns = [f"{ccp}_holding" for ccp in context.ccp]
pnl_columns = [f"{ccp}_pnl" for ccp in context.ccp]
cost_columns = [f"{ccp}_cost" for ccp in context.ccp]
result_list = []
weight_list = []
rate_list=[]

weight_df = pd.DataFrame(index=back_list, columns=context.ccp)

delta = (lambda x: 3 if x.weekday()==2 else 1)

for index in range(back_start_index,back_end_index+1):
    
    yesterday = datelist[index-1]
    today = datelist[index]
    
    dt = delta(yesterday)
    
    yesterday_spot = daily_close.loc[yesterday,context.ccp].values
    today_spot = daily_close.loc[today,context.ccp].values
    yesterday_right_usd = daily_usd.loc[yesterday,context.right_ccp].values
    today_right_usd = daily_usd.loc[today,context.right_ccp].values
    today_left_rate = daily_rate.loc[today,context.left_ccp].values
    today_right_rate = daily_rate.loc[today,context.right_ccp].values
        
    pnl1 = context.ccp_holding * (today_spot - yesterday_spot) * today_right_usd
    pnl2 = context.ccp_holding * (today_left_rate-today_right_rate) * (yesterday_spot * yesterday_right_usd) * (dt/36000)
    rate_temp= (today_left_rate-today_right_rate) * (yesterday_spot * yesterday_right_usd) * (dt/36000)

    #if index == back_start_index+1:
    #    break
    
    # 每天生成一串随机的weight 下单
    weight = weight1.loc[today,context.ccp].values
    #weight = weight_yuye.loc[today].values
    #weight = np.random.randint(0,21,context.n_ccp)/(10*context.n_ccp)
    #weight = np.ones(context.n_ccp)/context.n_ccp
    target_ccp_holding = (weight * context.ref_aum)/(today_right_usd*today_spot)
    #target_ccp_holding = weight_yuye.loc[today, context.ccp].values
    # 下单产生cost
    cost = abs(target_ccp_holding-context.ccp_holding)*today_spot*today_right_usd* context.commission
    context.ccp_holding = target_ccp_holding
    context.ccp_cost = cost
    context.ccp_pnl = context.ccp_pnl+pnl1+pnl2-cost
        
    result_list.append(list(context.ccp_holding)+list(context.ccp_pnl)+list(context.ccp_cost)+[sum(context.ccp_cost),sum(context.ccp_pnl)])
    weight_list.append(list(weight))
    rate_list.append(list(rate_temp))

result_df = pd.DataFrame(np.array(result_list), index=back_list, columns=holding_columns + pnl_columns +cost_columns +["cost", "pnl"])
weight_df = pd.DataFrame(np.array(weight_list), index=back_list, columns=context.ccp)
rate_df=pd.DataFrame(np.array(rate_list),index=back_list,columns=context.ccp)

print("to_csv:")
weight_df.to_csv(os.path.join(TARGET_PATH, "results/fx_daily_weight_v21.csv"))
result_df.to_csv(os.path.join(TARGET_PATH, "results/fx_daily_result_v21.csv"))
rate_df.to_csv(os.path.join(TARGET_PATH, "results/fx_daily_rate_v21.csv"))
#result_df.to_csv(r"D:\work\backtesting\test_sample\yuye\fx_daily_result_v21.csv")


# In[ ]:




