# RISK Manager 

## Introduction

This module aims at checking the order before sending the order out in case of unexcepted loss due to wrong algo or other reasons.



## Risk config 

An example of risk config which could be add in the config file for live trading.
```{python}
risk_config = {
                 "active":False,
                 "Order_flow_limit": 10,
                 "Order_size_limit":1000,
                 "Trade_limit":1000,
                 "Order_cancel_limit":10,
                 "time_interval":1,
                 "security_weight_limit":np.array([0.1,0.9,1],dtype="float64"),
                 "industry_limit":np.array([0.3, 1], dtype="float64"),
                 "black_list":np.array([0,1,0], dtype="float64")}
```

### Params:
- `active` :               
    - (bool)风控模块的运行状态(处于“运行中”状态时，每笔委托在发出前会执行模块中定义好的多个风控条件检查，全部符合条件才能正常通过，只要有一条不符合该委托都会被拒绝，并且会在日志中输出具体的风控信息;处于“未启动”状态时，则会忽略所有的风控检查，允许委托直接发出。        
- `Order_size_limit`:      
    - (int) 单笔委托上限控制的是每一笔委托允许的最大合约手数，防止由于逻辑或者参数设置错误导致开仓太多合约的风险。
- `Order_flow_limit`:      
    - (int) 流控主要控制的是单位时间内允许发出的最大的委托笔数，例如限制是每1秒允许最多发出4笔委托，如果发送第5笔就会被拒绝，从而防止某些成交触发的交易信号由于逻辑错误被重复触发，在1秒内发出几十笔的情况。
- `Trade_limit`:           
    - (int) 总成交上限控制的是每日允许的总成交的合约数量（不是成交笔数），针对的是无人值守的情况下，策略算法出现逻辑错误开始频繁的买卖（比如每秒开多1手，再平多1手，不断重复），导致在手续费和买卖价差上快速亏损的情况。
- `Order_cancel_limit`:    
    - (int) 每日最大撤单的数量
- `time_interval`：        
    - (int) 单位时间
- `security_weight_limit`：
    - (1D-array) 每一个SYMBOL的VALUE占总PV的比值 应当小于这个LIMIT   e.g.   np.array([0.1,0.9,1]) 
- `industry_limit`:        
    - (1D-array) 每一个INDUSTRY的所有VALUE和占总PV的比值 应当小于这个LIMIT  e.g.  np.array([0.3,1 ]) 所有的SYMBOL只有两个INDUSTY, 用0，1表示， 0 INDUSTRY的WEIGHT LIMIT是0.3, 1 INDUSTRY的WEIGHT LIMIT是 1.
- `black_list`:            
    - (1D-array) 黑名单 对应每个symbol的0/1 array, 1 代表在黑名单内，不可交易；0 代表可以。    e.g  array([0,1,0]) 对应3个symbol是否可交易
 

### Output：

1. order array (Only include volume）
- change after checking the risk threshold. if not pass, the order corrsponding to that symbol change to 0.

2. risk array 
- shape=（M, N)  [0,1 matrix ]  (0 means pass the risk check, 1 means not pass)
    - M: number of risk thresholds
    - N: number of symbols 
    - Currently have 6 risk thresholds : 
        - order_volume = 0
        - traded = 1
        - cancelled = 2
        - flow_count = 3
        - security_weight=4
        - industry_limit=5
        - black_list=6