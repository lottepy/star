# Metrics Calculator

## 安装方法

依赖回测框架v21

```
pip install git+ssh://git@gitlab.aqumon.com/junxinzhou/metrics_calculator.git --no-cache-dir
```

## 使用方法

metrics_calculator 共有两个可以调用的函数

1. 

```
from metrics_calculator import read_data
output = read_data(path = 'your/lv1_db/dir_name',
                   flatten = True)
# chunk_size 决定了每一个pickle文件中存多少条记录
# processes 决定了用多少个进程读取数据库
# 如果processes设定的比较高, 那么chunk_size就要相应的设定的小一些, 避免内存不够

output[0] # id_list
output[1] # universe_list
output[2] # params_list
output[3] # rtn_list
output[4] # direction_list
output[5] # commission_list
output[6] # timestamp_list
```

2.

```
from metrics_calculator import metrics_calculator
metrics_calculator(path = 'your/lv1_db/dir_name',
                   flatten = True,
                   allocation_freq = 'M',
                   lookback_points_list = [60,252],
                   addition = True,
                   multiplier = 252)
```