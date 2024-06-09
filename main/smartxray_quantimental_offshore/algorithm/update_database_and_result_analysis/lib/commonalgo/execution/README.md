# 新·Executor 简介

给定broker id, account id, [subaccount id] 及 目标weights/positions，以一定的下单策略执行下单操作，使持仓与目标一致。可以控制一些下单的策略，如是否使用孖展账户(如何计算position、先卖后买还是直接全部下单)、是否启用串流数据等。

- [新·Executor 简介](#%e6%96%b0executor-%e7%ae%80%e4%bb%8b)
  - [运行环境](#%e8%bf%90%e8%a1%8c%e7%8e%af%e5%a2%83)
  - [输入](#%e8%be%93%e5%85%a5)
    - [1. [date].csv （每天生成一个csv输入）](#1-datecsv-%e6%af%8f%e5%a4%a9%e7%94%9f%e6%88%90%e4%b8%80%e4%b8%aacsv%e8%be%93%e5%85%a5)
    - [2. config.ini](#2-configini)
  - [输出](#%e8%be%93%e5%87%ba)
    - [1. executor[date].log](#1-executordatelog)
    - [2. after_trading_positions.csv](#2-aftertradingpositionscsv)
  - [部署](#%e9%83%a8%e7%bd%b2)
  - [宽睿部署（特殊情况）](#%e5%ae%bd%e7%9d%bf%e9%83%a8%e7%bd%b2%e7%89%b9%e6%ae%8a%e6%83%85%e5%86%b5)
  - [具体逻辑](#%e5%85%b7%e4%bd%93%e9%80%bb%e8%be%91)
  - [适配情况](#%e9%80%82%e9%85%8d%e6%83%85%e5%86%b5)
  - [config.ini配置指南](#configini%e9%85%8d%e7%bd%ae%e6%8c%87%e5%8d%97)
    - [CTP](#ctp)
    - [宽睿](#%e5%ae%bd%e7%9d%bf)
    - [IB HC](#ib-hc)
    - [IB HK](#ib-hk)
    - [IB US](#ib-us)
    - [IB SG (期货)](#ib-sg-%e6%9c%9f%e8%b4%a7)

## 运行环境

`pip3 install pandas requests oss2 pika==0.12`

## 输入

支持从本地或远端读取输入文件，无论是本地还是远程，程序都会把当前使用到的文件的路径输出到log中，这样不会不知道文件在哪里。

### 1. [date].csv （每天生成一个csv输入）

可以通过`iuid/symbol`指定标的，其中iuid为高优先级，暂未完成symbol转iuid的实现（还没有具体的需求）。
通过`target_weight/target_position`指定操作的权重/数量(多少手)，其中target_position为高优先级。若target_position为空，将根据：1. 当前的账户总资产；2. 增加的资金（如CN_00_CNY的target_position，不需要则可省略这一行）；3. 当前的标的价格 这3点计算对应的target_position。

Example:

| iuid         | symbol | target_weight | target_position |
| ------------ | ------ | ------------- | --------------- |
| CN_60_rb1910 |        | 0.13423       |                 |
| CN_60_au1912 |        | -0.05747      |                 |

对应csv：

```csv
iuid,symbol,target_weight,target_position
CN_60_rb1910,,0.13423,
CN_60_au1912,,-0.05747,
```

### 2. config.ini

保存`broker, account, region, endpoint`等信息。其中，

- `region`可选`CN/US/HK/HC(港股通)`，并且将会决定对应的当前持仓、货币种类。
- `is_subaccount`为是否子账户，会影响部分API的URL。
- `margin_enabled`为是否属于孖展账户，会影响下单的方式（孖展账户会一次过全部下单）。
- `rq_stream_enabled`为是否启用RQ Client的订阅功能，以替代主动拉取实时数据。
- `kuanrui_enabled`为是否在宽睿的服务器中运行，若是，则会从本地经socket获取行情数据。
- `asset_limit`为限制使用的最大资金量，货币单位受region控制，若为-1，则无限制。
- `confirm`为是否在下单前手动确认。
- `dingding_token`为钉钉机器人的token，可以推送一些交易信息。
- `no_of_split`为调仓时拆单的数量，在全部换仓的情况下，大约会分别买卖*no_of_split*次。
- kuanrui的socket client有3个配置项：`HOST`、`PORT_OES`、`PORT_MDS`

Example:

```ini
[info]
broker = 102
account = 123456789
region = HK

[network]
endpoint = http://192.168.8.79:5565/api/v3/
; endpoint reference https://confluence-algo.aqumon.com/pages/viewpage.action?pageId=5506071

[order]
margin = false
rq_stream = true
kuanrui = false
asset_limit = -1
confirm = true
```

## 输出

### 1. executor[date].log

所有log都会输出到这个文件中，为避免log太大，每天一个log文件。
log中包含broker、account、endpoint、choice返回的snapshot数据、target position、current position、当前资产、发送的orders及其callback信息等。

### 2. after_trading_positions.csv

是trading executor的get_current_positions()的输出。

## 部署

运行`python3 order.py`即可。

## 宽睿部署（特殊情况）

宽睿部署在封闭网络的服务器中，只能通过本机的特定接口获取行情，运行之前需要修改一些代码与参数：

1. order.py : 打开 is_local、data_dir='', prefix=''
2. lib/commonalgo/execution/config/data/config.ini : 修改配置，包括kuanrui服务器IP地址，这个文件应该有备份
3. lib/commonalgo/execution/config/data/target.csv : 修改权重
4. lib/commonalgo/setting/network_utils.py : 关闭对不同端口的测试 并手动乱写choice、bbg相关参数为任意字符串

## 具体逻辑

1. 判断从本地还是远程获取配置信息（是否有传入 --local 参数，默认从远程服务器获取），确定config.ini、目标权重/持仓的csv（默认：本地的命名为target.csv，远程的以当天的日期命名）的**绝对**路径。
2. 读取目标csv并处理。处理包括：a) 获取所有品种的当前价格；b) 获取用于增加/保留仓位的add_value（保留/冻结）值；c) 若IUID为空，由symbol转IUID（未实现）；d) 若target_position为空，由weight转position。
3. 关于如何由weight转position：
   1. `资产 = 实际资产 + add_value`  // add_value>0时，可增加杠杆；add_value<0时，可降低仓位
   2. `position = round(weight * 资产 / (标的的当前价格 * 标的一手有多少股))`  // 由于是期货 简单取整即可 不考虑向上/向下取整
   3. 如果是股票，则有更复杂的转换方式，见`lib/commonalgo/execution/util/target_positions.py`
4. 初始化executor，查当前持仓。
5. 取消当前的所有订单。
6. 用目标持仓减去当前持仓，得变化量。对于做多转做空，或做空转做多，需要先平仓，再开仓，即有2步的变化量。
7. 用变化量生成订单，给executor执行订单。执行时，会不断接收最新的订单状态，判断是否已全部成交。若超过一定时间没有成交，则会先撤单，再以最新的价格提交订单。

## 适配情况

| 类型    | 地区          | 模拟                        | 实盘                        |
| ------- | ------------- | --------------------------- | --------------------------- |
| CTP期货 | CN            | ✅<br>从choice获取行情       | ✅<br>从choice获取行情       |
| 宽睿    | CN            | ✅<br>从宽睿封闭环境获取行情 | ✅<br>从宽睿封闭环境获取行情 |
| IB      | HC(沪/深港通) | ✅<br>从RQ Client获取行情    | ✅<br>从RQ Client获取行情    |
|         | HK            | ✅<br>从RQ Client获取行情    | ✅<br>从RQ Client获取行情    |
|         | US            | ✅<br>从RQ Client获取行情    | ✅<br>从RQ Client获取行情    |
|         | SG(期货)      | ✅<br>从RQ Client获取行情    | ✅<br>从RQ Client获取行情    |

## config.ini配置指南

### CTP

```ini
region = CN
margin = true
rq_stream = false
kuanrui = false
```

### 宽睿

```ini
region = CN
margin = false
rq_stream = false
kuanrui = true
```

### IB HC

```ini
region = HC
margin = false
rq_stream = true
kuanrui = false
```

### IB HK

```ini
region = HK
margin = false
rq_stream = true
kuanrui = false
```

### IB US

```ini
region = US
margin = false
rq_stream = true
kuanrui = false
```

### IB SG (期货)

```ini
region = SG
margin = true
rq_stream = true
kuanrui = false
```
