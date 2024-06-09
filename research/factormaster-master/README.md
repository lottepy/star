# FactorMaster

## Universe(stockpools)

### ASHARE
全部A股上市股票, 包含沪深主板, 中小板, 创业板, 科创板.

剔除:
- ST及风险警示股
- 次新股(IPO后63个交易日内)

### ASHARE-TRADE
全部 ASHARE 股票

剔除:
- 一字涨跌停股票
- 停牌股票

### STOCKCONNECT
全部 ASHARE 股票

剔除:
- 不可在A股通交易的股票

### CSI300
沪深300成分股

### CSI300-TRADE
沪深300成分股

剔除:
- 一字涨跌停股票
- 停牌股票

### CSI500
中证500成分股

### CSI300-TRADE
中证500成分股

剔除:
- 一字涨跌停股票
- 停牌股票

### CSI800
中证800成分股

### CSI800-TRADE
中证800成分股

剔除:
- 一字涨跌停股票
- 停牌股票

### AQUMON1000
全部 ASHARE 中股票选择总市值最大的1000只股票(不考虑一字涨跌停/停牌情况)

### AQUMON1000-TRADE
全部 AQUMON1000 股票中

剔除:
- 一字涨跌停股票
- 停牌股票

## Payoffs

### Pricing factors

#### CAPM
- Classic CAPM
    - 使用 ASHARE TRADE Universe
    - Rebalance annually
- Simplified CAPM
    - 使用 ASHARE TRADE Universe
    - Rebalance monthly
- Weekly CAPM
    - 使用 ASHARE TRADE Universe
    - Rebalance weekly

#### Fama-French 3
- Classic FF3
    - 使用 ASHARE TRADE Universe
    - Rebalance annually
- Simplified FF3
    - 使用 ASHARE TRADE Universe
    - Rebalance monthly
- Weekly FF3
    - 使用 ASHARE TRADE Universe
    - Rebalance weekly

#### Carhart 4
- Classic Carhart4
    - 使用 ASHARE TRADE Universe
    - Rebalance annually
- Simplified Carhart4
    - 使用 ASHARE TRADE Universe
    - Rebalance monthly
- Weekly Carhart4
    - 使用 ASHARE TRADE Universe
    - Rebalance weekly

#### Fama-French 5
- Classic FF5
    - 使用 ASHARE TRADE Universe
    - Rebalance annually
- Simplified FF5
    - 使用 ASHARE TRADE Universe
    - Rebalance monthly
- Weekly FF5
    - 使用 ASHARE TRADE Universe
    - Rebalance weekly

### Anomaly
refer to [Anomaly.md](factormaster/payoffs/anomaly/anomaly.md)

## Indices

### AQUMON1000 指数
编制方法:

AQUMON 1000 指数对标罗素 1000 指数, 选择 A 股市值最大的 1000 支股票, 代表了全市场
90% 左右的市值, 用来反映 A 股市场总体走势. 
- 样本空间: 每个再平衡日, 所有在市 A 股剔除 ST, 新股, 待退市股和净资产为负股票(即ASHARE Stockpool);
- 选样方法: 样本空间中选择总市值最大的 1000 支股票;
- 加权方式: 自由流通市值加权(目前选择总市值加权)
- 指数计算: 
  1. 1000 支股票中, 只有非停牌和非一字涨跌停的股票才会参与指数计算(即AQUMON1000-TRADE Stockpool);
  2. 权重由自由流通市值确定(目前选择总市值加权);
  3. 每月最后一个交易日为调仓日;
  4. 基期为 2010/12/31, 基点为 1000 点;
  5. 如果样本空间不足 1000 支股票, 则选择所有股.

### AQUMON1000 因子指数
编制方法:

AQUMON1000 因子指数则是在 AQUMON1000 指数的基础上, 利用因子指标进一步筛选, 得
到具有因子偏向的组合. 借鉴主流数据商的因子体系, AQUMON1000 因子指数包含 7 大类: 价
值(value), 盈利(Profitability), 成长(Growth), 红利(Yield), 低波动(Low-Vol), 
规模(Size)和动量(Momentum), 每类因子的含义和相应的变量见下表. 

| 因子 | 变量 | 含义|
| --- | --- | --- |
| 价值 | BM, EP, EBIT, GP| 估值越低, 预期收益越高|
| 盈利 | ROA, ROE | 盈利越高, 预期收益越高 |
| 成长 | ROA 同比变化, ROE 同比变化, 净利润同比增长率, 营业收入同比增长率 | 增长指标越大, 预期收益越高 |
| 红利 | DP | 股息越高, 预期收益越高 |
| 低波 | 特质波动率 | 风险越小, 预期收益越高 |
| 规模 | 总市值 | 市值越小, 预期收益越高 |
| 动量 | 过去12个月收益率(剔除最近1个月) | 动量越强, 预期收益越高 |
注: 红利还没有implement.