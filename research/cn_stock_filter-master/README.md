# cn_stock_filter

This project aims to provide various types of filters for cn stocks in different frequencies.

## Scripts

1. download non-standard data
2. generate daily filter
3. combine daily filter to filter combination

## Filters

### A Share
- location: filter/ashare

- original frequency: weekly

- data source: choice sector

### price_limit

adjusted daily return above 9.8% (9.9%) or below -9.8% (-9.9%).

- location: filter/price_limit/99 or /98

- original frequency: daily

- data source: adjust price

### suspension (delist)

Trading amount exactly equal to 0

- location: filter/suspension

- original frequency: daily

- data source: daily trading amount

### price_level

Last day's close below certain level.

- location: filter/price_level/1 or 2

- original frequency: daily

- data source: close

### (last_)amount

Trading amount(shift 1) below certain level.

Assume a investment amount of 
1. 100,000
2. 200,000
3. 500,000
4. 1,000,000
5. 2,000,000
6. 5,000,000
7. 10,000,000
8. 20,000,000

and a relevant trading percentage of 20%, the trading amount requirement is

1. 500,000
2. 1,000,000
3. 2,500,000
4. 5,000,000
5. 10,000,000
6. 25,000,000
7. 50,000,000
8. 100,000,000



- location: filter/amount/1 to 8

- original frequency: daily

- data source: amount


### (last_)market_cap

Market Cap (shift 1) below target trading amount * 20

1. 2,000,000
2. 4,000,000
3. 10,000,000
4. 20,000,000
5. 40,000,000
6. 100,000,000
7. 200,000,000
8. 400,000,000


- location: filter/market_cap/1 to 8

- original frequency: daily

- data source: market_cap

### small_market_cap

Last day's smallest market cap

- location: filter/small_market_cap

- original frequency: daily

- data source: market_cap