# Data Loader

## Introduction

This sub-module has three main functionalities.
- Parse configuration dictionary
- Generate and route data bundle
- Download data bundle

Since additional data is needed during the process of parsing data configuration and routing data bundles, it's better for 
`DataLoader` includes the first two functionalities. 

## Workflow

A typical workflow is shown below:
1. `DataLoader` receives a configuration dictionary
2. `DataLoader` parses the dictionary to `<Data Group>` dictionary
3. `DataLoader` find symbol information from some `DataCenter`
4. `DataLoader` generate `<Data Bundle>` according to the symbol infomation and pre-defined rules
5. `DataLoader` send `<Data Bundle>` to different itself or `DataProcessor`
6. `Dataloader` download the `<Data Bundle>`
    - Find `proxy` for `DataCenter`. If not found, a new one is created.
    - Send request to `proxy`
    - Collect response from `proxy` and add data to `DataCollection`

## Definition

`<Data Config>`: A dictionary owned by users. May have some default values.

`<Data Group>`: Dictionaries owned by `DataManager`. All default values should be filled.

`<Data Bundle>`: Dictionaries owned by `DataManager`. Each `<Data Bundle>` should be a simple job, such as downloading a bunch of data,
calculate the interest rate or stack 2-D array into 3-D format

`DataCenter`: Can be "DataMaster", "ResultReader", etc.

`DataCollection`: A Dictionary contains data of different tickers. Each ticker is a 2-D bar data. 

The different attribution of a 2-D bar data can store different data. 
For example, "AUDCAD BT150" can have a "data" slot containing the OHLC price and a "fxrate" slot containing the exchange rate of the trading currency. 
The return result of `DataLoader` is an ordinary dictionary whose keys have a pattern of "Ticker/Slot", which will be used to update `DataCollection`