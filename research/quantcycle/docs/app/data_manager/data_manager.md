# Data Manager (DATA MODULE)

## Introduction

This module aims at handling all data relevant requests, including getting symbol infomation from datamaster,
getting symbol historical data from datamaster, getting symbol historical data from local backup, resampling downloaded data, 
and so on.

This module is one of the most complex modules in the project.

In short, this module will download and prepare all data for the platform.

All data can be classified as either "Historical (Time-series)" or "Static". To have data prepared, user (or platform) need to 
pass a dictionary to the module specifying all information requested by the module.

After downloading and processing data, the module will return a dictionary containing all numerical data in a format as `tuple(raw_array(data), raw_array(time_index), symbol, fields)`, while for other data types (e.g. `str`), they would be saved as it were.

## Input Format

A sample input for the module is like below
```{python}
sample_dict = {
    "Data":{
        "DataGroup1":{
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["AUDCAD", "USDTWD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 2,
                "Day": 29
            },
            "Frequency": "DAILY"
        },
        "DataGroup2":{
            "Label": "FX-RATE",
            "DataCenter": "DataMaster",
            "Symbol": 'somewhere.csv'
        },
        "DefaultValues":{
            "Type": "Download",
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 2,
                "Day": 29
            },
            "Frequency": "DAILY"
        }
        "DataGroup3": {
            "Label": "Strat",
            "Type": "Download",
            "DataCenter": "ResultReader",
            "DataCenterArgs": {
                'DataPath':r"tests/app/data_manager/data_manager_tests/hdf5_files/RSI_lv1.hdf5"
            },
            "Symbol": range(2), #strat ID must be list of int
            "SymbolArgs": {
                "Metrics": {
                    "allocation_freq": '1M', # for pd.date_range
                    "lookback_points_list": [61, 252], 
                    "addition": True,
                    "multiplier": 252
                }
            },
            "Fields": ['pnl','metrics'],
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2019,
                "Month": 12,
                "Day": 31
            },
            "Frequency": "DAILY"
        }        
    },
    ################# optional #################
    "dm_pickle":{
        "save_dir": "results/",
        "save_name": "FX_pickle_test",
        "to_pkl": True,
        "from_pkl": True
    }
}
```

### `Data` section

A `Data` section must be contained in the dictionary. Inside the `Data` section, at least one section starting with `DataGroup` must exist.

`DefaultValues` is an optional section to fill any missing value in other `DataGroup`.

A `DataGroup` is a unit data will be downloaded in the same time.

A `DataGroup` must have following keys:
- `Label`: Name of the data group
- `Type`: Either "Download" or "Queue", specifying how to get the data. "Download" means data module will actively request data from data center, while "Queue" data module will passively wait messages from queues.
- `DataCenter`: From where data will be downloaded.
- `Symbol`: Can be a list of symbols or a csv file path. Determine members of the data group.
- `SymbolArgs`(optional): `DataSource` and `BackupDataSource` decide the suffix of the symbol. For FX symbols, data source differs for different closing time. The values of these two keys can be chosen from "BT150" and "BGNL"
- `Fields`: Can be a list of string or a string. Supported fields are listed in [supported fields](docs/support_fields.md)
- `StartDate`: Must have `Year`, `Month`, `Day`. Can have `Hour`, `Minute`, `Second`
- `EndDate`: Must have `Year`, `Month`, `Day`. Can have `Hour`, `Minute`, `Second`
- `Frequency`: Can be chosen from "DAILY", "HOURLY", "MINUTE"

#### To load local hdf5 file data
- `Symbol`: Can be a list of integers. Determine the data from specific strategy ids range to be loaded.
- `SymbolArgs`: Must include `metrics` key, and the following keys under it:
    - `allocation_freq`: duration between each allocation, e.g. "1M" -> 1 month
    - `lookback_points_list`: data points required to calculate metrics, e.g. [61, 252] -> metrics_61, metrics_252
    - `addition`: returns are simply added together if True, returns are compounded if false
    - `multiplier`: multiplier to get annualized data. 252 is for daily data.
- `Fields`: Can be a list of string or a string. Supported fields are listed in [supported fields](docs/support_fields.md)
- `StartDate`: Must have `Year`, `Month`, `Day`. Can have `Hour`, `Minute`, `Second`
- `EndDate`: Must have `Year`, `Month`, `Day`. Can have `Hour`, `Minute`, `Second`

### Optional '`dm_pickle`' section:

A `dm_pickle` section contains the following 4 keys:
- `save_dir`: path for saving pickle file, e.g. "results/"
- `save_name`: name for saving pickle file, e.g. "FX_pickle_test"
- `to_pkl`: save loaded and processed data into pickle file if True 
- `from_pkl`: load data from local pickle file DIRECTLY if True 

Notes: 
- If `to_pkl` is flase and `from_pkl` is true, DM will skip the whole download process and load local pickle data ONLY.



## Output Format

The output is a dictionary containing different combination of
`<DataGroup>-<SymbolType>/<DataType>` as keys.

Take `DataGroup1` above as an example. The name of the data group is "FX";
all symbols are of type "FX". So the output will contains:
- "FX-FX/MAIN": contains the price of the symbols.
- "FX-FX/CCPFXRATE": contains the foreign exchange rate of the base currency of symbols.
- "FX-FX/INT": contains the interest rate of symbols.

The value of each key has a format as `Tuple(data, time, symbols, fields)`

The `data` is a 3-D numpy array, of which each axis is described by `time`, `symbol`, `fields`.
The `time` is a 2-D numpy array, of which each axis is described by `time`, `fields`.
The `symbols` is a list of str.
The `fields` is a list of str.

## Module Structure

The module is divided into three parts, which are `DataLoader`, `DataProcessor`, `DataDistributor`.

A typical workflow is shown below:
1. `DataManager` receives a configuration dictionary
2. `DataManager` parses configuration dictionary into `<Data Group>`
3. `DataManager` divides `<Data Group>` into small `<Data Bundle>` and routes them to different sub-module
4. `DataManager` calls `DataLoader` to download all `<Data Bundle>`s which are passed to it
5. `DataManager` calls `DataProcessor` to process all `<Data Bundle>`s which are passed to it
6. `DataManager` calls `DataDistributor` to transform all numpy array into multiprocess.raw_array