# Data Manager (DATA PROCESSOR)

## Introduction

This module is under the DataManager module. 

This module aims to process data collected by DataLoader module according to the signals given by the DataLoader module, including construction of useful df and numpy array, and calculation of extra info that cannot be loaded from any data source. 

In short, this module organizes data loaded by DataLoader into useful DataFrame, and converts them into 3D numpy array that is ready for DataDistributor to process into Raw Array.
The inputs from DataLoader are dictionary of dataframes and actions required to be completed while a single dictionary of tuples with other information would be passed to the next module as output, i.e. DataDistributor.

`tuple(numpy_array(data), numpy_array(time_index), symbol, fields)`

## Module Structure

The module is divided into two major classes, `DataProcessor` and `Method`.

A typical workflow is shown below:
1. `DataProcessor.register` receives signals from DataLoader, i.e. register `data_bundle` generated by DataLoader module
2. `DataProcessor.run` is called, consisting of 3 steps
    1. `_handle_data` calls different methods according to the actions stated in the data_bundle, and construct useful df and list of info
    2. `_convert_df_to_np` convert DataFrame generated into standardized 3D numpy array, and organize all data into a single dictionary. i.e. `dict_data_array`
    3. `_pass_data_to_distributor` passes the `dict_data_array` to the DataDistributor module

### Structure of DataFrame generated by `method`
- index: timestamp UTC
- columns: fields of each symbol in the order according to data_bundle

    e.g. for 2 symbols, 4 fields with OHLC.

    |      | Open | High | Low | Close | Open | High | Low | Close |
    |:----:|:----:|:----:|:---:|:-----:|:----:|:----:|:---:|:-----:|
    | Time |      |      |     |       |      |      |     |       |

### Structure of 3D numpy array converted from DataFrame
- The 3 indice of 3D numpy array are `time`, `symbol`, `field`.

    i.e. data_array[i,j,k]: i: time, j: symbol, k: field.

## 'Method' under DataProcessor

Major data processing "methods" are defined as Method class under DataProcessor. 
- `quantcycle/app/data_manager/data_processor/method/`

`METHOD_MAP` stores all the available class names of methods.

Output data from method is divided into two major types:


- dict_data_array: Store numpy array type data

    e.g. {'`Label FX/MAIN`': tuple(numpy_array(data), numpy_array(time_index), list(symbol), list(fields)}

- dict_data_str: Store dict of str type data
    
    e.g. {'`Label FX/INFO`: dict(info)'}

### Flow of the method call 

In the `DataProcessor.run()` method, `_handle_data()` calls `method` corresponding to the signals provided by the data bundle. 

For action in each data_bundle,

1. `METHOD_MAP` finds the corresponding method class to be used
2. `on_bundle` function is called to create_data_mapping and collect necessary data to be processed
3. `run` function under the specific method class is called, and should output neccessary df or dict according to the is_final_step, output_data_type.

### To add new data process `method`:
1. add a new key and value into `METHOD_MAP`
2. construct a new class under 'method' folder under MethodBase class
3. strictly refer to the structure of existing 'method' classes, implementing the following methods
    1. create_data_mapping(...)
    2. run(...)
4. write unittest for new method    


### Following methods are currently supported:

DataFrame to be converted into Numpy type data (to be converted into RawArray)
- output_data_type = '`df`'
1. INT (is_final_step = `True`)
2. STACK (is_final_step = `True`)
3. CCYFX (is_final_step = `True`)
4. BACKUP (is_final_step = `False`)
5. APPEND (is_final_step = `False`)
6. MIN2HOUR (is_final_step = `False`)

Numpy array (to be converted into RawArray)
- output_data_type = '`np_numeric`'
1. METRICS (is_final_step = `True`)
2. PASS3DNP (is_final_step = `True`)

Others (to be saved directly into data_package)
- output_data_type = '`bypass_arr2raw`'
1. INFO (is_final_step = `True`)
2. STRATIDMAP (is_final_step = `True`)

## Methods List

## 'df' numpy type data
### `INT`
This method calculate the interest rate of currency paris from the TomNext rate and FX rate.

- Return DataFrame

|            | CCP1 INT | CCP2 INT| CCP3 INT|
|------------|----------|---------|---------|
| 01-01-2020 |
| 02-01-2020 | 


Necessary Data: 
1. TomNext 
2. FX rate 

### `STACK`
This construct a target dataframe through pd.concat of dfs along horizontal axis.

- DataFrame for each symbol

|      | Open | High | Low | Close |
|:----:|:----:|:----:|:---:|:-----:|
| Time |      |      |     |       |

- resulting DataFrame

|      | Open | High | Low | Close | Open | High | Low | Close |
|:----:|:----:|:----:|:---:|:-----:|:----:|:----:|:---:|:-----:|
| Time |      |      |     |       |      |      |     |       |

Necessary Data: 
1. pd.DataFrame with same time index and fields

### `CCYFX`
This construct dataframes with evaluated XXXUSD FX rate of trading currency.

- return DataFrame

|            | EURUSD | GBPUSD | IDRUSD |
|------------|------|------|-----|
| 01-01-2020 |
| 02-01-2020 | 

Necessary Data: 
1. Currency FX rate with USD

### `BACKUP`
This backups data loaded from one DataSource with BackupDataSource.

- DataSource (f"{symbol} {self.data_source}/{self.slot}")

|            | Open | High | Low | Close |
|------------|------|------|-----|-------|
| 01-01-2020 | 1    | 2    | 3   | NaN   |
| 02-01-2020 | 5    | NaN  | 7   | 8     |

- BackupDataSource (f"{symbol} {self.backup_data_source}/{self.slot}")

|            | Open | High | Low | Close |
|------------|------|------|-----|-------|
| 01-01-2020 | 1    | 1    | 1   | NaN   |
| 02-01-2020 | 5    | 6    | 7   | 8     |
| 04-01-2020 | 9    | 9    | 9   | 9     |


- new DataFrame (f"{symbol} {self.new_data_source}")

|            | Open | High | Low | Close |
|------------|------|------|-----|-------|
| 01-01-2020 | 1    | 2    | 3   | NaN   |
| 02-01-2020 | 5    | 6    | 7   | 8     |
| 04-01-2020 | 9    | 9    | 9   | 9     |

Necessary Data:
1. DataFrame from the DataSource
2. DataFrame from the BackupDataSource

### `APPEND`
This will append data from AppendDataSource into the target data from the DataSource

- DataSource

|            | Open | High | Low | Close |
|------------|------|------|-----|-------|
| 01-01-2020 |      |      |     |       |
| 02-01-2020 |      |      |     |       |

- AppendDataSource

|            | Open | High | Low | Close |
|------------|------|------|-----|-------|
| 03-01-2020 |      |      |     |       |

- return df

|            | Open | High | Low | Close |
|------------|------|------|-----|-------|
| 01-01-2020 |      |      |     |       |
| 02-01-2020 |      |      |     |       |
| 03-01-2020 |      |      |     |       |


Necessary Data:
1. DataFrame from the DataSource
2. DataFrame from the AppendDataSource

### `MIN2HOUR`
This generate a new df from the minute data into hour data. 

Necessary Data:
1. DataFrame from the DataSource (minute data)

## Numpy array (bypass df conversion to np array)

### `METRICS`

This will calculate metrics for strategy

```{python}
# in the following order
[
    'holding_period_return',
    'sharpe_ratio',
    'MDD',
    'ADD', 
    'rate_of_return',
    'number_of_trades',
    'return_from_longs',
    'return_from_shorts', 
    'ratio_of_longs',
    'hit_ratio',
    'profit_rate',
    'return_from_hits', 
    'return_from_misses',
    'return_from_profit_days',
    'return_from_loss_days'
]
```

Necessary Data:
1. hdf5 file
2. allocation_freq # for pd.date_range
3. lookback_points_list
4. addition
5. multiplier
6. OutputDataType

e.g. config
```{python}
        ...
        "SymbolArgs": {
            "Metrics": {
                "allocation_freq": '1M', # for pd.date_range
                "lookback_points_list": [61, 252], 
                "addition": True,
                "multiplier": 252
            }
        },
        "Fields": ['metrics'],
        ...
```

### `PASS3DNP`

To pass loaded 3D numpy array data directly to dict_np_data_need_pack for data_distributor.

For ResultReader, the last tuple value is "Strat ID to Symbols/fields Mapping"

Special case for id_sym_map:
As for 'pnl', there is only one 'pnl' value for one strat ID, id_sym_map would be
'pnl': [ ['pnl'], ['pnl'], ... ]

## Others

### `INFO`
This will get the info of symbols and output

Necessary Data:
1. DataFrame from the DataSource
2. DataFrame from the AppendDataSource

### `STRATIDMAP`
To generate the list of symbols list used for the strategy. In the order given by the ResultReader.


e.g. in the order of strat ID 

[

    ['AUDUSD'],
    ['AUDUSD', 'NZDJPY]', 
    ['AUDJPY', 'CADCHF'],
    ...
]

Necessary:
1. hdf5 file
