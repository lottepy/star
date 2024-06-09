# Magnum QuantCycle V2.1.2

A platform that manages both backtesting and live trading for full-cycle quant strategy 
development.

## Highlights

 - The platform can seamlessly switch from backtesting to live trading, saving much pain from strategy developers to
   deal with the underlying complications.
 - The architecture supports multi-level abstraction. We demonstrate a typical *[three-level framework](#a-brief-workflow)* 
   for multiple strategies. Strategy allocation algorithms at the higher level can be developed to combine 
   strategies at lower levels.

## Supported Assets and Data Source

 - Instrument types: Forex, equities, and futures.
 - Frequency: Daily, hourly, and minutely.
 - Data source: DataMaster and local CSV.

<!---## Prerequisites

This platform requires *Data Master Client* to run. Please follow https://gitlab.aqumon.com/pub/py-datamaster-client
to install Data Master Client. Be advised to start a new virtual environment in case any 
installed packages are screwed up.-->

## Module Naming

### Action modules

 - **[ [Data manager](app/data_manager/data_manager.md) ]** To manage data input and output.
    - *[Data loader](app/data_manager/sub-modules/data_loader.md)* downloads raw data from various data sources 
       and formalize the downloaded data.
    - *[Data distributor](app/data_manager/sub-modules/data_distributor.md)* feeds signal generator with specific 
       window sizes along the timeline.
    - *[Data processor](app/data_manager/sub-modules/data_processor.md)* organizes data in different format to 
       serve the data modules.
 - **[ Signal generator ]** A trading strategy takes historical market data and current holdings as 
                        input. It performs calculations and generates target orders. This is the module on
                        which the strategy algorithm developer should focus. Refer to 
                        *[Strategy Development Section](#strategy-development)* for details.
 - **[ [Order crosser](docs/app/order_crosser/order_router.md) ]** Functioning like an exchange. Its output is 
                        the order execution data.
 - **[ [Risk manager](docs/app/risk_manager/risk_manager.md) ]** To provide general order verification before 
                    order submission, rejecting invalid orders and orders of high risk.
 - **[ Status recorder ]** Used for signal generator to dump status calculated during backtesting, 
                       such that live trading can be switched to immediately by
                       loading the status, exempting from rerunning the strategy on historical
                       market data.
 - **[ Result exporter ]** saves data to files and reads data from files.
 - **[ Engine ]** Coordinating all modules for backtesting and live trading.
<!--- - **[ Algo trader ]** To support advanced orders such as TWAP and VWAP. -->
 
### Supporting modules

 - **[ [PMS manager](docs/app/pms_manager/pms_manager.md) ]** To manage portfolio data passing among core modules. It holds positions and 
                   calculates P&L, costs, etc. It also manages user accounts.
                   
### Module availability

Due to application and implementation differences between backtesting and live trading, we list the 
availability of the above modules for backtesting and live trading respectively:

| Module                              | Backtesting | Live Trading |
|-------------------------------------|:-----------:|:------------:|
| Data manager                        |      ✓      |      ✓       |
| Signal generator                    |      ✓      |      ✓       |
| Order crosser (local)               |      ✓      |      ✓       |
| Order crosser (remote<sup>1</sup>)  |             |      ✓       |
| Risk manager                        |             |      ✓       |
| Status recorder                     |      ✓      |              |
| Result exporter                     |      ✓      |      ✓       |
| Engine                              |      ✓      |      ✓       |
| PMS manager                         |      ✓      |      ✓       |

*<sup>1</sup>Remote order crosser pushes the order to other project servers, for example, by FTP.*


## A Brief Workflow

The hierarchical architecture of our QuantCycle platform supports multi-level abstraction where higher 
level summarizes results and selects models from lower levels.

Typically, we define three levels of abstraction. The higher level takes the output of the lower level 
as input. The modules and pipeline in each level are similar. The main difference between levels is the 
abstraction of data and thus the algorithms in signal generators devised to handle the data. 

### Level 1

Level 1 is the strategy level---the finest level where each strategy parameter is separately evaluated.
One engine runs one strategy. An engine takes the market data as input and runs through the pipeline
which includes the following steps:

 - The user defines strategies and assets that the strategies work on.
 - Data manager pulls market data, organizes the data and feeds signal generators.
 - Signal generators are where strategy developers develop a pool of strategies.
 - Strategies fire trading signals which are checked by risk manager. They are then sent to algo 
   trader and order crosser for order execution.
 - Strategies may also call status recorder to load or save strategy status in order to seamlessly 
   switch from backtesting to live trading.
 - Final results and trading summary can be plotted or exported to csv files for further investigation.
 
### Level 2

Level 2 is the category level---the mid-level where strategies are combined by strategy allocation. The
allocation algorithm determines good parameters for a strategy based on its Level-1 results. It also 
determines a combination of strategies. Different parameters of the allocation process result in different
combinations. Each combination is called a category. One Level-2 engine runs one category. The output of
the Level 2 is the category results/performance.

### Level 3

Level 3---the highest level---combines different categories from Level-2 altogether. Only one engine is
in charge at this level. The engine determines the final trading signals / target orders.

### Flexibility of levels

Our architecture is indeed flexible on the number of levels and their definitions. Advanced users can 
adjust the levels on their needs, implementing algorithms for each level. For simplicity, a one-level 
framework may already suffice to strategy development.
 
## Getting Started
 
### Quick Guide

Please refer to the following guides at *[README.md](README.md)* for quick start:

 - [Backtesting quick guide](README.md#strategy-backtest)
 - [Live trading quick guide](README.md#production-run)

<!---The big picture of backtesting pipeline includes the following steps:

```python
import pandas as pd
from quantcycle.engine.backtest_engine import BacktestEngine

backtest_engine = BacktestEngine()      # engine init

# Load engine config (json) file and strategy pool dataframe
engine_config_file = r'strategy/FX/technical_indicator/oscillator/KD/config/KD_lv1.json'
strategy_pool_df = pd.read_csv(r'strategy/FX/technical_indicator/oscillator/KD/
                               strategy_pool/KD_lv1_strategy_pool.csv')
backtest_engine.load_config(engine_config_file, strategy_pool_df)

backtest_engine.prepare()               # load data and modules
backtest_engine.start_backtest()        # do backtesting
backtest_engine.export_results()        # save status and export results
```

Similarly, for live trading:

```python
import pandas as pd
from engine_manager import EngineManager

engine_manager = EngineManager()                                                # Engine init

engine_manager.add_engine('strategy/FX/technical_indicator/oscillator/RSI/config/RSI_lv1.json'),
    pd.read_csv(r"strategy/FX/technical_indicator/oscillator/RSI/strategy_pool/RSI_lv1_strategy_pool.csv")) 
                                                                                # Add new engine with config and strategy pool 

load_data_event = engine_manager.load_engine_data()                             # Perpare data for using
engine_manager.start_engine() 
engine_manager.run()                                                            # Start live trading 

load_data_event.wait()          

timepoint = '20121231'                                                          # Update for new day 
engine_manager.handle_current_fx_data(timepoint)                                # Handle current data
handle_current_data_event = engine_manager.handle_current_data(timepoint)       # Handle current fx data
handle_current_data_event.wait()
engine_manager.handle_current_rate_data(timepoint)                              # Handle current rate data

                                                                                
engine_manager.kill_engine()       
engine_manager.wait_engine()
engine_manager.kill()
engine_manager.wait()                                                           # Stop all engines
```-->


### Outline

In the following sections, we walk through all the aspects a strategy developer need to concern. They are:

 - Engine input, including *[strategy pool settings](#strategy-pool-settings)* and *[engine configuration](#engine-configuration)*.
 - Examples of multi-level strategy pool settings, starting at *[Level-2 settings](#level-2-strategy-pool-settings)*.
 - *[Speed boost in backtesting](#speed-boost-in-backtesting)* over big data
 - Strategy algorithm development---the *[`on_data()` function call](#function-on_data)*
 - Inspecting the results by *[ResultReader](#inspecting-the-results)*
 - Market data input by *[local CSV storage](#market-data-input-by-local-csv)* instead of downloading the
   data from DataMaster
 - Non-tradable *[reference data](#reference-data)* for strategies to watch.
 - *[Signal remark](#signal-remark)* for strategies to save and load remarks in the `on_data()` loops.
 - *[Demo](#demo)* for all the example code.
 - *[Functions in development](#functions-in-development)*


Now we take a closer look on engine's input. Generally, an engine needs two types of
parameters to set it up: *[strategy pool settings](#strategy-pool-settings)* and *[engine configuration](#engine-configuration)*. 

### Strategy Pool Settings

All three levels share this same protocol. The main difference between levels is the strategy algorithms in
*signal generator*. Before starting the engine, you need to prepare strategy pool settings. We show an example 
of a simple RSI strategy setting for Level-1 engine in *[this csv file](docs/linked_examples/RSI_lv1_strategy_pool.csv)*
which can be generated using the following helper function:

```python
from quantcycle.utils.strategy_pool_generator import strategy_pool_generator

pool_setting = {"symbol": {"FX": ["EURGBP", "EURJPY", "AUDNZD"]},
                "strategy_module": "strategy.FX.technical_indicator.oscillator.RSI.algorithm.RSI_lv1",
                "strategy_name": "RSI_strategy",
                "save_path": "examples/RSI_strategy/strategy_pool/RSI_lv1_strategy_pool.csv",
                "params": {
                    "length": [10],
                    "break_threshold": [10, 20, 30],
                    "stop_profit": [0.01],
                    "stop_loss": [0.005],
                    "max_hold_days": [10]
                }}

strategy_pool_df = strategy_pool_generator(pool_setting)
```

Typical settings of the strategy pool include:

 - `symbol`: A dict with keys specifying asset classes `FX`, `STOCKS`, or `FUTURES`. The value is a list of
             symbols. You also need to add the asset classes to the *[data source subscription](#data-sources)* 
             in the engine configuration. 
 - `strategy_module`: The module path that should follow the naming convention of python packages.
 - `strategy_name`: The user-defined strategy name.
 - `save_path`: The path for saving this pool setting to a csv file.
 - `params`: A dict with strategy-related parameters. Keys are the names and values are lists of parameters.
             The helper method generates combinations of the parameters.

The helper function `strategy_pool_generator()` saves a csv file at `save_path` by default. It also returns the 
DataFrame of this csv file. Using the helper is indeed optional. You can create a csv file directly to set up a 
strategy pool or just pass the DataFrame you create in the same format of the csv to the engine.

Corresponding to *[this strategy pool settings](docs/linked_examples/RSI_lv1_strategy_pool.csv)*, our example 
RSI strategy algorithm can be found at *[RSI_lv1.py](docs/linked_examples/RSI_lv1.py)*.
We will give more details on how to develop a strategy algorithm in *[Strategy Development Section](#strategy-development)*.


### Engine Configuration

Next, you need to prepare the engine configuration in a json file, for example, *[RSI_lv1.json](docs/linked_examples/RSI_lv1.json)*.
Each component of this json file is detailed below.

#### Backtest time

```
  "start_year": 2012,
  "start_month": 1,
  "start_day": 1,
  "end_year": 2020,
  "end_month": 1,
  "end_day": 1,
```

 - `start_year/month/day`: Backtest starting day
 - `end_year/month/day`: Backtest ending day
 - **For live trading engine:** 
    - `start_year/month/day`: Backtest starting day for Level 1
    - `end_year/month/day`: Backtest ending day for Level 1
    - Live trading engines do not backtest on Level-2 or any higher levels.
    - Note: If the live engine is on Level 2 or Level 3, set `start_year/month/day` the same to Level-1 
      configuration. It makes sure that enough data is collected for metrics' calculation.


#### Data sources

```
  "data": {
    "FX": {
      "DataCenter": "DataMaster",
      "SymbolArgs": {
        "DataSource": "BGNL",
        "BackupDataSource": "BGNL"
      },
      "Fields": "OHLC",
      "Frequency": "DAILY"
    }
    "STOCKS": {
      "DataCenter": "DataMaster",
      "SymbolArgs": {
        "DMAdjustType": 0,
        "DMCalendar": "SSE",
        "AccountCurrency": "USD"
      },
      "Fields": "OHLC",
      "Frequency": "DAILY"
    }
  },
  "secondary_data": {
    "RSI_lv1": {
      "DataCenter": "ResultReader",
      "DataCenterArgs": {
        "DataPath": "results/test_FX/result/RSI_lv1"
      },
      "SymbolArgs": {
        "Metrics": {
          "allocation_freq": "1M",
          "lookback_points_list": [61, 252],
          "addition": true,
          "multiplier": 252
        }
      },
      "Fields": ["pnl", "metrics", "position"],
      "Frequency": "DAILY"
    }
  },
  "dm_pickle": {
    "save_dir": "results/test_FX/cache/",
    "save_name": "dm_pickle_RSI_lv2",
    "to_pkl": true,
    "from_pkl": false
  },
```

 - `data`: A dict specifying *main data* sources. Each data source is defined by a dict inside. The main data is 
           **the market data for trading**. The timestamps of the main data are used to coordinate the entire 
           pipeline. As a result, the highest frequency a strategy can trade is the main data's frequency.
    - `FX` is the label of the Forex asset group. **Available labels are `FX`, `STOCKS`, and `FUTURES`.**
    - `DataCenter` tells *data manager* where to fetch data. It can be `DataMaster` or `LocalCSV`. Local CSV data 
                   source has its own format. See details *[here](#market-data-input-by-local-csv)*.
    - `DataCenterArgs` and `SymbolArgs` are data source specific. See *[data manager's readme](app/data_manager/data_manager.md)*.
    - `Fields`: For main data, "OHLC" is required. The value accepts a name string such as `"OHLC"` or a list,
                e.g. `["OHLC", "volume"]`.
    - `Frequency`: The default data frequency is `DAILY`. Although engine supports `HOURLY` and `MINUTELY`, 
                   intra-day data is subject to the data availability of DataMaster.
 - `secondary_data`: A dict specifying *secondary data* sources. The secondary data is the output from the lower 
                     level (pnl, positions, metrics, etc.) that higher level engines take as input. In the example 
                     above, `RSI_lv1` is this kind of output from Level-1 engine.
    - `RSI_lv1`: This label is user-defined. A typical setting is the strategy name of this data.
    - `DataCenter`: Must be `ResultReader`.
    - `DataCenterArgs`: To define the `DataPath` inside. It is the path configured in the engine's `result_output`
                        that generates this data.
    - `SymbolArgs`: See *[data manager's readme](app/data_manager/data_manager.md)*.
    - `Fields`: Any of the `["pnl", "positions", "cost", "metrics"]`.
    - `Frequency`: The actual frequency of this data.
 - `dm_pickle`: As downloading online market data takes much time in backtesting, this option provides a way to
   save the online market data pulled in the first run to your local storage, so that in the later runs, there is
   no need to download the data again. **Note that `dm_pickle` works only when all the data you request is exactly
   the same as the data in your storage.** To use this feature properly, in the first run, set
    - `to_pkl` to `true` and `from_pkl` to `false`.
  
   In the later runs, set
    - `to_pkl` to `false` and `from_pkl` to `true`.
    - `save_name`: You should bind the name to the level of the engine as this feature doesn't work across levels
                   anyway.
 - Follow *[data manager's readme](app/data_manager/data_manager.md)* to learn more about specific settings of 
   each data source, such as `DataCenterArgs` and `SymbolArgs` configuration.

#### Reference data setting
 
```
  "ref_data": {
    "ref_hourly": {
      "DataCenter": "LocalCSV",
      "DataCenterArgs": {
        "dir": "dummy_stock_hourly"
      },
      "Symbol": ["STOCK_1", "STOCK_2", "STOCK_3"],
      "StartDate": {
        "Year": 2018,
        "Month": 9,
        "Day": 12
      },
      "EndDate": {
        "Year": 2019,
        "Month": 1,
        "Day": 10
      },
      "Frequency": "HOURLY"
    }
  },
```

 - See *[reference data section](#reference-data)*.
 
Don't confuse reference data with secondary data mentioned above. *Reference data* can be any user-provided 
time-series data. A common use case for reference data is the market data for strategy reference. Inside 
an `on_data()` loop, the strategy can refer to the reference data just like the main data, but could not
trade the assets. On the other hand, secondary data is narrowly defined. It contains the results and 
metrics of a strategy pool which is the output of an engine.
 
#### Account information

```
  "account": {
    "cash": 2900000,
    "commission": 0
    "commission_path": "tests/engine/backtest/data/aqm_turnover_union_fx.csv"
  },
```

 - `account`: A dict specifying account's initial cash and a file contains commission fee for each symbol.
   Check the *[sample file](tests/engine/backtest/data/aqm_turnover_union_fx.csv)* here.
    - Commission fee is the trading cost for each transaction. <!---If the key is omitted entirely, the 
      default commission fee is 0. Note that if this file is provided, all symbols must be listed in the file.-->
    - Commission fee lookup priority: 
        1.  The file at `commission_path`.
        2.  If the above is omitted, the number stated in `commission` field will be taken and applied to all 
            symbols.
        3.  If neither of the above is provided, the engine will assume 0 commission fee.
      
#### Strategy algorithm

```
  "algo": {
    "base_ccy": "USD",
    "window_size": {
        "main": 5
        "ref_data": 5
    }
  },
```

 - `algo`: A dict specifying strategy pool settings, base currency, window sizes used by strategies<!---, 
   and `monitor_open`-->.
   - `base_ccy` defines the account currency, e.g. `USD`, `CNY` and `LOCAL`. The definition here has 
      the highest priority if you have any conflict `AccountCurrency` in the data source setting.
   - `window_size` is a dict specifies window sizes of all data fields. It defines how many timestamps 
      the strategy need to look back in time. The default window size is 1:
      - `main`: The window size of the main data, including `fxrate` and `int`. (Required)
      - `ref_data`: The window size for all reference data. (Optional)
      - The name of a secondary data field (optional). For example, `RSI_pnl`. The default window size 
        is 1 if none of these names are defined.
<!--- - `monitor_open` is for daily strategies to check the data at the market open. 
      By default, it is false which means the strategy can only get the data of the day at the market
      close even for the opening price. When `monitor_open` is set `true`, the strategy can see the
      opening price at the trading hours of the day.
      
 - **Live trading notes:** `monitor_open` is ignored in live trading engine. Currently it is only supported
   in backtesting!-->

#### Settings about timing

```
  "timing": {
    "calc_rate_time_utc": "22:00"
  },
```

 - `calc_rate_time_utc`: Used by intraday data. You can define when to calculate rates at the day end. The 
   default value is 22:00 UTC if the key is omitted entirely. If the main data's timestamp skipped this time, 
   the rates would be calculated at the next available data point. 

#### Result export

```
  "result_output": {
    "flatten": false,
    "save_dir": "examples/RSI_strategy/result/",
    "save_name": "rsi_lv1",
    "save_n_workers": "best",
    "status_dir": "examples/RSI_strategy/status/",
    "status_name": "rsi_lv1"
  },
```

 - `result_output`: A dict for result export, 
    - `flatten` means whether to treat each symbol as a separate strategy. By default, when `flatten` 
      is `false`, a strategy is corresponding to a portfolio with a set of strategy parameters. A portfolio 
      is managed by *PMS manager*.
    - `save_dir` and `save_name` are simply shown above.
    - `save_n_workers` specifies the number of processes for parallel writing. If the key is omitted, the 
       default is the number of logical CPU cores + 1. This effect is the same if set to `"best"`.
    - `status_dir` and `status_name` are used for saving and loading strategy status. Status saving and 
       loading will be disabled if these fields are not given.
 - **Live trading notes:** `save_n_workers`,`status_dir` and `status_name` are ignored in live trading engine.
 
#### Speed optimization

```
  "optimization": {
    "numba_parallel": false,
    "python_parallel_n_workers": "best"
  }
```

 - `optimization`: A dict includes parameters for engine speed optimization.
    - `numba_parallel`: Enable numba parallel to speed up the calculation. True is recommended in your
      production run.
    - `python_parallel_n_workers`: If you somehow cannot work under Numba, we provide this alternative option
      to use python multiprocessing. **Make sure Numba JIT is disabled before you enable this option.** To 
      disable it, see the *numba main switch* mentioned in *[speed boost section](#speed-boost-in-backtesting)*.
       - `"best"` will set the process number to be the number of your logic CPU cores minus 1.
       - To set it on your own, you can put `2`, or `3`... indicating the number of processes.
       - To disable this option, simply set it to `0` or remove the whole line.
    - Note that parallelization is implemented by distributing strategies. If you have only 1 strategy,
      parallelization will not take effect.
 - **Live trading notes:** Numba is not implemented in live trading. These settings will be ignored.

#### Live trading engine pointer

```
  "engine": {
    "engine_name": "RSI",
    "need_backtest" : false,
    "is_sync_cash" : true,
    "is_sync_holding" : true,
    "Order_Router_module": "quantcycle.app.order_crosser.order_router",
    "Order_Router_name": "TestOrderRouter"
  }
```

 - only used for live trading.
 - `engine_name`: unique identifier for each engine.
    - used for secondary calling
 - `need_backtest`: need first backtest and then live.
 - `is_sync_cash`: sync cash from broker.
 - `is_sync_holding`: sync holding from broker.
 - `Order_Router_module`: order router module used for live trading.
 - `Order_Router_name`: order router class used for live trading.

#### Live trading order router
```
  "PortfolioTaskEngineOrderRouter": {
    "ACCOUNT": '369',
    "task_ippath" : 'https://algo-stg.aqumon.com/oms/api/task-engine/',
    "subtask_ippath" : 'https://algo-stg.aqumon.com/oms/api/execution-engine/',
    "currency" : 'CNY'
  }
```

### More Strategy Pool Settings

In order to better demonstrate the hierarchical architecture, we show an example three-level configuration 
here.

#### Level-2 strategy pool settings

Recall the Level-1 strategy pool settings introduced *[here](#strategy-pool-settings)*. Level-2 settings 
for our simple RSI strategies are shown below:

```python
    pool_setting = {"symbol": {"FX": ["EURGBP", "EURJPY", "AUDNZD"],
                               "RSI": list(np.arange(3 * 3).astype(np.float64))},
                    "strategy_module": "strategy.FX.technical_indicator.oscillator.RSI.algorithm.RSI_lv2",
                    "strategy_name": "allocation_strategy",
                    "params": {}}

    strategy_pool_df = strategy_pool_generator(pool_setting, save=False)
```

Note that the main change in the settings of Level 2 is the additional `RSI` symbols line, suggesting 
that in addition to the `FX` symbols, the Level-2 strategy will take the input of `RSI` which is the result 
from Level-1 RSI strategy.
 - The `FX` symbols usually match the symbols of the Level-1 strategy.
 - The `RSI` symbols are the ID list in the Level-1 strategy results. If Level-1 engine has `flatten=False`, 
   the IDs simply correspond to the actual strategies in Level 1, i.e., [0, 1, 2] for the Level-1 example. 
   Otherwise, if `flatten=True`, the list has 9 IDs (3 by 3), representing a total of 3 strategies x 3 
   symbols, i.e., [0, 1, ... 8] for the Level-1 example as shown above. IDs are `int` type by nature, but 
   `astype(np.float64)` is a must here for Numba compatibility.
 - `strategy_module` is the Level-2 algorithm that allocates strategies at Level 1. The module is named 
   "allocation_strategy" in the example.
 - The other fields are similar to Level-1 settings.
 
In addition to strategy pool settings, for Level 2, you also need to provide engine configuration file, 
for example, *[RSI_lv2.json](docs/linked_examples/RSI_lv2.json)*, similar to the file in Level 1.
 
#### Level-3 strategy pool settings

```python
    pool_setting = {"symbol": {"FX": ["EURGBP", "EURJPY", "AUDNZD", "GBPUSD", "NOKSEK", "NZDJPY"],
                               "RSI": list(np.arange(1).astype(np.float64)),
                               "KD": list(np.arange(1).astype(np.float64))},
                    "strategy_module": "strategy.Combination.combination_method1.algorithm.combination",
                    "strategy_name": "combination_strategy",
                    "params": {}}

    strategy_pool_df = strategy_pool_generator(pool_setting, save=False)
```

Level-3 engine combines categories from Level 2 and then generates final target orders. In the example 
above, the engine combines `RSI` and `KD` strategy categories altogether.
 - The `FX` symbols are the union of all symbols used in lower-level strategies.

For engine configuration, see *[combination.json](docs/linked_examples/combination.json)*.


### Speed Boost in Backtesting

#### Numba and parallelization

Backtesting engine supports Numba optimization and parallelization. Numba is our recommended solution to
boost the speed of a heavy task. Please check the following switches to enable optimization. Note that 
Numba requires strategy algorithms to use Numba supported functions only, which has a limited coverage. 
Unfortunately, not all numpy functions have been rewritten in Numba. Some numpy function arguments are 
not fully implemented. We thus recommend strategy developer to double check the results by turning Numba 
on/off.

 1. Numba main switch: The `.numba_config.yaml` file. It should be put in the **working directory** where 
    the path '.' refers to. Inside, set `disable_jit: 0` to enable Numba. Alternatively, you can set the
    environment variable `NUMBA_DISABLE_JIT=0` before starting the backtesting program. The environment 
    variable has higher priority such that if it is found, the `.numba_config.yaml` file is ignored.
 2. Besides Step 1, make sure your strategy class (described in the following section) is decorated 
    correctly by Numba's jitclass decorator.
    ```python
    @defaultjitclass()
    class YourStrategy(BaseStrategy):
        ...
    ```
 3. Enable Numba parallelization by *[this engine configuration](#speed-optimization)*.
 4. Enable parallel HDF5 writing by *[this engine configuration](#result-export)*. For best performance,
    set `save_n_workers` to `"best"`.

For numba supported functions, see:
https://numba.pydata.org/numba-doc/dev/reference/numpysupported.html

#### Python multiprocessing

Although Numba provides a great speed boost, sometimes, a user find it too difficult to implement Numba
compatible algorithms. In this case, backtesting engine can apply python multiprocessing to make better 
use of your multiple cores. Simply follow *[this engine configuration](#speed-optimization)* to set it 
up. Remember to disable Numba JIT before doing so.

#### `dm_pickle`

In addition, since downloading the market data online takes much time in backtesting, in the engines' 
first run, you can use `dm_pickle` to save the downloaded data to your local storage so as to save downloading 
time when you want to rerun the engine. **Note that to take advantage of this feature, you cannot change the 
strategy pool settings such as to add new symbols in the second run.** To manage `dm_pickle`, check the 
*[engine configuration here](#data-sources)*. There is noticeable saving in data loading time if you use 
the downloaded (pickled) market data.


### Strategy Development

The focus of a strategy developer is to implement the algorithms in signal generators for each level from 
Level 1 to Level 3. It is a good idea to start at the strategy level which is Level 1 at the beginning.

A strategy algorithm should extend the super class `BaseStrategy` (see [*here*](quantcycle/app/signal_generator/base_strategy.py)) 
by implementing four methods at least:

```python
from quantcycle.app.signal_generator.base_strategy import defaultjitclass, BaseStrategy

@defaultjitclass()
class YourStrategy(BaseStrategy):
    def init(self):
        # Your own constructor... Note the function name does not contain "__"
        pass

    def save_status(self):
        # Called by status recorder. It expects the return of a dict
        # where you dump all the strategy status
        strategy_status = Dict.empty(types.unicode_type, nb.float64)
        self.status = strategy_status
        
    def load_status(self, pickle_dict: dict):
        # Called by status recorder. It delivers a dict from which
        # you can restore your strategy.
        strategy_status = pickle_dict
        self.status = strategy_status
        
    def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
        # Implement your strategy algorithm here. Return a 2D array detailed
        # below.
```
 
#### Function `on_data`
 - Input: 
    - `data_dict` is a dictionary containing the data: 
       - Key:   data name  
       - Value: data array is 3D-array (time_dots, symbols, fields)
    - `time_dict` is a dictionary containing the time information corresponding to the data_dict: 
       - Key:   data name (same as keys in data_dict)
       - Value: time array is 2D-array (time_dots, time_fields)
    - `ref_data_dict` is a dictionary containing the reference data: 
       - Key:   user defined reference data name  
       - Value: data array is 3D-array (time_dots, symbols, fields)
    - `ref_time_dict` is a dictionary containing the time information corresponding to the data_dict:
       - Key:   reference data name (same as keys in ref_data_dict)
       - Value: time array is 2D-array (time_dots, time_fields)

**Your strategy should by no means alter the data in the dictionaries above as they are not a copy.**
    
In the example below, 'KD_lv1_pnl', 'KD_lv1_metrics_252', 'KD_lv1_metrics_61' are secondary data sources 
subscribed in the engine configuration. We show data with window size = 1.

```python
    data_dict = {   'main': array([[[ 1.6171,  1.6274,  1.6134,  1.6242],
                                  [ 1.1653,  1.1712,  1.1633,  1.1702],
                                  [70.507 , 71.624 , 70.242 , 71.558 ]]]), 
                    'fxrate': array([[[0.8078    ],
                                      [1.5988    ],
                                      [0.01313715]]]),
                    'int': array([[[5.38603954e-05],
                                   [3.30765000e-06],
                                   [2.10741851e-05]]]),
                    'ex_tradable': array([[[1.],
                                           [1.],
                                           [1.]]]),
                    'KD_lv1_pnl': array([[[-33520.53730073],
                                          [ 15677.87797952],
                                          [ 30793.1975143 ]]]), 
                    'KD_lv1_metrics_252': array( [[[ -4.01880774e-02, -9.17929989e-01,  5.68658063e-02,
                                                      2.17376885e-02, -4.01880774e-02,  3.30000000e+01,
                                                      6.94980625e-03,  2.74792665e-02,  4.54545455e-01,
                                                      5.45454545e-01,  5.04065041e-01,  4.00576366e-03,
                                                     -2.51164488e-03,  2.79956399e-03, -3.50427942e-03]]]), 
                    'KD_lv1_metrics_61':  array( [[[-4.24528286e-03, -4.44965719e-01,  1.82738565e-02,
                                                      7.16106214e-03, -1.75378899e-02,  9.00000000e+00,
                                                     -8.78146949e-03,  4.38186022e-02,  5.55555556e-01,
                                                      5.55555556e-01,  5.62500000e-01,  3.87059090e-03,
                                                     -2.71793595e-03,  2.22416418e-03, -3.16287415e-03]]])
                }
```  
       
```python    
    time_dict = {   'main':               array([[1356912000, 1356998400,    0,    2012,  12, 31,    0,     0,     0]]), 
                    'fxrate':             array([[1356912000, 1356998400,    0,    2012,  12, 31,    0,     0,     0]]),
                    'int':                array([[1356912000, 1356998400,    0,    2012,  12, 31,    0,     0,     0]]),
                    'ex_tradable':        array([[1356912000, 1356998400,    0,    2012,  12, 31,    0,     0,     0]]),
                    'KD_lv1_pnl':         array([[1356912000, 1356998400,    0,    2012,  12, 31,    0,     0,     0]]),
                    'KD_lv1_metrics_252': array([[1356912000, 1356998400,    0,    2012,  12, 31,    0,     0,     0]]), 
                    'KD_lv1_metrics_61':  array([[1356912000, 1356998400,    0,    2012,  12, 31,    0,     0,     0]])
                }
```                 

 - `ex_tradable`: Exchange tradable status. There are four states:
    - 1.0 - ok to trade
    - 2.0 - not able to trade at this time
    - 3.0 - sell only *(not implemented in this version)*
    - 4.0 - buy only *(not implemented in this version)*

The content of `ref_data_dict` and `ref_time_dict` are similar to the main data dictionaries above. For 
details, please go to *[reference data section](#reference-data)*.
 
Return: orders in 2D-array:
  - Format: np.array((2 fields, n symbols))
     - First row: execution order size -> int
     - Second row: execution order limit price -> float, for limit order only. This row is optional.

The example below shows a return order with three symbols. The first row specifies target order size,
the second row specifies limit price.
 
```python    
    execution order= array([[10,     20,    30],
                            [3.65,  4.0, 121.8]])
                   
```    
`BaseStrategy` provides following methods for `on_data()` to call in the return statement. The strategy 
developer can choose with ease:
 - `return_reserve_order_base_ccy`
    - Output: the execution order (1D array) 
    - Input:  the change amount in base currency for each symbol (1D array)
 - `return_reserve_target_base_ccy` 
    - Output: the execution order (1D array) 
    - Input:  the target amount in base currency for each symbol (1D array)
 - `return_reserve_order` 
    - Output: the execution order (1D array) 
    - Input:  the change amount in local currency for each symbol (1D array)
 - `return_reserve_target` 
    - Output: the execution order (1D array) 
    - Input:  the target amount in local currency for each symbol (1D array)
 - `return_target` 
    - Output: the execution order (1D array) 
    - Input:  the target volume for each symbol

The relevant code is shown below:
  
```python
    def fx_convert(self, order):
        return order * (1 / self.current_fx_data)

    def return_reserve_order_base_ccy(self, base_ccy_order):
        local_ccy_converted_order = self.fx_convert(base_ccy_order)
        return self.return_reserve_order(local_ccy_converted_order)

    def return_reserve_target_base_ccy(self, base_ccy_target):
        local_ccy_converted_target = self.fx_convert(base_ccy_target)
        return self.return_reserve_target(local_ccy_converted_target)

    def return_reserve_order(self, order):
        non_tradable_ticker = self.current_data < 0.01
        temp_order = (order / self.current_data)
        temp_order[non_tradable_ticker] = 0
        return temp_order

    def return_reserve_target(self, target):
        non_tradable_ticker = self.current_data < 0.01
        temp_target = (target / self.current_data) * (~non_tradable_ticker) \
                      + self.portfolio_manager.current_holding * non_tradable_ticker
        return self.return_target(temp_target)

    def return_target(self, target):
        return target - self.portfolio_manager.current_holding
``` 

#### Attributes `on_data` can refer to

There are quite a few attributes `on_data()` can use. They are mostly *static* as they don't change with the
update of the timestamps. For example, a strategy can easily look up symbols that align with the price data 
in *data_dict*.
 - `self.strategy_name`: Strategy name (str)
 - `self.strategy_param`: Strategy parameters (array of float)
 - `self.metadata`: Tons of useful static information such as symbols, fields, currency, etc. This is the 
                    most important attribute. See details below.
 - `self.portfolio_manager`: To access PMS of the strategy (class instance)
 - `self.id_mapping`: The mapping that deals with the *[ID misalignment problem](#handle-id-misalignment-when-flatten-is-set-true)*
                      of Level-1 engine when `flatten=True`.
 - `self.ccy_matrix`: The binary matrix that marks the trading currency of each symbol.

Note that the above attributes together with the attributes below are reserved by `BaseStrategy` class. **You 
should avoid overlapping these names when you create a new `self.xxx` attribute in your strategy. In addition, 
DO NOT alter the data inside the reserved attributes.**
 - `self.status` -> reserved name
 - `self.signal_remark` -> reserved name
 - `self.timestamp` -> reserved name
 - `self.backup_symbols` -> reserved name

`self.metadata` has rigorous data structure: It is a two-layer nested dictionary. The value of the innermost 
dict is a list of strings. The following table shows all the keys you can use to fetch various metadata.

| Key 1             | Key 2             | Meaning of the list of strings                                  |
| :---------------- | :---------------- | :-------------------------------------------------------------- |
| 'main'            | 'symbols'         | Symbols of `data_dict['main']` -> Dim-2                         |
| 'main'            | 'fields'          | Field names of `data_dict['main']` -> Dim-3                     |
| 'main'            | 'symbol_types'    | `CN_STOCK` etc. associate with symbols in `data_dict['main']`   |
| 'main'            | 'ccy_list'        | Trading currency of each symbol in `data_dict['main']`          |
| 'remark'          | 'fields'          | Column header of your signal remarks                           |
| 'your_ref_data_1' | 'symbols'         | Symbols of `ref_data_dict['your_ref_data_1']` -> Dim-2          |
| 'your_ref_data_1' | 'fields'          | Field names of `ref_data_dict['your_ref_data_1']` -> Dim-3      |
| 'your_ref_data_2' | 'symbols'         | Symbols of `ref_data_dict['your_ref_data_2']` -> Dim-2          |
| 'your_ref_data_2' | 'fields'          | Field names of `ref_data_dict['your_ref_data_2']` -> Dim-3      |
| ...               | ...               | ...                                                             |
| '0'               | 'RSI'             | Symbols of the secondary data `RSI` at secondary ID 0           |
| '0'               | 'KD'              | Symbols of the secondary data `KD` at secondary ID 0            |
| '1'               | 'RSI'             | Symbols of the secondary data `RSI` at secondary ID 1           |
| '1'               | 'KD'              | Symbols of the secondary data `KD` at secondary ID 1            |
| ...               | ...               | ...                                                             |
| 'masked_secondary_ids' | 'RSI'        | `RSI` IDs your strategy pool subscribed for this strategy       |
| 'masked_secondary_ids' | 'KD'         | `KD` IDs your strategy pool subscribed for this strategy        |

 - For example, to get the symbol list of the main data, simply put `self.metadata['main']['symbols']`.
 
Note that `self.metadata` does not show keys if the data is not subscribed. That is, if you don't have 
secondary data in your engine config, there are no keys like '0', '1', ..., in `self.metadata`.

Things are a bit more complicated if some of your strategies defined in the strategy pool subscribe to
a subset of secondary data IDs. `self.metadata['masked_secondary_ids']` is here to help the lookup.
To avoid this complication, just keep symbols the same for all strategies in your strategy pool.

#### Mixed asset classes

If your strategy pool has assets in multiple exchanges, or say two asset classes as main data, be aware 
that you may only trade in one exchange at a time. This is because the timestamps of all data points in 
one exchange may not align with another exchange.

Let's review a simple case: Two CN stocks and two HK stocks **traded daily**. We show close prices:

| datetime             | 1299.HK   | 2388.HK   | 600016.SH  | 601318.SH  |
| :------------------- | --------: | --------: | ---------: | ---------: |
| 2010-10-12 06:57 UTC | nan       | nan       | 5.3        | 58.51      |
| 2010-10-12 08:00 UTC | nan       | 25.65     | nan        | nan        |
| 2010-10-13 06:57 UTC | nan       | nan       | 5.4        | 59.3       |
| 2010-10-13 08:00 UTC | nan       | 25.9      | nan        | nan        |
| ...                  | ...       | ...       | ...        | ...        |

 - There are two timestamps everyday as CN stocks close at 06:57 UTC while HK stocks close at 08:00 UTC.
 - At 06:57 UTC, you can only trade CN stocks as prices of HK stocks are *nan*.
 - At 08:00 UTC, you can only trade HK stocks as prices of CN stocks are *nan*.
 - 1299.HK (AIA Group Ltd) has *nan* all along because the symbol was not listed in the exchange at the 
   time.

**TIPS:** If your strategy sends an order on symbols with *nan* prices, the symbols will be discarded. But 
if the order contains symbols with regular prices as well, these symbols can be traded as usual. This 
means you don't have to worry if you send invalid orders on symbols having no prices.

If you mix a group of intraday assets with daily assets as main data, the timestamps your `on_data()` see
are the union of timestamps in all assets. In this case, the daily assets can only be traded at their 
exchange's closing time of the day. They cannot be traded in many other hours simply because there are no
price data in these hours, indicated by *nan* prices.

**`ex_tradable`**

You can use `data_dict['ex_tradable']` to fetch the exchange tradable status. It shows whether a symbol 
is tradable at the time per the exchange's rule.

#### Status loading and saving

`save_status` and `load_status` work in backtesting. They are used for strategy to load/dump status
calculated during backtesting.

Currently status loading and saving are not implemented in live trading.


### Inspecting the Results

Engines in each level writes the results to HDF5 files specified in engines' json-format configurations. 
For example, you may find lines look similar to the following:

```
  "result_output": {
    "flatten": false,
    "save_dir": "results/RSI_strategy/result/",
    "save_name": "rsi_lv1",
  }
```

This indicates the paths of result files should start with `results/RSI_strategy/result/rsi_lv1`.
Depending on whether you have enabled parallel HDF5 output, you may find one or more pairs of files
such as the pair `rsi_lv1.hdf5` and `rsi_lv1.keys`, or

```
rsi_lv1-p0.hdf5
rsi_lv1-p0.keys
rsi_lv1-p1.hdf5
rsi_lv1-p1.keys
...
```

In order to inspect these results in a developer-friendly way, we provide a tool in *result exporter*
module:

```python
from quantcycle.app.result_exporter.result_reader import ResultReader

# Just specify the path indicated by the json configuration instead of 
# the path of an actual file.
reader = ResultReader('results/RSI_strategy/result/rsi_lv1')    # note the path

# First, you may want a summary of all strategies
summary_file = r'results/csv/rsi_lv1_summary.csv'
reader.export_summary(summary_file)

# Or, a summary of just some strategies
reader.export_summary(summary_file, [0, 1, 2])

# To inspect every strategy, the following method converts the strategy results 
# to csv files. In this way, each strategy has a seperate folder.
output_dir = r'results/csv/rsi_lv1'
reader.to_csv(output_dir)

# Or, you can filter the output by a list of strategy IDs or time duration, for
# example:
start_time = 1356912000
end_time = 1356982000
reader.to_csv(output_dir, [0, 1, 2], (start_time, end_time))
```

Alternatively, you can also use the following interfaces provided by *[ResultReader](quantcycle/app/result_exporter/result_reader.py)* 
to read the data from HDF5 directly:

```python
def get_strategy(self, id_list, fields: list, start_end_time=None, phase=None, df_disable=False):
    """
    Get historical fields, i.e. position, pnl, and cost by strategy IDs.
    :param id_list: A list of strategy_id to request
    :param fields: A list of field names, ['position', 'pnl', 'cost'] supported
    :param start_end_time: (Optional) Truncate time by (start, end)
    :param phase: (Optional) A list of time field filters, each is a string
    :param df_disable: True/False. Set True for much better performance.
    :return: For performance, explicitly set df_disable True. Return a dictionary {strategy_id: [list of results]}.
             The order of results in the list matches the order of requested fields. Each result is a tuple
             (np_array, row_indices, column_indices). By default, df_disable=False, each result is a DataFrame.
    """

def get_attr(self, id_list, fields: list):
    """
    Get non-historical attributes, i.e. strategy_params, ref_aum, and symbols by strategy IDs.
    :param id_list: A list of strategy_id to request
    :param fields: A list of field names, ['params', 'ref_aum', 'symbols'] supported
    :return: A DataFrame with requested fields as columns
    """
```

#### Order checker log

The engine may reject orders that violate certain rules. It is good to have a look at the order checker 
log after each run. The log is put in the same directory specified by `save_dir` in *[result export config](#result-export)*.
There is a file named with `_order.log`. Just open it. It is a text file.

```
1286866620 2010-10-12 06:57:00 UTC   0: ST ST -- --
1286870400 2010-10-12 08:00:00 UTC   0: NA -- ST ST
1286953020 2010-10-13 06:57:00 UTC   0: ST ST -- --
1286956800 2010-10-13 08:00:00 UTC   0: NA -- ST ST
1287039420 2010-10-14 06:57:00 UTC   0: ST ST -- --
1287043200 2010-10-14 08:00:00 UTC   0: NA -- ST ST
1287125820 2010-10-15 06:57:00 UTC   0: ST ST -- --
1287129600 2010-10-15 08:00:00 UTC   0: NA -- ST ST
1287471420 2010-10-19 06:57:00 UTC   0: NC NC NC NC
```

First, the file is empty if no orders are checked, otherwise you will see something like the above. 
The format is `{timestamp} {date_time} {strategy_id}: {order_status}`. We break down the meaning of
the order status. Every column corresponds to a symbol in `self.metadata['main']['symbols']`.
 - `--`: The order is OK
 - `NC`: Not enough cash
 - `ST`: Stop trade---the symbol is not tradable at this time
 - `SB`: Stop buy---the upper limit of the price range is hit
 - `SS`: Stop sell---the lower limit of the price range is hit
 - `LS`: Larger than sell limit
 - `NA`: The order you placed has *nan* value

**It is worth noting that**
 - Perhaps a row with `NC` needs your further inspection.
 - Not all symbols in the log are necessarily checked. Those marked with `--` are passed.
 - Not all records need to be worried. In multi-exchanges scenario, it is normal to see many rows 
   regularly alternating `ST` and `--`. Just like the example shown above.


### Market Data Input by Local CSV 

Market data can be either downloaded automatically from DataMaster or picked up from local CSV storage.
This scenario works for both *main data* and *reference data*.

If you want to feed the market data by local storage, prepare the following four folders similar to the
file structure below:

  ```bash
  local csv
    ├── data
    │   ├── symbol_1.csv
    │   ├── symbol_2.csv
    │   ├── ...
    │
    ├── fxrate
    │   ├── symbol_1.csv
    │   ├── symbol_2.csv
    │   ├── ...
    │
    ├── interest
    │   ├── symbol_1.csv
    │   ├── symbol_2.csv
    │   ├── ...
    │
    └── info
        └── info.csv
  ```  

Between, `info.csv` contains metadata of the symbols. The other three folders are used to mock a 
market data source. Check our *[example folder](linked_examples/csv_data)*. Then, feed the engine 
configuration (json) when you start the engine. All the other fields are same as usual except in the
`data` field, you need to point the corresponding folders to the engine.
 - `main_dir`: Price data. **Columns of OHLC prices are required if the data is used as *main data* 
               for trading.** There is no such limitation for *reference data* however.
 - `fxrate_dir`: FX rates for currency conversion. Inside, a column named "fx_rate" is required.
 - `int_dir`: Interest rates or dividends. Inside, a column named "interest_rate" is required.
 - `info`: The path to `info.csv`.

by following the example below:
```
  "data": {
    "STOCKS": {
      "DataCenter": "LocalCSV",
      "DataCenterArgs": {
        "main_dir": "examples/data/csv_data/stock_data/",
        "fxrate_dir": "examples/data/csv_data/fxrate/",
        "int_dir": "examples/data/csv_data/interest/",
        "info": "examples/data/csv_data/info/info.csv"
      },
      "Fields": "OHLC",
      "Frequency": "DAILY"
    }
  },
```

 - `Fields`: According to what purpose this local CSV is used for, for *main data*, "OHLC" is 
             required. For *reference data*, `Fields` can be defined by the user as long as they 
             are available in the data. The value accepts a name string such as `"OHLC"` or a list, 
             such as `["open", "close", "volume"]`.

#### Omitting fx rates or interest rates

To avoid the headache in preparation of the rates for every symbol, you may configure the engine to 
fetch the rates from DataMaster instead. In this case, `fxrate_dir` and `int_dir` can be omitted.
First, simply remove these two lines in `DataCenterArgs` in the above example. Next, `info.csv` needs 
to be updated by adding three columns titled `MAIN`, `CCPFXRATE`, and `INT`. Corresponding to the rate 
directories you removed, fill the column by "datamaster".

Check the updated *[info_main_only.csv](linked_examples/csv_data/info/info_main_only.csv)* in comparison 
to the complete *[info.csv](linked_examples/csv_data/info/info.csv)* for this manipulation.


### Reference Data

In many cases the strategy would like to refer to some time-series data for calculation. It doesn't trade
the underlying assets of these data. We provide an interface called *reference data* to handle this 
situation.

The design of the reference data is very flexible. It can be any time-series data in any frequencies. Even
quarterly data can work. Basically, the reference data is a CSV table containing the following columns:
 - The first column must be the datetime such as `2018-09-14 05:00:00`, UTC by default. The timezone code 
   can also be supplied, such as `2018-09-14 13:00:00+08:00` which is equivalent to 05:00:00 UTC.
 - Other columns can be defined by the user, but the value must be numbers in *float*.

An hourly data example is given below:

```
datetime,open,high,low,close
2018-09-14 05:00:00+00:00,0.9367,0.9371,0.9361,0.9362
2018-09-14 06:00:00+00:00,0.9362,0.9363,0.935,0.9352
2018-09-14 07:00:00+00:00,0.9352,0.9354,0.9341,0.935
2018-09-14 08:00:00+00:00,0.935,0.9352,0.9338,0.9339
2018-09-17 05:00:00+00:00,0.9346,0.9348,0.9342,0.9346
2018-09-17 06:00:00+00:00,0.9345,0.9347,0.934,0.9341
2018-09-17 07:00:00+00:00,0.9341,0.9351,0.9337,0.9349
2018-09-17 08:00:00+00:00,0.9349,0.9355,0.934,0.9352
2018-09-18 05:00:00+00:00,0.9385,0.9394,0.9379,0.9379
2018-09-18 06:00:00+00:00,0.9379,0.9386,0.9377,0.9383
2018-09-18 07:00:00+00:00,0.9383,0.9387,0.9378,0.9382
2018-09-18 08:00:00+00:00,0.9382,0.9383,0.9359,0.9361
```

Unlike the *[market data for trading](#market-data-input-by-local-csv)* where OHLC columns are required, 
reference data can have arbitrary columns. Note that it is allowed to skip some hours or even days too. 
Each symbol has a CSV table like this. Put all the CSV tables in one folder. Your next step is to feed 
this folder to the engine by adding `ref_data` to the engine configuration. The following json config 
example assumes that you have put STOCK_1.csv, STOCK_2.csv, and STOCK_3.csv in "dummy_stock_hourly" 
folder.

```
  "ref_data": {
    "ref_hourly": {
      "DataCenter": "LocalCSV",
      "DataCenterArgs": {
        "dir": "dummy_stock_hourly"
      },
      "Symbol": ["STOCK_1", "STOCK_2", "STOCK_3"],
      "StartDate": {
        "Year": 2018,
        "Month": 9,
        "Day": 12
      },
      "EndDate": {
        "Year": 2019,
        "Month": 1,
        "Day": 10
      },
      "Frequency": "HOURLY"
    }
  }
```

Between, 
 - `ref_hourly` (under `ref_data`) is the key name defined by you. Your strategy will be 
   able to refer to this data in an `on_data()` loop by using `ref_data_dict['ref_hourly']`.
 - `StartDate` and `EndDate`: These are handy if you want to filter the date range. They are often 
   omitted though. Once omitted, the engine will apply the same date range of the main data.
 - `Frequency` is used to calculate the date range for data request. If your data does not have a
   regular frequency, just provide the closest one among `DAILY`, `HOURLY`, `MINUTELY` and `SECONDLY`.
   Sometimes, you only want to read a few data columns. The optional `Fields` key can be helpful:

```
  "ref_data": {
    "ref_hourly": {
      ...
      "Fields": ["open", "close"]
      ...
    }
  }
```

You can feed the engine with multiple reference data sources in various time frequencies. An example 
config can be found *[here](linked_examples/stock_hourly_with_ref.json)*.

Reference data can have different window size from the main data. Set the window sizes in the engine
config. This is useful when you have both daily and hourly data. You may want to extend the window of
hourly data so that it matches the time span of the daily window.

```
  "algo": {
    "base_ccy": "LOCAL",
    "window_size": {
      "main": 3,
      "ref_data": 21
    }
```

#### Tips for hourly trading watching daily reference data

One use case worth noticing is when you trade hourly but refer to daily data. In trading hours, your 
strategy shouldn't have the daily information prior to the market close. In this case, you have 
to make sure the timestamps in your daily reference data align correctly with the time of the market 
close. For example, if the market closes at 08:00:00 UTC, you have to set each of your daily timestamps 
to at 08:00:00 UTC when the data contains high, low, close price for reference.

```
datetime,open,high,low,close
2017-12-11 08:00:00 UTC,4.1,4.17,4.09,4.17
2017-12-12 08:00:00 UTC,4.18,4.25,4.12,4.22
2017-12-13 08:00:00 UTC,4.23,4.23,4.17,4.19
2017-12-14 08:00:00 UTC,4.2,4.27,4.16,4.23
...
```

On the other hand, for the open price to be viewed right after the market open, you have two ways to implement. 
Choose either:
 - **Method 1:** Copy all the CSVs to another folder, align all the timestamps with the time of the market open
             every day, add the data as another reference data in the engine config, use `Fields` key to only 
             select the "open" column.
 - **Method 2:** Further craft the daily reference data by duplicating each row, set the timestamp for one of 
             the dups to the time of the market open, another to market close. For the market open one,
             mask the high, low, and close price by `nan`.

#### Fetch reference data from DataMaster

A user can alternatively configure the engine to download reference data from DataMaster. The configuration
is similar to *[requesting the main data from DataMaster](#data-sources)*. For example,

```
  "ref_data": {
    ...
    "futures_adjusted": {
      "DataCenter": "DataMaster",
      "Symbol": ["1603000558", "1603000563", "1603000557", "1603000562"]
      "SymbolArgs": {
        "DMAdjustType": 1,
        "AccountCurrency": "USD"
      },
      "Fields": ["OHLC"],
      "Frequency": "DAILY"
    },
    ...
  }
```

It is more flexible than requesting the main data though:
 - `futures_adjusted`: The group name of this reference data---you can name it yourself and later refer to 
   this group by `ref_data_dict['futures_adjusted']` in an `on_data()` loop.
 - `Symbol`: Unlike the main data where symbols are defined by strategy pool settings, in reference data
   you can specify symbols in the json configuration directly.
 - **TIPS:** The `Symbol` key can be omitted though. In this case, the engine will use the main data symbols 
   defined in the strategy pool. For multi-assets, you may assign symbols from a main data group. For 
   example, you can simply put symbols of `STOCKS` group by `"Symbol": "STOCKS"` instead of painstakingly
   pasting the symbols one by one.
 - `Fields`: Unlike the main data where "OHLC" is required, you can specify any available fields here.
 - `StartDate` and `EndDate`: These are handy if you want to filter the date range. They are often omitted
   though. Once omitted, the engine will apply the same date range of the main data.


### Signal Remark

In an *[on_data() loop](#function-on_data)*, the strategy may want to save some remarks according to the
incoming data. The remarks can not only be read by the user later on, but also be fed into another 
`on_data()` process. Signal remark is designed to serve this purpose. It is worth mentioning that signal 
remarks are *time-series data*, so they fit the logic of the main/reference data when loaded.

To save the remarks, you must first set up the engine so it knows where to save them. For example, in the
engine configuration,

```
  "signal_remark": {
    "save_dir": "results/test_stock/remarks/",
    "save_name": "remark_RSI_lv1"
  },
```

In the `on_data()` function of your strategy, save your remarks:

```python
def on_data(self, data_dict: dict, time_dict: dict, ref_data_dict: dict, ref_time_dict: dict):
    # Signal remark
    remark = Dict.empty(types.unicode_type, nb.float64)
    ...
    remark['remark_message'] = 0.1      # the value is float64
    self.save_signal_remark(remark)
```

Now, you may find several remark files in your specified output folder.

```
remark_RSI_lv1-0.csv
remark_RSI_lv1-0.fields
remark_RSI_lv1-1.csv
remark_RSI_lv1-1.fields
...
```

Between, the trailing `-0`, `-1`, etc., represent the IDs that bind to each set of strategy parameters. 
In other words, the number of IDs there are equal to the number of `on_data()` processes. Then, you can 
load the remarks by another engine. For example, in a Level-2 engine config file, define:

```
  "signal_remark": {
    "load_dir": "results/test_stock/remarks/",
    "load_name": "remark_RSI_lv1"
  },
```

To read the remarks in the Level-2 engine's `on_data()`, use `ref_data_dict['signal_remark']` and
`ref_time_dict['signal_remark']`.
 - `ref_data_dict['signal_remark']` is a 3D array. The first dimension is the window, the second dimension 
    is strategies, and the third is the user-defined columns (fields). This definition is similar to other 
    time-series data except that the second dimension corresponds to one or more CSV files in the `load_dir` 
    while usually the second dimension should mean symbols.
 - To find the user-defined field names, `self.remark_field_names` is handy.

We provide a good example of signal remark that *[marks the calendar for certain months and years](examples/backtest_signal_remark.py)*.

#### Handle ID misalignment when `flatten` is set true

In the situation when `flatten` is enabled in *[result export](#result-export)*, the ID of the secondary
data, such as pnl, does not align with the ID of the saved signal remarks. If your engine input has both 
secondary data and signal remarks, you need to pay attention to the retrieval of the correct ID.

To help, we provide a function in *BaseStrategy* to ease ID alignment. Assuming you always use the 
**post-flattening** ID to retrieve secondary and remarks data, call the following function to fetch the 
correct remarks rather than access the 3D array by yourself.

```python
flattened_id = 20   # the ID after flattening
remark_array = self.get_remark(ref_data_dict['signal_remark'], flattened_id)
```

 - `self.get_remark()` returns a 2D array, with its 1st-dim being the window, 2nd-dim being user-defined
   columns (fields). This function would still work flawlessly if `flatten` is false. In this way, calling 
   the function is identical to accessing `ref_data_dict['signal_remark'][:, id, :]` directly. **Note that 
   you need to add at least one group of secondary data to make `self.get_remark()` functional.**


### Demo

We provide *[examples](../examples)* covering most functions mentioned in this manual for quick learning.


### Functions in Development

 - For equities, we now support CN stocks (SH/SZ), HK stocks, HK ETFs and US ETFs. However, DataMaster 
   currently has a problem handling US ETFs---they can only be requested separately from other stocks. 
   Stocks in other region, such as US stocks, are yet to be included by DataMaster. Anyway, you can always 
   manipulate input by local CSV and reference data.
 - Support a mix of data sources (DataMaster/local CSV) within one asset class, e.g. `STOCKS`.


<!---### Known Issues
 - Timestamps may not be aligned correctly if your main data is the daily data downloaded from DataMaster 
   and reference data is any data in higher frequency, such as hourly data. This will be fixed after
   multi-exchanges/mixed-assets support.-->