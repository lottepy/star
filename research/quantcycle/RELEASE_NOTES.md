> ## V2.1.2 (2020-12-23) 
> 
> #### Bug Fix
> - Data manager removed DataMaster symbol matching when local CSV is used. As a result,
>   there is no need to escape the same symbols available in DataMaster when you specify
>   the data to be input purely from local CSV.


> ## V2.1.1 (2020-12-10)
>
> #### Bug Fix
> - Data manager fix: *nan* may show up in *fx rate* and *interest rate* tables using 
>   main data loaded from Local CSV. It happens in CSV data concatenation when a symbol
>   misses a timestamp that other symbols have.
> - This bug is not critical. An update is recommended to the affected users.


> ## V2.1.0 (2020-12-01)
>
> #### Added Functions
> - [Order checker log](docs/manual_v2.md#order-checker-log): To see if any orders were 
>   rejected and for what reason.
> - Support zero and negative futures price.
>
> #### Notable Changes
> - The obsolete `load_info()` in Level-2 and Level-3 strategies is removed. To get the 
>   same information, please read through the *[metadata attribute](docs/manual_v2.md#attributes-on_data-can-refer-to)*.
>
> #### Bug Fixes
> - A hidden bug in loading secondary data symbols on Level 3 is fixed.


> ## V2.0.2 (2020-11-24)
>
> #### Added Function
> - For usability, symbols of a main data group can be assigned to reference data. Refer to
>   *[TIPS](docs/manual_v2.md#fetch-reference-data-from-datamaster)*.
>
> #### Bug Fixes
> - Data manager fix: A bug affects multi-assets in Level-2 run when "INFO" data mistakenly 
>   concatenates secondary data.


> ## V2.0.1 (2020-11-19)
>
> #### Added Function
> - Production engine can push DingDing msg.


> ## V2.0.0 (2020-11-18)
>
> #### Added Functions
> - **A BIG DEAL:** Support mixed asset classes (FX/STOCKS/FUTURES) in backtesting.
>   - First, walk through [this section](docs/manual_v2.md#mixed-asset-classes) to learn 
>     the changes it brings to your strategy.
>   - Second, review *[data source configuration](docs/manual_v2.md#data-sources)* to 
>     set it up.
>
> #### Notable Changes to Your Existing Code
> - The `main_data` key is removed in engine configuration.
>   - Now, the *main data*, *secondary data*, and *reference data* are well defined.
>   - It is no longer acceptable to put the Level-1 results, such as `RSI_lv1`, in the 
>     main data's setting. Instead, you should put that into the secondary data's setting.
>     Check [this example](docs/linked_examples/RSI_lv2.json) for correction.
> - The attributes of `BaseStrategy` are reconstructed. If your strategy refers to 
>   something like `self.symbol_batch` previously, please expect broken links.
>   - For this example, simply replace `self.symbol_batch` by `self.metadata['main']['symbols']`.
>   - Follow [this section](docs/manual_v2.md#attributes-on_data-can-refer-to) to see all 
>     the attributes you can use in `on_data()`.
>
> #### Added Live Trading Modules
> - Production engine setup for live trading
>   - User can fire config and strategy pool csv used from backtest to production server.
>   - Control action with production server. See [README.md](README.md#production-run).
>   - Production server will automatically receive data and fire order to broker. See 
>     *[live trading engine pointer](docs/manual_v2.md#live-trading-engine-pointer)* and 
>     *[live trading order router](docs/manual_v2.md#live-trading-order-router)*.
>
> #### Bug Fixes
> - Python multiprocessing fails when the number of strategies is less than workers.
>   - This is expected because multiprocessing is implemented by distributing strategies.
>   - To reduce confusion, the number of workers will be reset to the number of strategies
>     in this situation so that you can always use `"best"` setting.
> - The backtesting may fail on a Level-2 engine if it takes `flatten=False` results from 
>   Level-1 engines.
>   - This is due to a hidden bug in secondary data masking.


> ## V1.4.1 (2020-11-02)
>
> #### Bug Fix
> - FIXED: The backtesting engine did not take the commission fee if it is given in a 
>   single number in engine config. *(Critical)*


> ## V1.4.0 (2020-10-27)
>
> #### Added Functions
> - Backtesting engine supports Python multiprocessing as an alternative if it is not 
>   possible for the strategy to work under Numba environment.
>   - To set it up, follow [speed optimization in engine config](docs/manual_v2.md#speed-optimization). 
>   - We strongly recommend you to also review [all speed boost options](docs/manual_v2.md#speed-boost-in-backtesting).
>
> #### Bug Fixes
> - [A helper method](docs/manual_v2.md#handle-id-misalignment-when-flatten-is-set-true) 
>   is provided for signal remark to handle ID misalignment when `flatten` is true.


> ## V1.3.0 (2020-10-19)
>
> #### Added Functions
> - Futures support
>   - Currently, futures prices must be positive.
> - [Signal remark](docs/manual_v2.md#signal-remark)
> - Reference data now supports both local CSV and [DataMaster's remote data](docs/manual_v2.md#fetch-reference-data-from-datamaster).
>
> #### Notable Changes
> - `on_data()` no longer shows data names with empty data except for the main data.
>   - For example, `if time_dict[long_metrics_name].shape[0] != 0:` will not work because
>     when `shape[0] == 0`, the key `long_metrics_name` does not exist. You should replace
>     the condition by `if long_metrics_name in time_dict:`.
>
> #### Other Improvements
> - Backtesting engine's code has been significantly beautified. Most hard-coded parts were 
>   removed.
>
> #### Bug Fixes
> - Start and end dates configured in reference data now behave the same as those in the
>   main data.
> - Better logic when handling `OHLC` fields and user requested data fields.
> - FIXED: Some secondary data fields such as metrics were hard coded previously.
> - `base_ccy` in engine configuration is favored. If you have conflict `AccountCurrency` in 
>   data source configuration, `base_ccy` will override the setting.


> ## V1.2.0 (2020-09-25)
>
> #### Added Functions
> - Intraday support---hourly, minutely
> - [Reference data](docs/manual_v2.md#reference-data):
>   - A user can feed the engine with any time-series data from local CSV for 
>     reference by strategies.
>   - Note that `on_data()` now takes [4 function arguments](docs/manual_v2.md#function-on_data)
>     as input.
> - [Local CSV improvement](docs/manual_v2.md#omitting-fx-rates-or-interest-rates):
>   - Either `fxrate_dir` or `int_dir` is optional now. Their data can be fetched from
>     data master instead.
> - Multi-currency support:
>   - The account's base currency can be any of `LOCAL`/`USD`/`CNY`/`HKD`.
>   - All assets' trading currencies are supported.
> - Strategies can now [see *fx rates* and *interest rates*](docs/manual_v2.md#function-on_data) 
>   other than the main data in the `on_data()` loop.
> - Live trading engine supports BT150 as a backup data source.
> - Live trading can be triggered by messages rather than by schedule.
>
> #### Bug Fixes
> - Level-2 strategies can get symbols of the secondary data in `on_data()`.
> - Fixed check order logic in PMS module.
>   - The pnl/positions in both backtesting and live trading may be affected.


> ## V1.1.1 (2020-09-01)
>
> #### Added Functions
> - Result reader can [export summary](docs/manual_v2.md#inspecting-the-results).
> - Result reader to_csv() shows human readable date time.
> - Backtesting engine supports [dm_pickle](docs/manual_v2.md#data-sources) to skip market
>   data downloading if the data has already been downloaded once.


> ## V1.1.0 (2020-08-27)
> 
> #### Added Functions
> - Equities support: CN and HK stocks, HK ETFs, US ETFs, mixed CN/HK stocks
> - Load market data from local CSV (alpha version)
>
> #### Bug Fixes
> - Window size in backtesting
> - The [fields](docs/manual_v2.md#function-on_data) `on_data()` can see are now 
> consistent in backtesting/live trading.