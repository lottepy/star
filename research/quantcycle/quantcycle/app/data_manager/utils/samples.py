# gives some samples of different things

sample_data_group = {
    "Label": "FX",
    "DataCenter": "DataMaster",
    "Symbol": ["AUDCAD", "USDTWD", "USDINR"],
    "SymbolArgs": {
        "DataSource": "BT150"
    },
    "Fields": "OHLC",
    "StartTS": 1577836800,
    "EndTS": 1579046400,
    "Frequency": "DAILY"
}

sample_data_bundle_INFO = {
    "Label": "FX/INFO",
    "Type": "Download",
    "DataCenter": "DataMaster",
    "Symbol": ["AUDCAD", "USDTWD", "USDINR"],
    "SymbolArgs": {
        "DataSource": "BT150"
    },
    "Fields": "INFO",
    "StartTS": 1577836800,
    "EndTS": 1579046400,
    "Frequency": "DAILY"
}

sample_data_bundle_MAIN = {
    "Label": "FX/MAIN",
    "Type": "Download",
    "DataCenter": "DataMaster",
    "Symbol": ["AUDCAD", "USDTWD", "USDINR"],
    "SymbolArgs": {
        "DataSource": "BT150"
    },
    "Fields": "OHLC",
    "StartTS": 1577836800,
    "EndTS": 1579046400,
    "Frequency": "DAILY"
}

sample_data_bundle_INT = {
    "Label": "FX/GEN",  # Gen for generated
    "Type": "Process",
    "DataCenter": "DataMaster",
    "Symbol": ["AUDCAD", "USDTWD", "USDINR"],
    "SymbolArgs": {
        "DataSource": "BT150"
    },
    "Fields": "INT",
    "StartTS": 1577836800,
    "EndTS": 1579046400,
    "Frequency": "DAILY"
}

# get data from local hdf5 using result reader
sample_data_bundle_RR = {
    "Label": "Strat/RR",
    "Type": "Download",
    "DataCenter": "ResultReader",
    "DataCenterArgs": {
        "DataPath": r'results/backtest_results_noflatten.hdf5'
    },
    "Symbol": [1, 2],
    "SymbolArgs": {
        "DataSource": "BT150"
    },
    "Fields": ['position', 'params', 'symbols', 'pnl'],
    "StartTS": 1577836800,
    "EndTS": 1579046400,
    "Frequency": "DAILY"
}

################################ for localCSV ################################
sample_data_bundle_csv_proxy = {
    "Label": 'ABC-CLASS/VARIABLE',
    "Type": "Download",
    "DataCenter": 'LocalCSV',
    "DataCenterArgs": {
        'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
        'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data/",  # dir path must use "/" please
        'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
    },
    "Symbol": ['coinA', 'coinB'],
    "SymbolArgs": {"DataSource": 'coin_source_from_config'},
    "Slot": "data",
    "Fields": 'OHLC',
    "StartTS": 1569801600,
    "EndTS": 1569974400,
    "Frequency": 'DAILY'
}

sample_data_bundle_csv_proxy_hourly = {
    "Label": 'ABC-CLASS/VARIABLE',
    "Type": "Download",
    "DataCenter": 'LocalCSV',
    "DataCenterArgs": {
        'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
        'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data_hourly/",  # dir path must use "/" please
        'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
    },
    "Symbol": ['coinA', 'coinB'],
    "SymbolArgs": {"DataSource": 'coin_source_from_config'},
    "Slot": "data",
    "Fields": 'OHLC',
    "StartTS": 1569801600,
    "EndTS": 1569974400,
    "Frequency": 'DAILY'
}

sample_data_bundle_csv_proxy_no_coinB_info = {
    "Label": 'ABC-CLASS/VARIABLE',
    "Type": "Download",
    "DataCenter": 'LocalCSV',
    "DataCenterArgs": {
        'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
        'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data/",  # dir path must use "/" please
        'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_no_coinB.csv" # please use "info" key for information
    },
    "Symbol": ['coinA', 'coinB'],
    "SymbolArgs": {"DataSource": 'coin_source_from_config'},
    "Slot": "data",
    "Fields": 'OHLC',
    "StartTS": 1569801600,
    "EndTS": 1569974400,
    "Frequency": 'DAILY'
}

sample_data_bundle_csv_proxy_no_coinB_info_no_symbols_list = {
    "Label": 'ABC-CLASS/VARIABLE',
    "Type": "Download",
    "DataCenter": 'LocalCSV',
    "DataCenterArgs": {
        'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
        'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data/",  # dir path must use "/" please
        'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_no_coinB.csv" # please use "info" key for information
    },
    "Symbol": [],
    "SymbolArgs": {"DataSource": 'coin_source_from_config'},
    "Slot": "data",
    "Fields": 'OHLC',
    "StartTS": 1569801600,
    "EndTS": 1569974400,
    "Frequency": 'DAILY'
}

sample_data_bundle_csv_no_field = {
    "Label": 'ABC-CLASS/VARIABLE',
    "Type": "Download",
    "DataCenter": 'LocalCSV',
    "DataCenterArgs": { 
        'key_name': 'ABC-FX/CCPFXRATE', # optional, if no, default key "{Label}-{folder_name}"
        'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fxrate/",  # dir path must use "/" please
        'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
    },
    "Symbol": ['AUDCAD'],
    "SymbolArgs": {"DataSource": 'coin_source_from_config'},
    "Slot": "data",
    "Fields": [],
    "StartTS": 1569801600,
    "EndTS": 1569974400,
    "Frequency": 'DAILY'
}

sample_data_bundle_csv_no_info = {
    "Label": 'ABC-CLASS/VARIABLE',
    "Type": "Download",
    "DataCenter": 'LocalCSV',
    "DataCenterArgs": { 
        'key_name': 'ABC-FX/CCPFXRATE', # optional, if no, default key "{Label}-{folder_name}"
        'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fxrate/"  # dir path must use "/" please
        # 'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
    },
    "Symbol": ['AUDCAD'],
    "SymbolArgs": {"DataSource": 'coin_source_from_config'},
    "Slot": "data",
    "Fields": ['close'],
    "StartTS": 1569801600,
    "EndTS": 1569974400,
    "Frequency": 'DAILY'
}

sample_data_bundle_fx_rate_csv_to_symbol = {
    "Label": 'ABC-CLASS/VARIABLE',
    "Type": "Download",
    "DataCenter": 'LocalCSV',
    "DataCenterArgs": {
        'key_name': 'ABC-FX/CCPFXRATE', # optional, if no, default key "{Label}-{folder_name}"
        'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fxrate/",  # dir path must use "/" please
        'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv", # please use "info" key for information
        'fx2sym': True
    },
    "Symbol": ['AUDCAD', 'AUDJPY'],
    "SymbolArgs": {"DataSource": 'coin_source_from_config'},
    "Slot": "data",
    "Fields": 'OHLC',
    "StartTS": 1569801600,
    "EndTS": 1569974400,
    "Frequency": 'DAILY'
}
##############################################################################
