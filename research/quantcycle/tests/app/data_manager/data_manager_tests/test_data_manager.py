from unittest import TestCase, mock

import pytest
import numpy as np
import pandas as pd
import numpy.testing as npt

from quantcycle.app.data_manager.data_distributor.data_distributor_sub import DataDistributorSub
from quantcycle.app.data_manager import DataManager
from quantcycle.app.data_manager.utils.csv_import import get_csv_info_df

sample_config = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['AUDCAD', 'AUDJPY', 'AUDNZD', 'AUDUSD', 'CADCHF', 'CADJPY', 'EURCAD', 'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCAD', 'GBPCHF', 'GBPJPY', 'GBPUSD', 'NOKSEK', 'NZDJPY', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDIDR', 'USDINR', 'USDJPY', 'USDKRW', 'USDNOK', 'USDSEK', 'USDSGD', 'USDTHB', 'USDTWD'],
            "SymbolArgs": {
                "DataSource": "BT150"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_one_day = {
    'Data': {'DataGroup1': {'Label': 'FX',
                            'Type': 'Download',
                            'DataCenter': 'DataMaster',
                            'Symbol': ['AUDCAD', 'AUDJPY', 'AUDNZD', 'AUDUSD', 'CADCHF', 'CADJPY', 'EURCAD', 'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCAD', 'GBPCHF', 'GBPJPY', 'GBPUSD', 'NOKSEK', 'NZDJPY', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDIDR', 'USDINR', 'USDJPY', 'USDKRW', 'USDNOK', 'USDSEK', 'USDSGD', 'USDTHB', 'USDTWD'],
                            'SymbolArgs': {'DataSource': 'BGNL'},
                            'Fields': 'OHLC',
                            'StartDate': {'Year': 2012, 'Month': 1, 'Day': 2},
                            'EndDate': {'Year': 2012, 'Month': 1, 'Day': 2},
                            'Frequency': 'DAILY'}}}


sample_config_with_backup = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['AUDCAD', 'AUDJPY', 'AUDNZD', 'AUDUSD', 'CADCHF', 'CADJPY', 'EURCAD', 'EURCHF', 'EURGBP', 'EURJPY', 'EURUSD', 'GBPCAD', 'GBPCHF', 'GBPJPY', 'GBPUSD', 'NOKSEK', 'NZDJPY', 'NZDUSD', 'USDCAD', 'USDCHF', 'USDIDR', 'USDINR', 'USDJPY', 'USDKRW', 'USDNOK', 'USDSEK', 'USDSGD', 'USDTHB', 'USDTWD'],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_with_backup_one_day = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["USDTWD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 13
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 13
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_1d_noint = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["USDTWD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 6,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 6,
                "Day": 1
            },
            "Frequency": "DAILY",
            "Other": {
                "NoINT": True
            }
        }
    }
}

sample_config_for_pnl = {
    "Data": {
        "DataGroup1": {
            "Label": "Strat",
            "Type": "Download",
            "DataCenter": "ResultReader",
            "DataCenterArgs": {
                'DataPath':r"tests/app/data_manager/data_manager_tests/hdf5_files/RSI_lv1.hdf5"
            },
            "Symbol": 'ALL', #strat ID must be int
            "Fields": ['pnl','cost','ref_aum','params','symbols'],
            "StartDate": {
                "Year": 2018,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2019,
                "Month": 12,
                "Day": 1
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_for_metrics = {
    "Data": {
        "DataGroup1": {
            "Label": "ABC",
            "Type": "Download",
            "DataCenter": "ResultReader",
            "DataCenterArgs": {
                'DataPath':r"tests/app/data_manager/data_manager_tests/hdf5_files/RSI_lv1.hdf5"
            },
            "Symbol": 'ALL', #strat ID must be list of int
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
                "Year": 2018,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2019,
                "Month": 12,
                "Day": 1
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_for_empty_metrics = {
    "Data": {
        "DataGroup1": {
            "Label": "ABC",
            "Type": "Download",
            "DataCenter": "ResultReader",
            "DataCenterArgs": {
                'DataPath':r"tests/app/data_manager/data_manager_tests/hdf5_files/RSI_lv1.hdf5"
            },
            "Symbol": 'ALL', #strat ID must be list of int
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
                "Year": 2019,
                "Month": 12,
                "Day": 1
            },
            "EndDate": {
                "Year": 2019,
                "Month": 12,
                "Day": 1
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_for_select_pnltype = {
    "Data": {
        "DataGroup1": {
            "Label": "Strat",
            "Type": "Download",
            "DataCenter": "ResultReader",
            "DataCenterArgs": {
                'DataPath':r"tests/app/data_manager/data_manager_tests/hdf5_files/RSI_lv1.hdf5",
                'pnl_type':['order_feedback'],
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
    }
}

sample_config_for_save_load_pickle = {
    "dm_pickle":{
        "save_dir": "tests/app/data_manager/data_manager_tests/",
        "save_name": "FX_pickle_test",
        "to_pkl": True,
        "from_pkl": True
    },
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["USDTWD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "Frequency": "DAILY",
            "Other": {
                "NoINT": True
            }
        }
    }
}

sample_config_for_pickle_datamaster = {
    "dm_pickle":{
        "save_dir": "tests/app/data_manager/data_manager_tests/",
        "save_name": "FX_pickle_test",
        "to_pkl": False,
        "from_pkl": False,
        "pickle_datamaster": True,
        "load_datamaster": False
    },
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["USDTWD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "Frequency": "DAILY",
            "Other": {
                "NoINT": True
            }
        }
    }
}

sample_config_for_load_datamaster_pickle = {
    "dm_pickle":{
        "save_dir": "tests/app/data_manager/data_manager_tests/",
        "save_name": "FX_pickle_test",
        "to_pkl": False,
        "from_pkl": False,
        "pickle_datamaster": False,
        "load_datamaster": True
    },
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["USDTWD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "Frequency": "DAILY",
            "Other": {
                "NoINT": True
            }
        }
    }
}

sample_config_for_load_pickle = {
    "dm_pickle":{
        "save_dir": "tests/app/data_manager/data_manager_tests/",
        "save_name": "FX_pickle_test",
        "to_pkl": False,
        "from_pkl": True
    },
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["USDTWD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "Frequency": "DAILY",
            "Other": {
                "NoINT": True
            }
        }
    }
}

config_for_single_holiday = {
    "Data": {
        'DataGroup1': {'DataCenter': 'DataMaster', 'EndDate': {'Day': 30, 'Month': 12, 'Year': 2012}, 'Fields': 'OHLC', 'Frequency': 'DAILY', 'Label': 'FX', 'StartDate': {'Day': 30, 'Month': 12, 'Year': 2012}, 'Symbol': ['AUDCAD', 'AUDNZD', 'AUDJPY'], 'SymbolArgs': {'DataSource': 'BGNL'}, 'Type': 'Download'}
    } 
}

sample_config_for_info_only = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["USDTWD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL"
            },
            "Fields": "INFO",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_info_only_futures = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "Symbol": ["1603000567"],
            "SymbolArgs": {
            },
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data_hourly/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_futures.csv" # please use "info" key for information
            },
            "Fields": "INFO",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_tradable_table_only = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ', 'VTI.US','300084.SZ'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'HKD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "tradable_table",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_tradable_table_only_intraday = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ', 'VTI.US','300084.SZ'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'HKD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "tradable_table",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "HOURLY",
        }
    }
}
sample_config_for_tradable_table_only_time_check = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ', 'VTI.US','300084.SZ'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'HKD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "tradable_table",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate":  {'Year': 2012, 'Month': 1, 'Day': 5, 'Hour': 23, 'Minute': 59, 'Second': 59},
            "Frequency": "DAILY",
        }
    }
}
sample_config_for_tradable_table_only_FX = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['AUDCAD'],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL",
            },
            "Fields": "tradable_table",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_tradable_table_only_FX_intraday = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['AUDCAD'],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL",
            },
            "Fields": "tradable_table",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 2
            },
            "EndDate": {
                "Year": 2012,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "HOUR",
        }
    }
}
sample_config_for_backup_datamaster_with_csv = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ["USDTWD", "AUDCAD"],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL",
                'LOCAL_DATA': True,
                "LOCAL_DATA_DIR": "tests/app/data_manager/data_loader/data_center/data_center_tests"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2011,
                "Month": 12,
                "Day": 29
            },
            "EndDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 6
            },
            "Frequency": "DAILY",
        },
    }
}

sample_config_for_local_csv_proxy_hourly = {
    "Data": {
        "DataGroup2": {
            "Label": "MyStrat1",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data_hourly/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
            },
            "Symbol": [],
            "SymbolArgs": {
                "DataSource": "coin_source_from_config"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30,
                "Hour": 1,

            },
            "EndDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30,
                "Hour": 2,
            },
            "Frequency": "HOURLY",
        },
    }
}
sample_config_for_local_csv_proxy_hourly_2 = {
    "Data": {
        "DataGroup2": {
            "Label": "MyStrat1",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/US/Bundles/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/US/info/info.csv" # please use "info" key for information
            },
            # "Symbol": r"tests/app/data_manager/data_manager_tests/US/info/symbols.csv",
            "Symbol": [],
            "SymbolArgs": {
                # "DataSource": "coin_source_from_config"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2018,
                "Month": 9,
                "Day": 30,
                "Hour": 1,

            },
            "EndDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30,
                "Hour": 2,
            },
            "Frequency": "HOURLY",
        },
    }
}

sample_config_for_local_csv_proxy = {
    "Data": {
        "DataGroup2": {
            "Label": "MyStrat1",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
            },
            "Symbol": ['coinA', 'coinB'],
            "SymbolArgs": {
                "DataSource": "coin_source_from_config"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup3": {
            "Label": "MyStrat2",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-FX/INT', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_int/", 
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
            },
            "Symbol": [],
            "SymbolArgs": {
                "DataSource": "my_csv"
            },            
            "Fields": ['BID'],
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup4": {
            "Label": "MyStrat3",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-FX/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_data/", 
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
            },
            "Symbol": ['AUDUSD', 'AUDJPY'],
            "SymbolArgs": {
                "DataSource": "BT150"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup5": {
            "Label": "MyStrat4",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN2', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info.csv" # please use "info" key for information
            },
            "Symbol": [],
            "SymbolArgs": {
                "DataSource": "source_from_config"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_local_csv_proxy_no_fields = {
    "Data": {
        "DataGroup1": {
            "Label": "MyStrat4",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN2', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_no_coinB.csv" # please use "info" key for information
            },
            "Symbol": ['coinA'],
            "SymbolArgs": {
                "DataSource": "source_from_config"
            },
            "Fields": [],
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_local_csv_proxy_no_info_dir = {
    "Data": {
        "DataGroup1": {
            "Label": "MyStrat4",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-STOCK/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_data/",  # dir path must use "/" please
                # 'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_no_coinB.csv" # please use "info" key for information
            },
            "Symbol": ['JPStock'],
            "SymbolArgs": {
                "DataSource": "source_from_config"
            },
            "Fields": [],
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_local_csv_proxy_empty_infodf = {
    "Data": {
        "DataGroup1": {
            "Label": "MyStrat4",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-STOCK/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_data/",  # dir path must use "/" please
                'info': pd.DataFrame()
            },
            "Symbol": ['JPStock'],
            "SymbolArgs": {
                "DataSource": "source_from_config"
            },
            "Fields": [],
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_local_csv_proxy_csv_not_exist_settlement_open_hourly = {
    "Data": {
        "DataGroup1": {
            "Label": "MyStrat4",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data_hourly/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_no_coinB.csv" # please use "info" key for information
            },
            "Symbol": ['coinA','coinB'],
            "SymbolArgs": {
                "DataSource": "source_from_config",
                "SettlementType": "open"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 9
            },
            "Frequency": "HOURLY",
        }
    }
}

sample_config_for_local_csv_proxy_csv_not_exist_settlement_open = {
    "Data": {
        "DataGroup1": {
            "Label": "MyStrat4",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_no_coinB.csv" # please use "info" key for information
            },
            "Symbol": ['coinA','coinB'],
            "SymbolArgs": {
                "DataSource": "source_from_config",
                "SettlementType": "open"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_config_for_local_csv_proxy_csv_not_exist = {
    "Data": {
        "DataGroup1": {
            "Label": "MyStrat4",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-CRYPTO/MAIN2', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/asset_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_no_coinB.csv" # please use "info" key for information
            },
            "Symbol": ['coinA','coinB'],
            "SymbolArgs": {
                "DataSource": "source_from_config"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_FX_datamaster_data_backup_by_local_csv_proxy = {
    "Data": {
        "DataGroup0": {
            "Label": "ABC",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['EURCAD','AUDCAD','USDJPY'],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL",
                'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup1": {
            "Label": "MyStrat1",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-FX/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv" # please use "info" key for information
            },
            "Symbol": ['EURCAD','AUDCAD','USDJPY'],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL",
                'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup2": {
            "Label": "MyStrat2",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-FX/INT', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_int/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv" # please use "info" key for information
            },
            "Symbol": ['EURCAD','AUDCAD','USDJPY'],
            "SymbolArgs": {
                "DataSource": "BT150",
                "BackupDataSource": "BGNL",
                'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'close',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }
}

sample_stock_datamaster_data_backup_by_local_csv_proxy = {
    "Data": {
        "DataGroup0": {
            "Label": "ABC",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK','JPStock',"APPL.US",'TSLA.US'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'CNY',       # support 'USD' or 'LOCAL' ccy
                'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup1": {
            "Label": "MyStrat1",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-STOCKS/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv" # please use "info" key for information
            },
            "Symbol": ['00700.HK','JPStock',"APPL.US",'TSLA.US'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'CNY',       # support 'USD' or 'LOCAL' ccy
                'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }    
}

sample_stock_datamaster_data_backup_by_local_csv_SPLIT_proxy = {
    "Data": {
        "DataGroup0": {
            "Label": "ABC",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['JPStock',"APPL.US",'TSLA.US'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'CNY',       # support 'USD' or 'LOCAL' ccy
                'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup1": {
            "Label": "MyStrat1",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-STOCKS/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv" # please use "info" key for information
            },
            "Symbol": ['JPStock',"APPL.US",'TSLA.US'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'CNY',       # support 'USD' or 'LOCAL' ccy
                'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup2": {
            "Label": "MyStrat2",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-STOCKS/SPLIT', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/split_data/",  # dir path must use "/" please
                'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv" # please use "info" key for information
            },
            "Symbol": ['JPStock',"APPL.US",'TSLA.US'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'CNY',       # support 'USD' or 'LOCAL' ccy
                'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'split_ratio_last',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }    
}

sample_stock_datamaster_data_backup_by_local_csv_proxy_infodf = {
    "Data": {
        "DataGroup0": {
            "Label": "ABC",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK','JPStock',"APPL.US",'TSLA.US'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'CNY',       # support 'USD' or 'LOCAL' ccy
                'DatacenterMap': get_csv_info_df(r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv")
                # 'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        },
        "DataGroup1": {
            "Label": "MyStrat1",
            "Type": "Download",
            "DataCenter": "LocalCSV",
            "DataCenterArgs": {
                'key_name': 'ABC-STOCKS/MAIN', # optional, if no, default key "{Label}-{folder_name}"
                'dir': r"tests/app/data_manager/data_manager_tests/csv_data/fx_data/",  # dir path must use "/" please
                'info': get_csv_info_df(r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv")
                # 'info': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv" # please use "info" key for information
            },
            "Symbol": ['00700.HK','JPStock',"APPL.US",'TSLA.US'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'CNY',       # support 'USD' or 'LOCAL' ccy
                'DatacenterMap': get_csv_info_df(r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv")
                # 'DatacenterMap': r"tests/app/data_manager/data_manager_tests/csv_data/info/info_dm_map.csv"
            },
            "Fields": 'OHLC',
            "StartDate": {
                "Year": 2019,
                "Month": 9,
                "Day": 30
            },
            "EndDate": {
                "Year": 2019,
                "Month": 10,
                "Day": 2
            },
            "Frequency": "DAILY",
        }
    }    
}

class DataManagerFXIntegrationTestCase(TestCase):

    def test_fx_daily_data(self):
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['FX-FX/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/INT']['data_arr'].size != 0

    def test_fx_one_day(self):
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_one_day)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)
        assert data_manager.data_processor.dict_data_array['FX-FX/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/INT']['data_arr'].size == 0

    def test_fx_daily_with_backup(self):
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_with_backup)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['FX-FX/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/INT']['data_arr'].size != 0
        
    def test_fx_daily_with_backup_one_day(self):
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_with_backup_one_day)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['FX-FX/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/INT']['data_arr'].size == 0

    def test_fx_daily_one_day_no_int(self):
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_1d_noint)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['FX-FX/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['FX-FX/CCPFXRATE']['data_arr'].size != 0
        assert list(data_manager.data_processor.dict_data_array.keys()) == ['FX-FX/MAIN', 'FX-FX/CCPFXRATE']

sample_config_stack_multi_assets = {
    "Data": {
        "DataGroup1": {
            "Label": "FX",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['AUDCAD', 'AUDJPY'],
            "SymbolArgs": {
                "DataSource": "BT150"
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        },
        "DataGroup2": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ', 'VTI.US'],
            "SymbolArgs": {
                "DMAdjustType": 0,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        },
        "DataGroup3": {
            "Label": "Futures123",
            "Type": "Download",
            "DataCenter": "DataMaster",
#            "Symbol": ["1603000025",'1603000059','1603000093'],
            "Symbol": ["1603000557","1603000562","1603000567"],
            "SymbolArgs": {
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' 
            },
            "Fields": ["OHLC"],
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY",
        },
        "DataGroup4": {
            "Label": "ALL",
            "Type": "Download",  # data_manager need this "STACK" keyword to work
            "DataCenter": "DataManager",
            "Symbol": ['Stock-STOCKS', 'FX-FX', 'Futures123-FUTURES'],
            "Fields": [],
            "DMActions": "STACK",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY",
        },
    }
}

sample_config_stack_single_assets = {
    "Data": {
        "DataGroup2": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ', 'VTI.US'],
            "SymbolArgs": {
                "DMAdjustType": 0,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        },
        "DataGroup4": {
            "Label": "ALL",
            "Type": "Download",  # data_manager need this "STACK" keyword to work
            "DataCenter": "DataManager",
            "Symbol": ['Stock-STOCKS'],
            "Fields": [],
            "DMActions": "STACK",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY",
        },
    }
}

class DataManagerIntegrationTestCase(TestCase):
    def test_1_save_pickle_data_then_load(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_save_load_pickle)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert True

    def test_2_load_pickle_data(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_load_pickle)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert True
    
    def test_1_save_pickle_datamaster(self):
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_pickle_datamaster)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert True

    def test_2_load_pickle_datamaster(self):
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_load_datamaster_pickle)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert True
    def test_single_holiday(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(config_for_single_holiday)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['FX-FX/MAIN']['data_arr'].size == 0
        assert list(data_manager.data_processor.dict_data_array['FX-FX/MAIN']['symbol_arr']) == ['AUDCAD', 'AUDNZD', 'AUDJPY']
        assert list(data_manager.data_processor.dict_data_array['FX-FX/MAIN']['fields_arr']) == ['open', 'high', 'low', 'close']
        assert data_manager.data_processor.dict_data_array['FX-FX/CCPFXRATE']['data_arr'].size == 0
        assert list(data_manager.data_processor.dict_data_array['FX-FX/CCPFXRATE']['symbol_arr']) == ['AUDCAD', 'AUDNZD', 'AUDJPY']
        assert list(data_manager.data_processor.dict_data_array['FX-FX/CCPFXRATE']['fields_arr']) == ['fx_rate']
        assert data_manager.data_processor.dict_data_array['FX-FX/INT']['data_arr'].size == 0
        assert list(data_manager.data_processor.dict_data_array['FX-FX/INT']['symbol_arr']) == ['AUDCAD', 'AUDNZD', 'AUDJPY']
        assert list(data_manager.data_processor.dict_data_array['FX-FX/INT']['fields_arr']) == ['interest_rate_last']

    def test_get_info_only(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_info_only)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array == {}
        assert list(data_manager.data_processor.dict_data_str['FX-FX/INFO'].keys()) == ['USDTWD']
        assert data_manager.data_processor.dict_data_str['FX-FX/INFO']['USDTWD']['symbol'] == 'NTN+1M BT150 Curncy'
    
    def test_get_info_local_csv_only(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_info_only_futures)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array == {}
        assert list(data_manager.data_processor.dict_data_str['ABC-CRYPTO/INFO'].keys()) == ['1603000567']
        assert data_manager.data_processor.dict_data_str['ABC-CRYPTO/INFO']['1603000567'] == {'csv_name': '1603000567', 'instrument_id': 1603000567, 'list_exchange': 'CBOT', 'subtype': 1603, 'symbol': '1603000567', 'symboltype': 'US_CONTINUOUSFUTURES', 'timezone': 'US/Central', 'trade_time': ['19:00-7:45', '08:30-13:20'], 'trading_currency': 'USD'}

    def test_get_tradable_info_only(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_tradable_table_only)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # assert data_manager.data_processor.dict_data_array == {}
        # assert list(data_manager.data_processor.dict_data_str['FX-FX/INFO'].keys()) == ['USDTWD']
        # assert data_manager.data_processor.dict_data_str['FX-FX/INFO']['USDTWD']['symbol'] == 'NTN+1M BT150 Curncy'
        assert True
    def test_get_tradable_info_only_intraday(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_tradable_table_only_intraday)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # assert data_manager.data_processor.dict_data_array == {}
        # assert list(data_manager.data_processor.dict_data_str['FX-FX/INFO'].keys()) == ['USDTWD']
        # assert data_manager.data_processor.dict_data_str['FX-FX/INFO']['USDTWD']['symbol'] == 'NTN+1M BT150 Curncy'
        assert True

    def test_get_tradable_info_only_time_check(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_tradable_table_only_time_check)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # assert data_manager.data_processor.dict_data_array == {}
        # assert list(data_manager.data_processor.dict_data_str['FX-FX/INFO'].keys()) == ['USDTWD']
        # assert data_manager.data_processor.dict_data_str['FX-FX/INFO']['USDTWD']['symbol'] == 'NTN+1M BT150 Curncy'
        assert True
    def test_get_tradable_info_only_FX(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_tradable_table_only_FX)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # assert data_manager.data_processor.dict_data_array == {}
        # assert list(data_manager.data_processor.dict_data_str['FX-FX/INFO'].keys()) == ['USDTWD']
        # assert data_manager.data_processor.dict_data_str['FX-FX/INFO']['USDTWD']['symbol'] == 'NTN+1M BT150 Curncy'
        assert True

    def test_get_tradable_info_only_FX_intraday(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_tradable_table_only_FX_intraday)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # assert data_manager.data_processor.dict_data_array == {}
        # assert list(data_manager.data_processor.dict_data_str['FX-FX/INFO'].keys()) == ['USDTWD']
        # assert data_manager.data_processor.dict_data_str['FX-FX/INFO']['USDTWD']['symbol'] == 'NTN+1M BT150 Curncy'
        assert True

    def test_stack_multi_assets(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stack_multi_assets)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # assert data_manager.data_processor.dict_data_array == {}
        # assert list(data_manager.data_processor.dict_data_str['FX-FX/INFO'].keys()) == ['USDTWD']
        # assert data_manager.data_processor.dict_data_str['FX-FX/INFO']['USDTWD']['symbol'] == 'NTN+1M BT150 Curncy'
        assert True
    def test_stack_single_assets(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stack_single_assets)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # assert data_manager.data_processor.dict_data_array == {}
        # assert list(data_manager.data_processor.dict_data_str['FX-FX/INFO'].keys()) == ['USDTWD']
        # assert data_manager.data_processor.dict_data_str['FX-FX/INFO']['USDTWD']['symbol'] == 'NTN+1M BT150 Curncy'
        assert True

class DataManagerCSVIntegrationTestCase(TestCase):
    def test_FX_datamaster_data_backup_by_local_csv_proxy(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_FX_datamaster_data_backup_by_local_csv_proxy)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-FX/MAIN']["data_arr"][0,:,:], 
                                np.array([[ 72.846 ,  73.259 ,  72.691 ,  72.725 ],
                                        [  0.897 ,   0.8975,   0.8932,   0.8932],
                                        [107.745 , 108.155 , 107.505 , 107.765 ]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-FX/CCPFXRATE']["data_arr"][1,:,:], 
                                np.array([[0.75551526],
                                        [0.75551526],
                                        [0.00927945]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-FX/INT']["data_arr"][0,:,:].astype(float), 
                                np.array([[-0.00013262],
                                        [-0.00013262],
                                        [     0.0]]), decimal=3)
                                        # [     np.nan]]), decimal=3)

        # assert True 
    def test_STOCK_datamaster_data_backup_by_local_csv_proxy(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_stock_datamaster_data_backup_by_local_csv_proxy)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-STOCKS/MAIN']["data_arr"][0,:,:], 
                                np.array([[np.nan    , np.nan  , np.nan   , np.nan  ],
                                [  0.897 ,   0.8975,   0.8932,   0.8932],
                                [  0.897 ,   0.8975,   0.8932,   0.8932],
                                [  0.897 ,   0.8975,   0.8932,   0.8932]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-STOCKS/CCPFXRATE']["data_arr"][1,:,:], 
                                np.array([[0.91057989],
                                        [0.06600462],
                                        [7.1384    ],
                                        [7.1384    ]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-STOCKS/INT']["data_arr"][0,:,:], 
                                np.array([[0.],
                                        [0.],
                                        [0.],
                                        [0.]]), decimal=3)

    def test_STOCK_datamaster_data_backup_by_local_csv_SPLIT_proxy(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_stock_datamaster_data_backup_by_local_csv_SPLIT_proxy)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-STOCKS/SPLIT']["data_arr"][0,:,:], 
                                np.array([[1. ],
                                        [2. ],
                                        [1.9]]), decimal=3)


    def test_STOCK_datamaster_infodf_data_backup_by_local_csv_proxy(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_stock_datamaster_data_backup_by_local_csv_proxy_infodf)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-STOCKS/MAIN']["data_arr"][1,:,:], 
        #                         np.array([[324.    , 332.4   , 323.4   , 330.2   ],
        #                         [  0.897 ,   0.8975,   0.8932,   0.8932],
        #                         [  0.897 ,   0.8975,   0.8932,   0.8932],
        #                         [  0.897 ,   0.8975,   0.8932,   0.8932]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-STOCKS/MAIN']["data_arr"][0,:,:], 
                                np.array([[np.nan    , np.nan  , np.nan   , np.nan  ],
                                [  0.897 ,   0.8975,   0.8932,   0.8932],
                                [  0.897 ,   0.8975,   0.8932,   0.8932],
                                [  0.897 ,   0.8975,   0.8932,   0.8932]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-STOCKS/CCPFXRATE']["data_arr"][1,:,:], 
                                np.array([[0.91057989],
                                        [0.06600462],
                                        [7.1384    ],
                                        [7.1384    ]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-STOCKS/INT']["data_arr"][0,:,:], 
                                np.array([[0.],
                                        [0.],
                                        [0.],
                                        [0.]]), decimal=3)

    def test_for_local_csv_proxy(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_local_csv_proxy)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert True

    def test_for_local_csv_proxy_no_fields(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_local_csv_proxy_no_fields)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['ABC-CRYPTO/MAIN2']['data_arr'].size != 0
        assert list(data_manager.data_processor.dict_data_array['ABC-CRYPTO/MAIN2']['symbol_arr']) == ['coinA']
        assert list(data_manager.data_processor.dict_data_array['ABC-CRYPTO/MAIN2']['fields_arr']) == ['open', 'high', 'low', 'close', 'volume']

    def test_for_local_csv_proxy_no_info_dir(self):
        ''' 
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_local_csv_proxy_no_info_dir)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert True

    def test_for_local_csv_proxy_empty_infodf(self):
        ''' 
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_local_csv_proxy_empty_infodf)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert True

    def test_for_local_csv_proxy_csv_not_exist(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_local_csv_proxy_csv_not_exist)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        npt.assert_almost_equal(data_manager.data_processor.dict_data_array["ABC-CRYPTO/MAIN2"]['data_arr'][0,:,:].astype(float), 
                                np.array([[ 1.006, 73.104, 72.702, 72.972],
                                        [np.nan, np.nan, np.nan, np.nan]]), decimal=3)

    def test_for_local_csv_settlement_open(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_local_csv_proxy_csv_not_exist_settlement_open)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        npt.assert_almost_equal(data_manager.data_processor.dict_data_array["ABC-CRYPTO/MAIN"]['data_arr'][0,:,:].astype(float), 
                                np.array([[1.006, 0],
                                [np.nan, np.nan]]), decimal=3)

    def test_for_local_csv_settlement_open_hourly(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_local_csv_proxy_csv_not_exist_settlement_open_hourly)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        npt.assert_almost_equal(data_manager.data_processor.dict_data_array["ABC-CRYPTO/MAIN"]['data_arr'][0,:,:].astype(float), 
                                np.array([[1.006, 0],
                                [np.nan, np.nan]]), decimal=3)

    def test_for_local_csv_proxy_hourly(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_local_csv_proxy_hourly)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        npt.assert_almost_equal(data_manager.data_processor.dict_data_array["ABC-CRYPTO/MAIN"]['time_arr'], 
                                np.array([[1569805200, 1569805200,          0,       2019,          9, 30,          1,          0,          0],
                                        [1569808800, 1569808800,          0,       2019,          9,30,          2,          0,          0]]), decimal=3)

    def test_for_backup_datamaster_with_csv(self):
        ''' 
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_backup_datamaster_with_csv)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)
        
        npt.assert_almost_equal(data_manager.data_processor.dict_data_array["FX-FX/MAIN"]['data_arr'][3,:,:], 
                                np.array([[ 30.333,  30.36 ,  30.235,  30.255],
                                        [200.   , 200.   , 200.   , 200.   ]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_processor.dict_data_array["FX-FX/CCPFXRATE"]['data_arr'][3,:,:], 
                                np.array([[0.03305239],
                                        [0.98677719]]), decimal=3)
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack['FX-FX/INT']["data_arr"][3,:,:], 
                                np.array([[-2.20203688e-05],
                                        [ 7.87961975e-05]]), decimal=3)

    def _test_debug(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_debug)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert True        


sample_config_stock_data_HKD = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ', 'VTI.US'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'HKD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_stock_data_USD = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK','000005.SZ', 'VTI.US','00005.HK','01299.HK'],
            "SymbolArgs": {
                "DMAdjustType": 0,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_stock_data_LOCAL = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'LOCAL',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_stock_data_USD_open_close_PMS = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'LOCAL',       # support 'USD' or 'LOCAL' ccy
                "SettlementType": "open"    # open or close
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_stock_data_USD_close_PMS = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['00700.HK', '000005.SZ'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'LOCAL',       # support 'USD' or 'LOCAL' ccy
                "SettlementType": "close"    # open or close
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_stock_data_US_Stock_only = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['MSFT.US'],
            "SymbolArgs": {
                "DMAdjustType": 0,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_stock_data_US_ETF_only = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['VTI.US', 'SCHF.US', 'VWO.US', 'VB.US', 'AGG.US', 'EMB.US', 'HYG.US', 'FLOT.US', 'VTIP.US', 'GLD.US', 'XLE.US', 'QQQ.US', 'ASHR.US', 'INDA.US'],
            "SymbolArgs": {
                "DMAdjustType": 0,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_stock_data_HK_ETF_only = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['2813.HK', '3010.HK', '3077.HK', '3081.HK', '3101.HK', '3140.HK', '3141.HK', '3169.HK'],
            "SymbolArgs": {
                "DMAdjustType": 0,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2020,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}


# sz_symbols = ["000001.SZ","000002.SZ","000004.SZ","000005.SZ","000006.SZ","000007.SZ","000008.SZ","000009.SZ","000010.SZ","000011.SZ","000012.SZ","000014.SZ","000016.SZ","000017.SZ","000018.SZ","000019.SZ","000020.SZ","000021.SZ","000022.SZ","000023.SZ","000025.SZ","000026.SZ","000027.SZ","000028.SZ","000029.SZ","000030.SZ","000031.SZ","000032.SZ","000033.SZ","000034.SZ","000035.SZ","000036.SZ","000037.SZ","000038.SZ","000039.SZ","000040.SZ","000042.SZ","000043.SZ","000045.SZ","000046.SZ","000048.SZ","000049.SZ","000050.SZ","000055.SZ","000056.SZ","000058.SZ","000059.SZ","000060.SZ","000061.SZ","000062.SZ","000063.SZ","000065.SZ","000066.SZ","000068.SZ","000069.SZ","000070.SZ","000078.SZ","000088.SZ","000089.SZ","000090.SZ","000096.SZ","000099.SZ","000100.SZ","000150.SZ","000151.SZ","000153.SZ","000155.SZ","000156.SZ","000157.SZ","000158.SZ","000159.SZ","000166.SZ","000301.SZ","000333.SZ","000338.SZ","000400.SZ","000401.SZ","000402.SZ","000403.SZ","000404.SZ","000407.SZ","000408.SZ","000409.SZ","000410.SZ","000411.SZ","000413.SZ","000415.SZ","000416.SZ","000417.SZ","000418.SZ","000419.SZ","000420.SZ","000421.SZ","000422.SZ","000423.SZ","000425.SZ","000426.SZ","000428.SZ","000429.SZ","000430.SZ","000488.SZ","000498.SZ","000501.SZ","000502.SZ","000503.SZ","000504.SZ"]
sz_symbols = ["000001.SZ","000002.SZ","000004.SZ","000005.SZ","000006.SZ","000007.SZ","000008.SZ","000009.SZ","000010.SZ","000011.SZ","000012.SZ","000014.SZ","000016.SZ","000017.SZ","000018.SZ","000019.SZ","000020.SZ"]
sample_config_stock_data_SZ_stocks_only = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": sz_symbols,
            "SymbolArgs": {
                "DMAdjustType": 0,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2017,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2019,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY"
        }
    }
}

sample_config_stock_data_SZ_stocks_not_existing = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": ['000001.SZ','000002.SZ','002731.SZ'],
            "SymbolArgs": {
                "DMAdjustType": 1,      # (int) for different adjust_type, default 0
                "DMCalendar": "SSE",
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2012,
                "Month": 1,
                "Day": 1
            },
            "EndDate": {
                "Year": 2012,
                "Month": 6,
                "Day": 1
            },
            "Frequency": "DAILY"
        }
    }
}
# dividend_symbol = ['601012.SH']
# dividend_symbol = ['000012.SZ']
# dividend_symbol = ['00700.HK']
dividend_symbol = ['601012.SH','000012.SZ','00700.HK']
sample_config_stock_data_dividend = {
    "Data": {
        "DataGroup1": {
            "Label": "Stock",
            "Type": "Download",
            "DataCenter": "DataMaster",
            "Symbol": dividend_symbol,
            "SymbolArgs": {
                "DMAdjustType": 0,      # (int) for different adjust_type, default 0
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
                "DMGetDividend": False,
                "DMGetSplit": False,
            },
            "Fields": "OHLC",
            "StartDate": {
                "Year": 2017,
                "Month": 5,
                "Day": 13
            },
            "EndDate": {
                "Year": 2018,
                "Month": 6,
                "Day": 1
            },
            "Frequency": "DAILY"
        }
    }
}

class DataManagerSTOCKIntegrationTestCase(TestCase):

    def test_stock_daily_dividend(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_dividend)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/SPLIT']['data_arr'].size != 0
        # assert data_manager.data_processor.dict_data_array['Stock-STOCKS/SPLIT']['data_arr'][1][2][0] == 1.15
        # assert data_manager.data_processor.dict_data_array['Stock-STOCKS/SPLIT']['data_arr'][3][1][0] == 1.4


    def test_stock_daily_USD(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_USD)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0

    def test_stock_daily_HKD(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_HKD)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0

    def test_stock_daily_LOCAL(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_LOCAL)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0

    def test_stock_daily_USD_open_close_main_data(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_USD_open_close_PMS)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0

    def test_stock_daily_USD_close_main_data(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_USD_close_PMS)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0

    def _test_stock_daily_US_Stock_Only(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_US_Stock_only)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0


    def test_stock_daily_US_ETF_Only(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_US_ETF_only)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0

    def test_stock_daily_HK_ETF_Only(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_HK_ETF_only)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0


    def test_stock_daily_SZ_stocks_Only(self):

        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_SZ_stocks_only)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0

    def test_stock_daily_SZ_stocks_not_exist(self):
        '''
            Waiting for DataMaster to fix the problem
            Column names are sometimes 0 1 2 3 4 instead of open high low close
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_stock_data_SZ_stocks_not_existing)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/MAIN']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/CCPFXRATE']['data_arr'].size != 0
        assert data_manager.data_processor.dict_data_array['Stock-STOCKS/INT']['data_arr'].size != 0

sample_config_for_debug_for_download_data = {
    "dm_pickle":{
        "save_dir": "tests/app/data_manager/data_manager_tests/",
        "save_name": "stock_cn_data",
        "to_pkl": True,
        "from_pkl": False
    },
    'Data': {'DataGroup1': {'Label': 'STOCKS', 'Type': 'Download', 'DataCenter': 'DataMaster', 'Symbol': 'tests/app/data_manager/data_manager_tests/csv_data/aqm_cn_stock.csv', 'SymbolArgs': {'DMAdjustType': 3, 'AccountCurrency': 'LOCAL', 'DMCalendar': 'SSE'}, 'Fields': ['acas_roa', 'acas_roe'], 'StartDate': {'Year': 2009, 'Month': 1, 'Day': 31}, 'EndDate': {'Year': 2020, 'Month': 10, 'Day': 14}, 'Frequency': 'DAILY'}}
}

# sample_config_for_debug = {
#     "dm_pickle":{
#         "save_dir": "tests/app/data_manager/data_manager_tests/",
#         "save_name": "stock_cn_data",
#         "to_pkl": True,
#         "from_pkl": False
#     },
#     'Data': {'DataGroup1': {'Label': 'STOCKS', 'Type': 'Download', 'DataCenter': 'DataMaster', 'Symbol': 'tests/app/data_manager/data_manager_tests/csv_data/aqm_cn_stock.csv', 'SymbolArgs': {'AccountCurrency': 'LOCAL'}, 'Fields': ['acas_roa', 'acas_roe'], 'StartDate': {'Year': 2003, 'Month': 1, 'Day': 31}, 'EndDate': {'Year': 2020, 'Month': 10, 'Day': 14}, 'Frequency': 'DAILY'}}
# }

sample_config_for_debug = {

   "Data":{
      "DataGroup0":{
         "DataCenter":"DataMaster",
         "SymbolArgs":{
            "DataSource":"BT150",
            "BackupDataSource":"BGNL",
            "SettlementType":"close"
         },
         "Fields":[
            "open",
            "close"
         ],
         "Frequency":"DAILY",
         "Label":"FX",
         "Type":"Download",
         "Symbol":[
            "EURUSD",
            "GBPJPY",
            "NOKSEK",
            "NZDJPY",
            "USDJPY"
         ],
         "StartDate":{
            "Year":2009,
            "Month":12,
            "Day":18
         },
         "EndDate":{
            "Year":2020,
            "Month":1,
            "Day":11,
            "Hour":23,
            "Minute":59,
            "Second":59
         }
      },
      "DataGroup1":{
         "DataCenter":"DataMaster",
         "SymbolArgs":{
            "DMAdjustType":0,
            "AccountCurrency":"USD",
            "SettlementType":"open"
         },
         "Fields":[
            "open",
            "close"
         ],
         "Frequency":"DAILY",
         "Label":"STOCKS",
         "Type":"Download",
         "Symbol":[
            "000005.SZ",
            "600016.SH",
            "600028.SH",
            "601398.SH",
            "601628.SH"
         ],
         "StartDate":{
            "Year":2009,
            "Month":12,
            "Day":18
         },
         "EndDate":{
            "Year":2020,
            "Month":1,
            "Day":11,
            "Hour":23,
            "Minute":59,
            "Second":59
         }
      },
      "DataGroup2":{
         "DataCenter":"DataMaster",
         "SymbolArgs":{
            "DMAdjustType":0,
            "AccountCurrency":"USD",
            "SettlementType":"close"
         },
         "Frequency":"DAILY",
         "Label":"FUTURES",
         "Type":"Download",
         "Symbol":[
            "1603000557",
            "1603000558",
            "1603000562",
            "1603000563",
            "1603000567",
            "1603000568"
         ],
         "Fields":[
            "open",
            "close"
         ],
         "StartDate":{
            "Year":2009,
            "Month":12,
            "Day":18
         },
         "EndDate":{
            "Year":2020,
            "Month":1,
            "Day":11,
            "Hour":23,
            "Minute":59,
            "Second":59
         }
      },
      "DataGroup3":{
         "Label":"TRADABLE",
         "Type":"Download",
         "DataCenter":"DataMaster",
         "Symbol":[
            "EURUSD",
            "GBPJPY",
            "NOKSEK",
            "NZDJPY",
            "USDJPY",
            "000005.SZ",
            "600016.SH",
            "600028.SH",
            "601398.SH",
            "601628.SH",
            "1603000557",
            "1603000558",
            "1603000562",
            "1603000563",
            "1603000567",
            "1603000568"
         ],
         "Fields":"tradable_table",
         "Frequency":"DAILY",
         "SymbolArgs":{
            "AccountCurrency":"USD",
            "SettlementType":"open"
         },
         "StartDate":{
            "Year":2009,
            "Month":12,
            "Day":18
         },
         "EndDate":{
            "Year":2020,
            "Month":1,
            "Day":11,
            "Hour":23,
            "Minute":59,
            "Second":59
         }
      },
      "DataGroup4":{
         "Label":"MIX",
         "Type":"Download",
         "DataCenter":"DataManager",
         "DMActions":"STACK",
         "Symbol":[
            "FX-FX",
            "STOCKS-STOCKS",
            "FUTURES-FUTURES"
         ],
         "Fields":[
            
         ],
         "Frequency":"DAILY",
         "StartDate":{
            "Year":2009,
            "Month":12,
            "Day":18
         },
         "EndDate":{
            "Year":2020,
            "Month":1,
            "Day":11,
            "Hour":23,
            "Minute":59,
            "Second":59
         }
      },
      "DataGroup5":{
         "DataCenter":"DataMaster",
         "Symbol":[
            "000005.SZ",
            "600016.SH",
            "600028.SH",
            "601398.SH",
            "601628.SH"
         ],
         "SymbolArgs":{
            "DMAdjustType":1,
            "AccountCurrency":"USD"
         },
         "Fields":"OHLC",
         "Frequency":"DAILY",
         "Type":"Download",
         "Label":"stocks-REF",
         "StartDate":{
            "Year":2009,
            "Month":12,
            "Day":18
         },
         "EndDate":{
            "Year":2020,
            "Month":1,
            "Day":11,
            "Hour":23,
            "Minute":59,
            "Second":59
         }
      },
      "DataGroup6":{
         "DataCenter":"DataMaster",
         "Symbol":[
            "EURUSD",
            "GBPJPY",
            "NOKSEK",
            "NZDJPY",
            "USDJPY"
         ],
         "SymbolArgs":{
            "DataSource":"BT150",
            "BackupDataSource":"BGNL"
         },
         "Fields":"OHLC",
         "Frequency":"DAILY",
         "Type":"Download",
         "Label":"fx-REF",
         "StartDate":{
            "Year":2009,
            "Month":12,
            "Day":18
         },
         "EndDate":{
            "Year":2020,
            "Month":1,
            "Day":11,
            "Hour":23,
            "Minute":59,
            "Second":59
         }
      }
   },
   "dm_pickle":{
      
   }
}
# python -m pytest tests/app/data_manager/data_manager_tests/test_data_manager.py -k "test_debug"
# "dm_pickle":{
#     "save_dir": "tests/app/data_manager/data_manager_tests/",
#     "save_name": "stock_cn_data",
#     "to_pkl": True,
#     "from_pkl": False
# },
# 'Data': {'DataGroup1': {'Label': 'STOCKS', 'Type': 'Download', 'DataCenter': 'DataMaster', 'Symbol': 'tests/app/data_manager/data_manager_tests/csv_data/aqm_cn_stock.csv', 'SymbolArgs': {'AccountCurrency': 'LOCAL'}, 'Fields': ['choice_tot_assets', 'choice_tot_equity'], 'StartDate': {'Year': 2003, 'Month': 1, 'Day': 31}, 'EndDate': {'Year': 2020, 'Month': 10, 'Day': 14}, 'Frequency': 'DAILY'}}

class DataManagerResultReaderIntegrationTestCase(TestCase):
    def _test_strat_pnl(self):
        ''' This test has dependency on tests/app/data_manager/data_manager_tests/hdf5_files/RSI_lv1.hdf5 file

        Can be replaced with other hdf5 files via config
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_pnl)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)


        assert data_manager.data_distributor_main.dict_np_data_need_pack['Strat-Strat/pnl']['data_arr'].size != 0
        assert data_manager.data_distributor_main.dict_np_data_need_pack['Strat-Strat/pnl']['time_arr'].size != 0
        assert list(data_manager.data_distributor_main.dict_np_data_need_pack['Strat-Strat/pnl']['symbol_arr']) == [0,1,2,3,4,5,6,7,8]
        assert (data_manager.data_distributor_main.dict_np_data_need_pack['Strat-Strat/pnl']['fields_arr'] == np.array([['pnl'],['pnl'],['pnl'],['pnl'],['pnl'],['pnl'],['pnl'],['pnl'],['pnl']])).all()
    
    def _test_strat_metrics(self):
        ''' This test has dependency on tests/app/data_manager/data_manager_tests/hdf5_files/RSI_lv1.hdf5 file

        Can be replaced with other hdf5 files via config
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_metrics)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/position']['data_arr'].size != 0
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/position']['time_arr'].size != 0
        assert list(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/position']['symbol_arr']) == [0,1,2,3,4,5,6,7,8]
        assert (data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/position']['fields_arr'] == np.array([['AUDCAD'],['AUDJPY'],['AUDNZD'],['AUDCAD'],['AUDJPY'],['AUDNZD'],['AUDCAD'],['AUDJPY'],['AUDNZD']])).all()
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/pnl']['data_arr'].size != 0
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack["ABC-Strat/metrics_61"]['data_arr'][-1,0,:], 
                                np.array([ 1.01714565e-02,  8.96812755e-01,  2.15608548e-02,  6.38152479e-03,
                                4.20197874e-02,  7.00000000e+00, -3.62264765e-03,  3.58333143e-02,
                                8.57142857e-01,  4.28571429e-01,  3.63636364e-01,  6.81063593e-03,
                                -3.15872034e-03,  3.80743155e-03, -1.81240887e-03]), decimal=3)
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_61']['time_arr'].size != 0
        assert list(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_61']['symbol_arr']) == [0,1,2,3,4,5,6,7,8]
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_61']['fields_arr'] == ['holding_period_return', 'sharpe_ratio', 'MDD', 'ADD', 'rate_of_return',
                        'number_of_trades', 'return_from_longs', 'return_from_shorts', 'ratio_of_longs',
                        'hit_ratio', 'profit_rate','return_from_hits', 'return_from_misses','return_from_profit_days','return_from_loss_days']
        npt.assert_almost_equal(data_manager.data_distributor_main.dict_np_data_need_pack["ABC-Strat/metrics_252"]['data_arr'][-1,0,:], 
                                np.array([-1.93303823e-02, -4.87251111e-01,  4.88705294e-02,  2.63103374e-02,
                                -1.93303823e-02,  2.50000000e+01,  4.84117336e-03,  7.12107616e-03,
                                7.20000000e-01,  4.40000000e-01,  3.63013699e-01,  4.87110071e-03,
                                -2.97284702e-03,  3.05515494e-03, -1.94896338e-03]), decimal=3)
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_252']['time_arr'].size != 0
        assert list(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_252']['symbol_arr']) == [0,1,2,3,4,5,6,7,8]
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_252']['fields_arr'] == ['holding_period_return', 'sharpe_ratio', 'MDD', 'ADD', 'rate_of_return',
                        'number_of_trades', 'return_from_longs', 'return_from_shorts', 'ratio_of_longs',
                        'hit_ratio', 'profit_rate','return_from_hits', 'return_from_misses','return_from_profit_days','return_from_loss_days']

    def _test_strat_empty_metrics(self):
        ''' This test has dependency on tests/app/data_manager/data_manager_tests/hdf5_files/RSI_lv1.hdf5 file

        Can be replaced with other hdf5 files via config
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_for_empty_metrics)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/position']['data_arr'].size == 0
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/position']['time_arr'].size == 0
        assert list(data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/position']['symbol_arr']) == [0,1,2,3,4,5,6,7,8]
        assert (data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/position']['fields_arr'] == np.array([['AUDCAD'],['AUDJPY'],['AUDNZD'],['AUDCAD'],['AUDJPY'],['AUDNZD'],['AUDCAD'],['AUDJPY'],['AUDNZD']])).all()
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/pnl']['data_arr'].size == 0
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_61']['data_arr'].size == 0
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_61']['time_arr'].size == 0
        assert not data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_61']['symbol_arr']
        assert not data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_61']['fields_arr']
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_252']['data_arr'].size == 0
        assert data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_252']['time_arr'].size == 0
        assert not data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_252']['symbol_arr']
        assert not data_manager.data_distributor_main.dict_np_data_need_pack['ABC-Strat/metrics_252']['fields_arr']



sample_config_futures_1d = {
    "Data": {
        "DataGroup1": {
            "Label": "Futures123",
            "Type": "Download",
            "DataCenter": "DataMaster",
#            "Symbol": ["1603000025",'1603000059','1603000093'],
            "Symbol": ["1603000557","1603000562","1603000567"],
            "SymbolArgs": {
                "AccountCurrency": 'USD',       # support 'USD' or 'LOCAL' ccy
                # "DMAdjustType": 1
            },
            "Fields": ["OHLC"],
            "StartDate": {
                "Year": 2016,
                "Month": 1,
                "Day": 6
            },
            "EndDate": {
                "Year": 2016,
                "Month": 1,
                "Day": 15
            },
            "Frequency": "DAILY",
        }
    }
}

class DataManagerFUTURESIntegrationTestCase(TestCase):
    def test_futures_data(self):
        '''
        '''
        data_manager = DataManager()
        data_manager.prepare()
        data_manager.load_config(sample_config_futures_1d)
        data_manager.load_data()
        data_manager.process_data()
        data_manager.data_distributor_main.pack_data()

        dds = DataDistributorSub()
        output = dds.unpack_data(
            data_manager.data_distributor_main.data_package)

        # assert data_manager.data_processor.dict_data_array['Futures123-FUTURES/MAIN']['data_arr'].size != 0
        # assert data_manager.data_processor.dict_data_array['Futures123-FUTURES/CCPFXRATE']['data_arr'].size != 0
        assert True