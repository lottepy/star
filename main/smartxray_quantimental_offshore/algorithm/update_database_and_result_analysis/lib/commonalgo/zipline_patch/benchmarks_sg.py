#
# Copyright 2013 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pandas as pd
import requests
from .markort_sgm import get_data
import datetime


def get_benchmark_returns(symbol):
    """
    Get a Series of benchmark returns from IEX associated with `symbol`.
    Default is `SPY`.

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.

    The data is provided by IEX (https://iextrading.com/), and we can
    get up to 5 years worth of data.
    """
    r = requests.get(
        'https://api.iextrading.com/1.0/stock/{}/chart/5y'.format(symbol)
    )
    data = r.json()

    df = pd.DataFrame(data)

    df.index = pd.DatetimeIndex(df['date'])
    df = df['close']

    return df.sort_index().tz_localize('UTC').pct_change(1).iloc[1:]

def get_iuid_from_symbol(symbol):
    cn_list = ["SHSZ300"]
    hk_list = ["HSI"]
    if symbol in cn_list:
        aqm_iuid = "CN_30_" + symbol
    elif symbol in hk_list:
        aqm_iuid = "HK_30_" + symbol
    else:
        aqm_iuid = "US_30_" + symbol
    return aqm_iuid

def get_aqm_bm_returns(symbol):
    """
    Get a Series of benchmark returns from AQUMON MARKORT SERVICE associated with `symbol`.
    Default is `SPY`.

    Parameters
    ----------
    symbol : str
        Benchmark symbol for which we're getting the returns.

    """
    aqm_iuid = get_iuid_from_symbol(symbol)
    start_str = '2000-01-01'
    end_str = datetime.date.today().isoformat()

    df = get_data([aqm_iuid], [4], 'D', start_str, end_str,
                  adjust_type=0, fill='nan', precision=4, tz_region=None, currency=None)

    df.index = pd.DatetimeIndex(df.index)
    df = df[(aqm_iuid,'4')].astype('float')

    return df.sort_index().tz_localize('UTC').pct_change(1).iloc[1:]
