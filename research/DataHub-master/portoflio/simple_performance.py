import pandas as pd
import numpy as np


# def max_drawdown(price_ts):
#     i = np.argmax(np.maximum.accumulate(price_ts) - price_ts)  # end of the period
#     j = np.argmax(price_ts[:i])  # start of period
#     return (price_ts[j] - price_ts[i]) / price_ts[j]
def max_drawdown(price_ts):
    maxdd_series =pd.Series([])
    for k in price_ts.columns:
        data = price_ts[k]
        # i = np.argmax(np.maximum.accumulate(data) - data)  # end of the period
        # j = np.argmax(data[:i])  # start of period

        i = (np.maximum.accumulate(data) - data).astype('float').idxmax()  # end of the period
        j = (data[:i]).astype('float').idxmax()
        value = (data[j] - data[i]) / data[j]
        maxdd_series = maxdd_series.append(pd.Series(value,index = [k]))
    return maxdd_series


def summary_from_series(price_ts):
    start_date = price_ts.index[0]
    # start_date = '2016-12-30'
    end_date = price_ts.index[-1]
    # price_ts = 1+ price_ts.ix[start_date:end_date]

    # columns = ['acc_ret', 'ann_ret', 'ann_vol', 'sharpe_ratio', 'max_drawdown']
    # summary = pd.DataFrame(np.zeros((1, len(columns))), columns=columns, index=price_ts.columns)
    summary = pd.DataFrame(index=price_ts.columns)

    rf_rate = 0.00
    return_ts = price_ts.pct_change().fillna(0)

    acc_ret = (price_ts.ix[end_date] / price_ts.ix[start_date] - 1)
    ann_vol = (return_ts.std() * np.sqrt(252))
    ann_ret = np.power(1 + acc_ret, 365.25 / ((pd.to_datetime(end_date) - pd.to_datetime(start_date)).days)) - 1
    summary['acc_ret'] = acc_ret
    summary['ann_ret'] = ann_ret
    summary['ann_vol'] = ann_vol
    summary['sharpe_ratio'] = (ann_ret - rf_rate) / ann_vol
    summary['max_drawdown'] = max_drawdown(price_ts)

    return summary


if __name__ == '__main__':
    # data = pd.read_csv('us2017.csv', index_col=['date'], infer_datetime_format=True, parse_dates=['date'])
    # data = pd.read_csv('hk2017.csv', index_col=0, infer_datetime_format=True)
    # excel_path = 'data/data_daily_20180303.csv'
    # data = pd.read_csv(excel_path,index_col=0)
    data = pd.read_csv('A.csv',index_col=0)
    # print summary_from_series(data['portfolio_value0'])
    sumary = summary_from_series(data)
    print(sumary)
    