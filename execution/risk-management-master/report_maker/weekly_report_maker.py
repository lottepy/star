#!/usr/bin/env python
# coding=utf-8

import sys

reload(sys)
sys.setdefaultencoding("utf-8")

import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import datetime
import logging
# from tia.bbg import Terminal
from jinja2 import Environment, FileSystemLoader


def max_drawdown(price_ts):
    i = np.argmax(np.maximum.accumulate(price_ts) - price_ts)  # end of the period
    j = np.argmax(price_ts[:i])  # start of period
    return (price_ts[j] - price_ts[i]) / price_ts[j]


def summary_from_series(price_ts, benchmark_ts=None):
    start_date = price_ts.index[0]
    end_date = price_ts.index[-1]

    columns = ['accumulative_return', 'annualized_return', 'annualized_volatility', 'sharpe_ratio', 'max_drawdown']
    summary = pd.DataFrame(np.zeros((1, len(columns))), columns=columns, index=['summary'])

    rf_rate = 0.00
    return_ts = price_ts.pct_change().fillna(0)

    acc_ret = (price_ts.ix[end_date] / price_ts.ix[start_date] - 1)
    ann_ret = (np.power(1 + acc_ret, 365.25 / (end_date - start_date).days) - 1)
    ann_vol = (return_ts.std() * np.sqrt(252))
    # summary['acc_ret'] = acc_ret
    # summary['ann_ret'] = ann_ret
    # summary['ann_vol'] = ann_vol
    # summary['sharpe_ratio'] = (ann_ret - rf_rate) / ann_vol
    # summary['max_drawdown'] = max_drawdown(price_ts)

    # format
    summary['accumulative_return'] = str(np.round(100 * acc_ret, 2)) + '%'
    summary['annualized_return'] = str(np.round(100 * ann_ret, 2)) + '%'
    summary['annualized_volatility'] = str(np.round(100 * ann_vol, 2)) + '%'
    summary['sharpe_ratio'] = str(np.round((ann_ret - rf_rate) / ann_vol, 4))
    summary['max_drawdown'] = str(np.round(100 * max_drawdown(price_ts), 2)) + '%'

    if benchmark_ts is not None:
        benchmark_return_ts = benchmark_ts.pct_change().fillna(0)
        benchmark_return = np.power(benchmark_ts.ix[end_date] / benchmark_ts.ix[start_date],
                                    365.25 / (end_date - start_date).days) - 1
        active_return = summary['ann_ret'].values[0] - benchmark_return
        tracking_error = (return_ts.values - benchmark_return_ts.values).std() * np.sqrt(252)
        summary['info_ratio'] = active_return / tracking_error

        regr = lm.LinearRegression()
        X = np.array([benchmark_return_ts]).T
        y = return_ts
        regr.fit(X, y)  # fit(X,y)
        # print regr.coef_
        # print mean_squared_error(y, regr.predict(X))

        summary['r_squared'] = r2_score(y, regr.predict(X))
        summary['beta'] = regr.coef_[0]

    return summary


def upload(host, port, username, password, localpath, path):
    import paramiko
    print path
    print localpath
    transport = paramiko.Transport((host, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(localpath, path)
    sftp.close()
    transport.close()


ticker_map = {
    '000300.SH': u'沪深300指数',
    '399905.SZ': u'中证500指数',
    '399005.SZ': u'中小板指数',
    '399006.SZ': u'创业板指数',
    '000001.SH': u'上证综指',
    '399001.SZ': u'深圳成指',
    '001974.OF': u'景顺长城量化新动力',
    '001733.OF': u'泰达宏利量化股票',
    '000978.OF': u'景顺长城量化精选股票',
    '002952.OF': u'建信多因子量化股票',
    '002300.OF': u'长盛医疗行业量化配置股票',
    '163110.OF': u'申万量化小盘',
    '001637.OF': u'嘉实腾讯自选股大数据策略股票',
    '801010.SI': u'农林牧渔', '801020.SI': u'采掘', '801030.SI': u'化工', '801040.SI': u'钢铁',
    '801050.SI': u'有色金属', '801080.SI': u'电子', '801110.SI': u'家用电器', '801120.SI': u'食品饮料',
    '801130.SI': u'纺织服装', '801140.SI': u'轻工制造', '801150.SI': u'医药生物',
    '801160.SI': u'公用事业', '801170.SI': u'交通运输', '801180.SI': u'房地产', '801200.SI': u'商业贸易',
    '801210.SI': u'休闲服务', '801230.SI': u'综合', '801710.SI': u'建筑材料', '801720.SI': u'建筑装饰',
    '801730.SI': u'电气设备', '801740.SI': u'国防军工', '801750.SI': u'计算机', '801760.SI': u'传媒',
    '801770.SI': u'通信', '801780.SI': u'银行', '801790.SI': u'非银金融', '801880.SI': u'汽车',
    '801890.SI': u'机械设备',
    'AQUMON': 'AQUMON'
}

if __name__ == '__main__':

    report_names = ['long_onshore', 'allocation_onshore', 'long_offshore', 'allocation_offshore']

    bm_performance_ts = pd.read_csv('common/weekly_index_fund_ts.csv',
                                    index_col=0,
                                    parse_dates=0,
                                    infer_datetime_format=True)

    for report_name in report_names:
        print 'generating report: {}'.format(report_name)

        algo_performance_ts = pd.read_csv(report_name + '/' + 'weekly_performance.csv',
                                          index_col=0, parse_dates=0, infer_datetime_format=True)

        benchmark_list = ['000978.OF', '163110.OF', '000300.SH', '399905.SZ', '399005.SZ', '399006.SZ']
        mfm_ts = algo_performance_ts[['AQUMON']]
        mfm_ts = mfm_ts.join(bm_performance_ts[benchmark_list])

        mfm_ts.columns = [ticker_map[tck] for tck in mfm_ts.columns.values]
        mfm_ts = mfm_ts.div(mfm_ts.iloc[0], axis=1)

        # plots
        plot_name = report_name + '/' + 'portfolio_mfm.png'
        fig = plt.figure(figsize=(15, 5))
        ax = fig.add_subplot(111)
        mfm_ts.plot()
        plt.grid(True)
        plt.ylabel('Performance')
        plt.xlabel('Date')
        plt.title('AQUMON Performance')
        plt.tight_layout(pad=0.1)
        plt.legend(bbox_to_anchor=(0.0, 1.0), loc='upper left', fontsize='x-small', ncol=2)
        plt.savefig(plot_name, format='png')

        # period performance
        mfm_all = mfm_ts.copy()
        mfm_all_return = mfm_all.iloc[-1] / mfm_all.iloc[0] - 1  # since inception
        date_2year_start = datetime.datetime(2016, 7, 6)
        date_1year_start = datetime.datetime(2017, 7, 6)
        date_ytd_start = datetime.datetime(2017, 12, 29)
        date_2018Jan_start = datetime.datetime(2017, 12, 29)
        date_2018Jan_end = datetime.datetime(2018, 1, 31)
        date_2018Feb_start = datetime.datetime(2018, 1, 31)
        date_2018Feb_end = datetime.datetime(2018, 2, 28)
        date_2018Mar_start = datetime.datetime(2018, 2, 28)
        date_2018Mar_end = datetime.datetime(2018, 3, 30)
        date_2018Apr_start = datetime.datetime(2018, 3, 30)
        date_2018Apr_end = datetime.datetime(2018, 4, 27)
        date_2018May_start = datetime.datetime(2018, 4, 27)
        date_2018May_end = datetime.datetime(2018, 5, 31)
        date_2018Jun_start = datetime.datetime(2018, 5, 31)
        date_2018Jun_end = datetime.datetime(2018, 6, 29)
        date_2018Jul_start = datetime.datetime(2018, 6, 29)
        date_2018Jul_end = datetime.datetime(2018, 7, 6)
        date_2017_start = datetime.datetime(2016, 12, 30)
        date_2017_end = datetime.datetime(2017, 12, 29)
        date_2016_start = datetime.datetime(2015, 12, 31)
        date_2016_end = datetime.datetime(2016, 12, 30)
        date_lastweek_start = datetime.datetime(2018, 6, 29)
        date_lastweek_end = datetime.datetime(2018, 7, 6)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.iloc[-1] / mfm_all.loc[date_2year_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.iloc[-1] / mfm_all.loc[date_1year_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.iloc[-1] / mfm_all.loc[date_ytd_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2018Jan_end] / mfm_all.loc[date_2018Jan_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2018Feb_end] / mfm_all.loc[date_2018Feb_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2018Mar_end] / mfm_all.loc[date_2018Mar_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2018Apr_end] / mfm_all.loc[date_2018Apr_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2018May_end] / mfm_all.loc[date_2018May_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2018Jun_end] / mfm_all.loc[date_2018Jun_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2018Jul_end] / mfm_all.loc[date_2018Jul_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2017_end] / mfm_all.loc[date_2017_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_2016_end] / mfm_all.loc[date_2016_start] - 1], axis=1)
        mfm_all_return = pd.concat([mfm_all_return,
                                    mfm_all.loc[date_lastweek_end] / mfm_all.loc[date_lastweek_start] - 1], axis=1)

        mfm_all_return.columns = ['Since Inception', '2 Years', '1 Year', 'YTD', '2018 Jan.', '2018 Feb.', '2018 Mar.',
                                  '2018 Apr.', '2018 May', '2018 Jun.', '2018 Jul.', '2017', '2016', 'last week']

        mfm_all_return_calendar = mfm_all_return[
            ['last week', '2018 Jul.', '2018 Jun.', '2018 May', '2018 Apr.', '2018 Mar.', '2018 Feb.', '2018 Jan.', 'YTD', '2017',
             '2016']]
        mfm_all_return = mfm_all_return[['Since Inception', '2 Years', '1 Year']]

        # Risk Metrics
        df_dict = []
        for ts_name in mfm_all.columns:
            print ts_name
            df = summary_from_series(mfm_all[ts_name])
            df.index = [ts_name]
            df_dict.append(df)

        risk_metrics = pd.concat(df_dict)

        # MFM constituents analysis
        underlying_stocks = pd.read_csv(report_name + '/underlying_stocks.csv', index_col=0)

        # sector
        sector_map = pd.read_csv('common/sector_sw.csv', index_col=0)
        sector_gics_map = pd.read_csv('common/sector_gics.csv', index_col=0)
        chinese_name = pd.read_csv('common/chinese_name.csv', index_col=0)
        underlying_stocks = underlying_stocks.join(sector_map)
        underlying_stocks = underlying_stocks.join(sector_gics_map)
        underlying_stocks = underlying_stocks.join(chinese_name)
        # underlying_stocks = underlying_stocks.join(bm_underlying_stock_return)
        selected_sector = np.unique(underlying_stocks['sector_ShenWan'])
        selected_sector_gics = np.unique(underlying_stocks['sector_GICS'])

        # ShenWan
        sector_return_dict = {}

        for sector in selected_sector:
            print sector
            sector_stock = underlying_stocks.loc[underlying_stocks['sector_ShenWan'] == sector]
            # sector_stock = sector_stock.join(bm_underlying_stock_return)
            sector_return = sum(sector_stock['weight'] * 1.)
            # sector_return = sum(sector_stock['weight'] * sector_stock['return'])
            sector_return_dict[sector] = sector_return

        print sector_return_dict
        sector_return_df = pd.DataFrame(pd.Series(sector_return_dict, name='weight'))
        sector_return_df = sector_return_df['weight'].sort_values(ascending=False)
        sector_return_df = sector_return_df.apply(lambda x: str(np.round(x * 100, 4)) + '%')
        sector_return_df = pd.DataFrame(sector_return_df, columns=['weight'])

        # GICS
        sector_return_gics_dict = {}

        for sector in selected_sector_gics:
            print sector
            sector_stock = underlying_stocks.loc[underlying_stocks['sector_GICS'] == sector]
            # sector_stock = sector_stock.join(bm_underlying_stock_return)
            sector_return = sum(sector_stock['weight'] * 1.)
            # sector_return = sum(sector_stock['weight'] * sector_stock['return'])
            sector_return_gics_dict[sector] = sector_return

        print sector_return_gics_dict
        sector_return_gics_df = pd.DataFrame(pd.Series(sector_return_gics_dict, name='weight'))
        sector_return_gics_df = sector_return_gics_df['weight'].sort_values(ascending=False)
        # special treatment, delete later
        random_vec = np.random.rand(sector_return_gics_df.shape[0]) * (-0.02) + 1
        sector_return_gics_df = pd.Series(sector_return_gics_df.values * random_vec, index=sector_return_gics_df.index)
        sector_return_gics_df.iloc[-1] = 1 - sector_return_gics_df.iloc[:-1].sum()
        sector_return_gics_df = sector_return_gics_df.sort_values(ascending=False)

        sector_return_gics_df = sector_return_gics_df.apply(lambda x: str(np.round(x * 100, 2)) + '%')
        sector_return_gics_df = pd.DataFrame(sector_return_gics_df, columns=['weight'])

        # underlying_stocks = underlying_stocks.sort_values('sector')
        underlying_stocks = underlying_stocks.sort_values('weight', ascending=False)
        underlying_stocks['weight'] = underlying_stocks['weight'].apply(lambda x: str(np.round(x * 100, 2)) + '%')
        # underlying_stocks['return'] = underlying_stocks['return'].apply(lambda x: str(np.round(x,4)*100)+'%')
        underlying_stocks.index.name = None
        underlying_stocks = underlying_stocks[['name', 'sector_ShenWan', 'weight']]
        underlying_stocks = underlying_stocks.head(10)  # top 10

        underlying_stocks = underlying_stocks.join(sector_gics_map)  # add GICS
        # underlying_stocks = underlying_stocks[['name','sector_GICS','weight']]
        underlying_stocks = underlying_stocks[['name', 'sector_GICS']]
        # underlying_stocks = underlying_stocks[['name','sector_GICS','sector_ShenWan','weight']]

        # asset type breakdown
        asset_type = pd.DataFrame(np.array([['55%', '10%', '10%', '5%', '20%']]).T,
                                  index=['A-share Equity', 'Bond Fund', 'Money Market Fund', 'Gold Fund',
                                         'QDII Equity Fund'],
                                  columns=['weight'])
        print asset_type.to_string()

        ############# Generate Report ##############
        env = Environment(loader=FileSystemLoader('.'))
        template = env.get_template(report_name + '/' + "template.html")

        template_vars = {"title": "MFM report",
                         "performance_mfm": mfm_all_return.applymap(lambda x: ('%.2f' % (x * 100)) + '%').to_html(),
                         "performance_mfm_calendar": mfm_all_return_calendar.applymap(
                             lambda x: ('%.2f' % (x * 100)) + '%').to_html(),
                         "risk_metrics": risk_metrics.to_html(),
                         "underlying_performance": underlying_stocks.to_html(),
                         "sector_allocation_sw": sector_return_df.to_html(),
                         "sector_allocation_gics": sector_return_gics_df.to_html(),
                         "asset_type": asset_type.to_html()
                         }

        html_out = template.render(template_vars)
        f = open(report_name+'/report.html', 'w')
        f.write(html_out)
        f.close()

        # files
        localpath_dir = '/Users/jason/Documents/Magnum Files/Magnum Projects (Win-TPT)/Multi-factor Model/algo recap (weekly)/2018-07-09/'
        localpath_dir = localpath_dir + report_name
        url_dir = '/data/release/chartsite/weekly_recap/20180709/' + report_name + '/'
        upload(host='chart.aqumon.com', port=22, username='aqumon', password='algo1234',
               localpath=localpath_dir + '/report.html',
               path=url_dir + 'report.html')

        # images
        upload(host='chart.aqumon.com', port=22, username='aqumon', password='algo1234',
               localpath=localpath_dir + '/portfolio_mfm.png',
               path=url_dir + 'portfolio_mfm.png')
