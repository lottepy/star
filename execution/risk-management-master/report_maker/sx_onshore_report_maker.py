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
from jinja2 import Environment, FileSystemLoader
from utils.bbg_downloader import download_his
import seaborn as sns
sns.set_style('whitegrid')
# sns.set_palette('Set4')

SEC_MAP = {
	'XLB US Equity': 'Basic Materials',
	'XLE US Equity': 'Energy',
	'XLI US Equity': 'Industrials',
	'XLF US Equity': 'Finance',
	'XLK US Equity': 'Technology',
	'XLP US Equity': 'Consumer Staples',
	'XLU US Equity': 'Utilities',
	'XLV US Equity': 'Health Care',
	'XLY US Equity': 'Consumer Discretionary',
	'VNQ US Equity': 'Real Estate',
	'IVV US Equity': 'S&P 500 Index ETF'
}

REG_MAP = {
	'ASHR US Equity': 'China',
	'EWA US Equity': 'Australia',
	'EWC US Equity': 'Canada',
	'EWJ US Equity': 'Japan',
	'EWG US Equity': 'Germany',
	'EWH US Equity': 'Hong Kong',
	'EWU US Equity': 'UK',
	'EWY US Equity': 'South Korea',
	'EWZ US Equity': 'Brazil',
	'INDA US Equity': 'India',
	'EWL US Equity': 'Switzerland',
	'ACWX US Equity': 'All Country Ex-US ETF'
}


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


def compute_return(start, end, assets):
	data = download_his(assets, ['PX_LAST'], start, end, period='DAILY', currency='USD')
	data.columns = data.columns.get_level_values(0)
	data = data[assets]
	period_return = (data.iloc[-1] / data.iloc[0]).sub(1.)
	return period_return


if __name__ == '__main__':
	risk_metrics = pd.read_csv('sx_onshore/pv_ts_sg_stats.csv', index_col=0)
	risk_metrics_2018 = pd.read_csv('sx_onshore/pv_ts_sg_stats_2018.csv', index_col=0)
	complete_performance = pd.read_csv('sx_onshore/complete_performance.csv', index_col=0, parse_dates=True)
	complete_performance_2018 = pd.read_csv('sx_onshore/complete_performance_2018.csv', index_col=0, parse_dates=True)
	holdings = pd.read_csv('sx_onshore/holdings.csv')
	# rotation_holdings = pd.read_csv('bochkam/rotation_holdings.csv')
	# regional_dis = pd.read_csv('sx_onshore/regional_dis.csv')
	# duration = pd.read_csv('sx_onshore/duration.csv')

	env = Environment(loader=FileSystemLoader('.'))
	template = env.get_template('sx_onshore/template.html')

	start = '2018-12-31'
	end = '2019-03-01'
	weekly_start = '2019-02-22'

	# 制作表现图并保存
	complete_performance.plot()
	plt.savefig('sx_onshore/complete_performance.png', format='png')
	complete_performance_2018.plot()
	plt.savefig('sx_onshore/complete_performance_2018.png', format='png')

	risk_metrics = risk_metrics.applymap('{:.2%}'.format)
	risk_metrics_2018 = risk_metrics_2018.applymap('{:.2%}'.format)
	# holdings = holdings.applymap({'Weight': '{:.2%}'.format})


	template_vars = {
		"title": "AQUMON-XXX",
		"risk_metrics": risk_metrics.to_html(),
		"risk_metrics_2018": risk_metrics_2018.to_html(),
		"holdings": holdings.to_html(index=False),
		# "regional_dis": regional_dis.to_html(index=False),
		# "duration": duration.to_html(index=False)
	}

	html_out = template.render(template_vars)
	f = open('sx_onshore/report.html', 'w')
	f.write(html_out)
	f.close()

	# files
	localpath_dir = 'sx_onshore/'
	# url_dir = '/home/aqumon/www/chart.aqumon.com/BOCHKAM/20190107/'
	url_dir = '/home/aqumon/www/chart.aqumon.com/2B-materials/SX-onshore/' \
			  + end.replace('-', '') + '/'
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/report.html',
		path=url_dir + 'report.html'
	)

	# images
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/complete_performance.png',
		path=url_dir + 'complete_performance.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/complete_performance_2018.png',
		path=url_dir + 'complete_performance_2018.png'
	)

