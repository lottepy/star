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
	risk_metrics_sgme_3y = pd.read_csv('huabao/pv_ts_sg_stats_sgme_3y.csv', index_col=0)
	risk_metrics_sgm_3y = pd.read_csv('huabao/pv_ts_sg_stats_sgm_3y.csv', index_col=0)
	complete_performance_sgme_3y = pd.read_csv('huabao/complete_performance_sgme_3y.csv', index_col=0, parse_dates=True)
	complete_performance_sgm_3y = pd.read_csv('huabao/complete_performance_sgm_3y.csv', index_col=0, parse_dates=True)
	holdings_sgm = pd.read_csv('huabao/holdings_sgm.csv')
	holdings_sgme = pd.read_csv('huabao/holdings_sgme.csv')
	monthly_return_sgm = pd.read_csv('huabao/monthly_return_sgm.csv', index_col=0)
	monthly_return_sgme = pd.read_csv('huabao/monthly_return_sgme.csv', index_col=0)
	monthly_excess_return_sgm = pd.read_csv('huabao/monthly_excess_return_sgm.csv', index_col=0)
	monthly_excess_return_sgme = pd.read_csv('huabao/monthly_excess_return_sgme.csv', index_col=0)

	env = Environment(loader=FileSystemLoader('.'))
	template = env.get_template('huabao/template.html')

	start = '2018-12-31'
	end = '2019-11-01'
	weekly_start = '2019-02-22'

	# 制作表现图并保存
	complete_performance_sgme_3y.plot(color=['#EE2F79', '#00ABC0', '#006EDC'])
	plt.savefig('huabao/complete_performance_sgme_3y.png', format='png')
	complete_performance_sgm_3y.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('huabao/complete_performance_sgm_3y.png', format='png')

	risk_metrics_sgme_3y = risk_metrics_sgme_3y.applymap('{:.2%}'.format)
	risk_metrics_sgm_3y = risk_metrics_sgm_3y.applymap('{:.2%}'.format)
	# holdings_sgm = holdings_sgm.applymap({'Weight': '{:.2%}'.format})

	template_vars = {
		"title": "AQUMON-hwabao",
		"risk_metrics_sgme_3y": risk_metrics_sgme_3y.to_html(),
		"risk_metrics_sgm_3y": risk_metrics_sgm_3y.to_html(),
		"holdings_sgme": holdings_sgme.to_html(index=False),
		"holdings_sgm": holdings_sgm.to_html(index=False),
		"monthly_ret_sgm": monthly_return_sgm.to_html(),
		"monthly_ret_sgme": monthly_return_sgme.to_html(),
		"monthly_excess_ret_sgm": monthly_excess_return_sgm.to_html(),
		"monthly_excess_ret_sgme": monthly_excess_return_sgme.to_html(),
	}

	html_out = template.render(template_vars)
	f = open('huabao/report.html', 'w')
	f.write(html_out)
	f.close()

	# files
	localpath_dir = 'huabao/'
	# url_dir = '/home/aqumon/www/chart.aqumon.com/BOCHKAM/20190107/'
	url_dir = '/home/aqumon/www/chart.aqumon.com/2B-materials/Hwabao/' \
			  + end.replace('-', '') + '/'
	
	element_set = ['report.html',
				   'complete_performance_sgme_3y.png', 'complete_performance_sgm_3y.png']
	for e in element_set:
		upload(
			host='chart.aqumon.com',
			port=22,
			username='aqumon',
			password='aqumon',
			localpath=localpath_dir + '/' + e,
			path=url_dir + e
		)


