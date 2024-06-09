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
	risk_metrics_sgm_ytd = pd.read_csv('youzheng/pv_ts_sg_stats_sgm_ytd.csv', index_col=0)
	risk_metrics_sgm_3y = pd.read_csv('youzheng/pv_ts_sg_stats_sgm_3y.csv', index_col=0)
	complete_performance_sgm_ytd = pd.read_csv('youzheng/complete_performance_sgm_ytd.csv', index_col=0, parse_dates=True)
	complete_performance_sgm_3y = pd.read_csv('youzheng/complete_performance_sgm_3y.csv', index_col=0, parse_dates=True)
	holdings_sgm = pd.read_csv('youzheng/holdings_sgm.csv')

	risk_metrics_sx_ytd = pd.read_csv('youzheng/pv_ts_sg_stats_sx_ytd.csv', index_col=0)
	risk_metrics_sx_3y = pd.read_csv('youzheng/pv_ts_sg_stats_sx_3y.csv', index_col=0)
	complete_performance_sx_ytd = pd.read_csv('youzheng/complete_performance_sx_ytd.csv', index_col=0, parse_dates=True)
	complete_performance_sx_3y = pd.read_csv('youzheng/complete_performance_sx_3y.csv', index_col=0, parse_dates=True)
	holdings_sx = pd.read_csv('youzheng/holdings_sx.csv')

	risk_metrics_sd_ytd = pd.read_csv('youzheng/pv_ts_sg_stats_sd_ytd.csv', index_col=0)
	risk_metrics_sd_3y = pd.read_csv('youzheng/pv_ts_sg_stats_sd_3y.csv', index_col=0)
	complete_performance_sd_ytd = pd.read_csv('youzheng/complete_performance_sd_ytd.csv', index_col=0, parse_dates=True)
	complete_performance_sd_3y = pd.read_csv('youzheng/complete_performance_sd_3y.csv', index_col=0, parse_dates=True)
	holdings_sd = pd.read_csv('youzheng/holdings_sd.csv')

	risk_metrics_ahas_ytd = pd.read_csv('youzheng/pv_ts_sg_stats_ahas_ytd.csv', index_col=0)
	risk_metrics_ahas_3y = pd.read_csv('youzheng/pv_ts_sg_stats_ahas_3y.csv', index_col=0)
	complete_performance_ahas_ytd = pd.read_csv('youzheng/complete_performance_ahas_ytd.csv', index_col=0, parse_dates=True)
	complete_performance_ahas_3y = pd.read_csv('youzheng/complete_performance_ahas_3y.csv', index_col=0, parse_dates=True)
	holdings_ahas = pd.read_csv('youzheng/holdings_ahas.csv', encoding='utf_8_sig')

	env = Environment(loader=FileSystemLoader('.'))
	template = env.get_template('youzheng/template.html')

	start = '2018-12-31'
	end = '2019-04-01'
	weekly_start = '2019-02-22'

	# 制作表现图并保存
	complete_performance_sgm_ytd.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('youzheng/complete_performance_sgm_ytd.png', format='png')
	complete_performance_sgm_3y.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('youzheng/complete_performance_sgm_3y.png', format='png')

	complete_performance_sx_ytd.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('youzheng/complete_performance_sx_ytd.png', format='png')
	complete_performance_sx_3y.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('youzheng/complete_performance_sx_3y.png', format='png')

	complete_performance_sd_ytd.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('youzheng/complete_performance_sd_ytd.png', format='png')
	complete_performance_sd_3y.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('youzheng/complete_performance_sd_3y.png', format='png')

	complete_performance_ahas_ytd.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('youzheng/complete_performance_ahas_ytd.png', format='png')
	complete_performance_ahas_3y.plot(color=['#EE2F79', '#00ABC0'])
	plt.savefig('youzheng/complete_performance_ahas_3y.png', format='png')

	risk_metrics_sgm_ytd = risk_metrics_sgm_ytd.applymap('{:.2%}'.format)
	risk_metrics_sgm_3y = risk_metrics_sgm_3y.applymap('{:.2%}'.format)
	# holdings_sgm = holdings_sgm.applymap({'Weight': '{:.2%}'.format})

	risk_metrics_sx_ytd = risk_metrics_sx_ytd.applymap('{:.2%}'.format)
	risk_metrics_sx_3y = risk_metrics_sx_3y.applymap('{:.2%}'.format)
	# holdings_sx = holdings_sx.applymap({'Weight': '{:.2%}'.format})

	risk_metrics_sd_ytd = risk_metrics_sd_ytd.applymap('{:.2%}'.format)
	risk_metrics_sd_3y = risk_metrics_sd_3y.applymap('{:.2%}'.format)
	# holdings_sd = holdings_sd.applymap({'Weight': '{:.2%}'.format})

	risk_metrics_ahas_ytd = risk_metrics_ahas_ytd.applymap('{:.2%}'.format)
	risk_metrics_ahas_3y = risk_metrics_ahas_3y.applymap('{:.2%}'.format)
	# holdings_sd = holdings_sd.applymap({'Weight': '{:.2%}'.format})

	template_vars = {
		"title": "AQUMON-Youzheng",
		"risk_metrics_sgm_ytd": risk_metrics_sgm_ytd.to_html(),
		"risk_metrics_sgm_3y": risk_metrics_sgm_3y.to_html(),
		"holdings_sgm": holdings_sgm.to_html(index=False),
		"risk_metrics_sx_ytd": risk_metrics_sx_ytd.to_html(),
		"risk_metrics_sx_3y": risk_metrics_sx_3y.to_html(),
		"holdings_sx": holdings_sx.to_html(index=False),
		"risk_metrics_sd_ytd": risk_metrics_sd_ytd.to_html(),
		"risk_metrics_sd_3y": risk_metrics_sd_3y.to_html(),
		"holdings_sd": holdings_sd.to_html(index=False),
		"risk_metrics_ahas_ytd": risk_metrics_ahas_ytd.to_html(),
		"risk_metrics_ahas_3y": risk_metrics_ahas_3y.to_html(),
		"holdings_ahas": holdings_ahas.to_html(index=False)
	}

	html_out = template.render(template_vars)
	f = open('youzheng/report.html', 'w')
	f.write(html_out)
	f.close()

	# files
	localpath_dir = 'youzheng/'
	# url_dir = '/home/aqumon/www/chart.aqumon.com/BOCHKAM/20190107/'
	url_dir = '/home/aqumon/www/chart.aqumon.com/2B-materials/Youzheng/' \
			  + end.replace('-', '') + '/'
	
	element_set = ['report.html',
				   'complete_performance_sgm_ytd.png', 'complete_performance_sgm_3y.png',
				   'complete_performance_sx_ytd.png', 'complete_performance_sx_3y.png',
				   'complete_performance_sd_ytd.png', 'complete_performance_sd_3y.png',
				   'complete_performance_ahas_ytd.png', 'complete_performance_ahas_3y.png']
	for e in element_set:
		upload(
			host='chart.aqumon.com',
			port=22,
			username='aqumon',
			password='algo1234',
			localpath=localpath_dir + '/' + e,
			path=url_dir + e
		)
	
	# upload(
	# 	host='chart.aqumon.com',
	# 	port=22,
	# 	username='aqumon',
	# 	password='algo1234',
	# 	localpath=localpath_dir + '/report.html',
	# 	path=url_dir + 'report.html'
	# )
	#
	# # images
	# upload(
	# 	host='chart.aqumon.com',
	# 	port=22,
	# 	username='aqumon',
	# 	password='algo1234',
	# 	localpath=localpath_dir + '/complete_performance.png',
	# 	path=url_dir + 'complete_performance.png'
	# )
	# upload(
	# 	host='chart.aqumon.com',
	# 	port=22,
	# 	username='aqumon',
	# 	password='algo1234',
	# 	localpath=localpath_dir + '/complete_performance_2018.png',
	# 	path=url_dir + 'complete_performance_2018.png'
	# )

