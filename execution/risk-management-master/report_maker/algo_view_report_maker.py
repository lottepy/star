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

	macro_indicator = pd.read_csv('algo_view/macro_indicator.csv')
	bear_prob = pd.read_csv('algo_view/bear_prob.csv')
	sector_select = pd.read_csv('algo_view/sector_select.csv')
	region_select = pd.read_csv('algo_view/region_select.csv')
	vix_data = pd.read_csv('algo_view/vix_data.csv')
	vix_figure = pd.read_csv('algo_view/vix_figure.csv', index_col=0, parse_dates=True)
	acas_list = pd.read_csv('algo_view/acas_list.csv')
	acas_factor = pd.read_csv('algo_view/acas_factor.csv')
	acas_distribution = pd.read_csv('algo_view/acas_distribution.csv', index_col=0)
	ahas_list = pd.read_csv('algo_view/ahas_list.csv')
	ahas_factor = pd.read_csv('algo_view/ahas_factor.csv')
	ahas_distribution = pd.read_csv('algo_view/ahas_distribution.csv', index_col=0)

	env = Environment(loader=FileSystemLoader('.'))
	template = env.get_template('algo_view/template.html')

	vix_figure.plot()
	plt.savefig('algo_view/vix_figure.png', format='png')
	acas_distribution.plot.barh(rot=0)
	plt.savefig('algo_view/acas_distribution.png', format='png')
	ahas_distribution.plot.barh(rot=0)
	plt.savefig('algo_view/ahas_distribution.png', format='png')

	template_vars = {
		"title": "AQUMON-VIEW",
		"macro_indicator": macro_indicator.to_html(index=False),
		"bear_prob": bear_prob.to_html(index=False),
		"sector_select": sector_select.to_html(index=False),
		"region_select": region_select.to_html(index=False),
		"vix_data": vix_data.to_html(index=False),
		"acas_list": acas_list.to_html(index=False),
		"acas_factor": acas_factor.to_html(index=False),
		"ahas_list": ahas_list.to_html(index=False),
		"ahas_factor": ahas_factor.to_html(index=False)
	}

	html_out = template.render(template_vars)
	f = open('algo_view/report.html', 'w')
	f.write(html_out)
	f.close()

	# files
	localpath_dir = 'algo_view/'
	url_dir = '/home/aqumon/www/chart.aqumon.com/algo-view/sample/'
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
		localpath=localpath_dir + '/vix_figure.png',
		path=url_dir + 'vix_figure.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/acas_distribution.png',
		path=url_dir + 'acas_distribution.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/ahas_distribution.png',
		path=url_dir + 'ahas_distribution.png'
	)
