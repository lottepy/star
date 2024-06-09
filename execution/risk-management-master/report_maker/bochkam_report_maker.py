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
	risk_metrics = pd.read_csv('bochkam/pv_ts_sg_stats.csv', index_col=0)
	complete_performance = pd.read_csv('bochkam/complete_performance.csv', index_col=0, parse_dates=True)
	olmar_performance = pd.read_csv('bochkam/olmar_performance.csv', index_col=0, parse_dates=True)
	holdings = pd.read_csv('bochkam/holdings.csv')
	rotation_holdings = pd.read_csv('bochkam/rotation_holdings.csv')
	regional_dis = pd.read_csv('bochkam/regional_dis.csv')
	duration = pd.read_csv('bochkam/duration.csv')

	env = Environment(loader=FileSystemLoader('.'))
	template = env.get_template('bochkam/template.html')

	start = '2018-12-31'
	end = '2019-11-01'
	weekly_start = '2019-10-25'

	# 制作表现图并保存
	complete_performance.plot(color=['#EE2F79', '#00ABC0'])
	# plt.show()
	plt.savefig('bochkam/complete_performance.png', format='png')
	olmar_performance[['Sector Rotation', 'Sector Equal Weight']].plot(
		color=['#EE2F79', '#00ABC0']
	)
	plt.savefig('bochkam/olmar_sector.png', format='png')
	olmar_performance[['Region Rotation', 'Region Equal Weight']].plot(
		color=['#EE2F79', '#00ABC0']
	)
	plt.savefig('bochkam/olmar_region.png', format='png')


	sector_list = ['XLB', 'XLE', 'XLI', 'XLF', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'VNQ', 'IVV']
	region_list = ['ASHR', 'EWA', 'EWC', 'EWJ', 'EWG', 'EWH', 'EWU', 'EWY', 'EWZ', 'INDA', 'EWL', 'ACWX']
	sector_ticker = [s + ' US Equity' for s in sector_list]
	region_ticker = [s + ' US Equity' for s in region_list]

	sector_asset_return = compute_return(start, end, sector_ticker)
	region_asset_return = compute_return(start, end, region_ticker)

	# 临时使用csv
	# sector_asset_return = pd.Series.from_csv('bochkam/sector_return.csv')
	# region_asset_return = pd.Series.from_csv('bochkam/region_return.csv')

	olmar_return = (olmar_performance.iloc[-1] / olmar_performance.iloc[0]).sub(1.)
	sector_asset_return['Equal Weight'] = olmar_return['Sector Equal Weight']
	sector_asset_return['Sector Rotation'] = olmar_return['Sector Rotation']
	region_asset_return['Equal Weight'] = olmar_return['Region Equal Weight']
	region_asset_return['Region Rotation'] = olmar_return['Region Rotation']

	# 一周表现
	sector_asset_weekly_return = compute_return(weekly_start, end, sector_ticker)
	region_asset_weekly_return = compute_return(weekly_start, end, region_ticker)

	# 临时使用csv
	# sector_asset_weekly_return = pd.Series.from_csv('bochkam/sector_weekly_return.csv')
	# region_asset_weekly_return = pd.Series.from_csv('bochkam/region_weekly_return.csv')

	olmar_weekly_return = (olmar_performance.iloc[-1] / olmar_performance.loc[weekly_start]).sub(1.)
	sector_asset_weekly_return['Equal Weight'] = olmar_weekly_return['Sector Equal Weight']
	sector_asset_weekly_return['Sector Rotation'] = olmar_weekly_return['Sector Rotation']
	region_asset_weekly_return['Equal Weight'] = olmar_weekly_return['Region Equal Weight']
	region_asset_weekly_return['Region Rotation'] = olmar_weekly_return['Region Rotation']

	sector_asset_return = sector_asset_return.rename(SEC_MAP).to_frame(name='YTD Return')
	region_asset_return = region_asset_return.rename(REG_MAP).to_frame(name='YTD Return')
	sector_asset_weekly_return = sector_asset_weekly_return.rename(SEC_MAP).to_frame(name='Return')
	region_asset_weekly_return = region_asset_weekly_return.rename(REG_MAP).to_frame(name='Return')

	sector_asset_return = sector_asset_return.applymap('{:.2%}'.format)
	region_asset_return = region_asset_return.applymap('{:.2%}'.format)
	sector_asset_weekly_return = sector_asset_weekly_return.applymap('{:.2%}'.format)
	region_asset_weekly_return = region_asset_weekly_return.applymap('{:.2%}'.format)
	risk_metrics = risk_metrics.applymap('{:.2%}'.format)
	# holdings = holdings.applymap({'Weight': '{:.2%}'.format})

	# sector_asset_return['Weekly Return'] = sector_asset_weekly_return['Return']
	# region_asset_return['Weekly Return'] = region_asset_weekly_return['Return']
	sector_asset_return['Monthly Return'] = sector_asset_weekly_return['Return']
	region_asset_return['Monthly Return'] = region_asset_weekly_return['Return']


	template_vars = {
		"title": "AQUMON-BOCHKAM",
		"risk_metrics": risk_metrics.to_html(),
		"sector_returns": sector_asset_return.to_html(),
		"region_returns": region_asset_return.to_html(),
		"holdings": holdings.to_html(index=False),
		"rotation_holdings": rotation_holdings.to_html(index=False),
		"regional_dis": regional_dis.to_html(index=False),
		"duration": duration.to_html(index=False)
	}

	html_out = template.render(template_vars)
	f = open('bochkam/report.html', 'w')
	f.write(html_out)
	f.close()

	# files
	localpath_dir = 'bochkam/'
	# url_dir = '/home/aqumon/www/chart.aqumon.com/BOCHKAM/20190107/'
	url_dir = '/home/aqumon/www/chart.aqumon.com/BOCHKAM/' \
			  + end.replace('-', '') + '/'
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='aqumon',
		localpath=localpath_dir + '/report.html',
		path=url_dir + 'report.html'
	)

	# images
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='aqumon',
		localpath=localpath_dir + '/complete_performance.png',
		path=url_dir + 'complete_performance.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='aqumon',
		localpath=localpath_dir + '/investment_framework.png',
		path=url_dir + 'investment_framework.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='aqumon',
		localpath=localpath_dir + '/olmar_sector.png',
		path=url_dir + 'olmar_sector.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='aqumon',
		localpath=localpath_dir + '/olmar_region.png',
		path=url_dir + 'olmar_region.png'
	)
