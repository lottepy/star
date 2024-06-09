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

CN_STOCK_MAP = {
	'601888 CH Equity': '中国国旅',
	'600009 CH Equity': '上海机场',
	'600900 CH Equity': '长江电力',
	'300015 CH Equity': '爱尔眼科',
	'600519 CH Equity': '贵州茅台',
	'CSIR0300 Index': '沪深300',
}

HK_STOCK_MAP = {
	'1299 HK Equity': '友邦保险',
	'2388 HK Equity': '中银香港',
	'2588 HK Equity': '中银航空租赁',
	'3613 HK Equity': '同仁堂国药',
	'700 HK Equity': '腾讯控股',
	'778 HK Equity': '置富产业信托',
	'11 HK Equity': '恒生银行',
	'345 HK Equity': '维他奶国际',
	'HSI 1 Index': '恒生指数',
}

US_STOCK_MAP = {
	'AAPL US Equity': 'Apple Inc.',
	'BABA US Equity': 'Alibab Group Holding',
	'BRK/B US Equity': 'Berkshire Hathaway Inc.',
	'COST US Equity': 'Costco Wholesale Corp.',
	'DPZ US Equity': "Domino's Pizza Inc.",
	'MCO US Equity': "Moody's Corp.",
	'TSM US Equity': 'Taiwan Semiconductor Manufacturing Co Ltd.',
	'SPXT Index': 'S&P 500 Index',
}

OTHER_STOCK_MAP = {
	'AIA AU Equity': 'Auckland International Airport',
	'AIA NZ Equity': 'Auckland International Airport',
	'HKL SP Equity': 'Hongkong Land Holdings',
	'SATS SP Equity': 'SATS Ltd.',
	'SYD AU Equity': 'Sydney Airport',
	'NDUEACWZ Index': 'MSCI ACWI ex USA',
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
	risk_metrics = pd.read_csv('bochkam_internal/pv_ts_sg_stats.csv', index_col=0)
	complete_performance = pd.read_csv('bochkam_internal/nav_performance.csv', index_col=0, parse_dates=True)
	olmar_performance = pd.read_csv('bochkam_internal/olmar_performance.csv', index_col=0, parse_dates=True)
	holdings = pd.read_csv('bochkam_internal/holdings.csv')
	rotation_holdings = pd.read_csv('bochkam_internal/rotation_holdings.csv')
	regional_dis = pd.read_csv('bochkam_internal/regional_dis.csv')
	duration = pd.read_csv('bochkam_internal/duration.csv')

	env = Environment(loader=FileSystemLoader('.'))
	template = env.get_template('bochkam_internal/template.html')

	start = '2018-12-31'
	end = '2019-01-04'
	weekly_start = '2018/12/31'

	# 制作表现图并保存
	complete_performance.plot()
	# plt.show()
	plt.savefig('bochkam_internal/complete_performance.png', format='png')
	olmar_performance[['Sector Rotation', 'Sector Equal Weight']].plot()
	plt.savefig('bochkam_internal/olmar_sector.png', format='png')
	olmar_performance[['Region Rotation', 'Region Equal Weight']].plot()
	plt.savefig('bochkam_internal/olmar_region.png', format='png')


	sector_list = ['XLB', 'XLE', 'XLI', 'XLF', 'XLK', 'XLP', 'XLU', 'XLV', 'XLY', 'VNQ', 'IVV']
	region_list = ['ASHR', 'EWA', 'EWC', 'EWJ', 'EWG', 'EWH', 'EWU', 'EWY', 'EWZ', 'INDA', 'EWL', 'ACWX']
	sector_ticker = [s + ' US Equity' for s in sector_list]
	region_ticker = [s + ' US Equity' for s in region_list]

	sector_asset_return = compute_return(start, end, sector_ticker)
	region_asset_return = compute_return(start, end, region_ticker)

	olmar_return = (olmar_performance.iloc[-1] / olmar_performance.iloc[0]).sub(1.)
	sector_asset_return['Equal Weight'] = olmar_return['Sector Equal Weight']
	sector_asset_return['Sector Rotation'] = olmar_return['Sector Rotation']
	region_asset_return['Equal Weight'] = olmar_return['Region Equal Weight']
	region_asset_return['Region Rotation'] = olmar_return['Region Rotation']

	# 一周表现
	sector_asset_weekly_return = compute_return(weekly_start, end, sector_ticker)
	region_asset_weekly_return = compute_return(weekly_start, end, region_ticker)

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

	sector_asset_return['Weekly Return'] = sector_asset_weekly_return['Return']
	region_asset_return['Weekly Return'] = region_asset_weekly_return['Return']

	china_list = ['601888 CH Equity', '600009 CH Equity', '600900 CH Equity',
				  '300015 CH Equity', '600519 CH Equity', 'CSIR0300 Index']
	hk_list = ['1299 HK Equity', '2388 HK Equity', '2588 HK Equity', '3613 HK Equity',
			   '11 HK Equity', '778 HK Equity', '345 HK Equity', 'HSI 1 Index']
	us_list = ['AAPL US Equity', 'TSM US Equity', 'BRK/B US Equity', 'DIS US Equity',
			   'COST US Equity', 'DPZ US Equity', 'MCO US Equity', 'SPXT Index']
	other_list = ['AIA AU Equity', 'AIA NZ Equity', 'SATS SP Equity', 'NDUEACWZ Index']

	start = '2018-12-31'
	end = '2019-01-04'

	china_return = compute_return(start, end, china_list).rename(CN_STOCK_MAP).to_frame(name='Return from 11/30').applymap('{:.2%}'.format)
	hk_return = compute_return(start, end, hk_list).rename(HK_STOCK_MAP).to_frame(name='Return from 11/30').applymap('{:.2%}'.format)
	us_return = compute_return(start, end, us_list).rename(US_STOCK_MAP).to_frame(name='Return from 11/30').applymap('{:.2%}'.format)
	other_return = compute_return(start, end, other_list).rename(OTHER_STOCK_MAP).to_frame(name='Return from 11/30').applymap('{:.2%}'.format)


	template_vars = {
		"title": "AQUMON-BOCHKAM",
		"risk_metrics": risk_metrics.to_html(),
		"sector_returns": sector_asset_return.to_html(),
		"region_returns": region_asset_return.to_html(),
		"holdings": holdings.to_html(index=False),
		"rotation_holdings": rotation_holdings.to_html(index=False),
		"regional_dis": regional_dis.to_html(index=False),
		"duration": duration.to_html(index=False),
		'china_return': china_return.to_html(),
		'hk_return': hk_return.to_html(),
		'us_return': us_return.to_html(),
		'other_return': other_return.to_html()
	}

	html_out = template.render(template_vars)
	f = open('bochkam_internal/report.html', 'w')
	f.write(html_out)
	f.close()

	# files
	localpath_dir = 'bochkam_internal/'
	# url_dir = '/home/aqumon/www/chart.aqumon.com/BOCHKAM-internal/20181231/'
	url_dir = '/home/aqumon/www/chart.aqumon.com/BOCHKAM-internal/' \
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
		localpath=localpath_dir + '/investment_framework.png',
		path=url_dir + 'investment_framework.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/olmar_sector.png',
		path=url_dir + 'olmar_sector.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/olmar_region.png',
		path=url_dir + 'olmar_region.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/china_stock.png',
		path=url_dir + 'china_stock.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/hk_stock.png',
		path=url_dir + 'hk_stock.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/us_stock.png',
		path=url_dir + 'us_stock.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/other_stock.png',
		path=url_dir + 'other_stock.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='algo1234',
		localpath=localpath_dir + '/fund_pctl.png',
		path=url_dir + 'fund_pctl.png'
	)