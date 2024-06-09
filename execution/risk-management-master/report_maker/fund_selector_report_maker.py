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


account_overview = pd.DataFrame(
	[['1,014,398.26', 1.44, 2.48],
	 ['1,021,850.99', 2.19, 2.29],
	 ['1,023,446.13', 2.34, 2.19]],
	index=['Conservative', 'Balanced', 'Aggressive'],
	columns=['Account Value (USD)', 'Return (%)', 'Benchmark Return (%)']
)


if __name__ == '__main__':
	
	risk_metrics = pd.read_csv('fund_selector/risk.csv', index_col=0)

	env = Environment(loader=FileSystemLoader('.'))
	template = env.get_template('fund_selector/template.html')

	template_vars = {
		"title": "AQUMON",
		"account_overview": account_overview.to_html(),
		"risk_metrics_conservative": risk_metrics['Conservative'].to_frame().to_html(),
		"risk_metrics_balanced": risk_metrics['Balanced'].to_frame().to_html(),
		"risk_metrics_aggressive": risk_metrics['Aggressive'].to_frame().to_html()
	}

	html_out = template.render(template_vars)
	f = open('fund_selector/report.html', 'w')
	f.write(html_out)
	f.close()

	# files
	localpath_dir = 'fund_selector/'
	# localpath_dir = localpath_dir + 'fund_selector'
	# url_dir = '/data/release/chartsite/fund_selector/20180724/'
	url_dir = '/home/aqumon/www/chart.aqumon.com/FundSelector/performance-2019/'
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
		localpath=localpath_dir + '/allocation_conservative.png',
		path=url_dir + 'allocation_conservative.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='aqumon',
		localpath=localpath_dir + '/allocation_balanced.png',
		path=url_dir + 'allocation_balanced.png'
	)
	upload(
		host='chart.aqumon.com',
		port=22,
		username='aqumon',
		password='aqumon',
		localpath=localpath_dir + '/allocation_aggressive.png',
		path=url_dir + 'allocation_aggressive.png'
	)
