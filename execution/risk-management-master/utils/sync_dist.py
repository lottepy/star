import requests
import re
import json
import decimal
from django.core.management.base import BaseCommand
from bs4 import BeautifulSoup
from markort.models import Instrument



class Command(BaseCommand):
	dist_map = {'BasicMaterials': 'Materials',
				'CyclicalConsumer': 'Consumer',
				'Non-cyclicalConsumer': 'Consumer',
				'ConsumerSerivces': 'Consumer',
				'ConsumerDiscretionary': 'Consumer',
				'ConsumerStaples': 'Consumer',
				'ConsumerGoods': 'Consumer',
				'ConsumerCyclical': 'Consumer',
				'ConsumerNonCyclical': 'Consumer',
				'InformationTechology': 'Technology',
				'TelecommunicationServices ': 'Telecommunications',
				'HealthCare': 'Healthcare',
				'Oil&Gas': 'Energy',
				'ETFCashComponent': 'Others',
				'Other': 'Others',
				'RealEstate': 'Others',

	}
	url = {'HK': ['http://www.aastocks.com/en/ltp/rtquote.aspx?symbol=',
				  'http://www.aastocks.com/en/stocks/quote/detail-quote.aspx'],
		   'US': ['http://etfdb.com/etf/']}
	def add_arguments(self, parser):
		parser.add_argument(
			'-s',
			'--status',
			action = 'store',
			dest = 'status',
		)
		parser.add_argument(
			'-r',
			'--region',
			action = 'store',
			dest = 'region',
		)
		parser.add_argument(
			'-c',
			'--category',
			action = 'store',
			dest = 'category',
		)
	def handle(self, status, region, category, *args, **options):
		instruments = Instrument.objects.filter(status = status, region = region, category = category)
		for instrument in instruments:
			iuid = instrument.iuid
			extra_data = {}
			region, category, code = iuid.split('_')
			if region == 'HK':
				extra_data = self.handle_HK(code, extra_data)
			if region == 'US':
				extra_data = self.handle_US(code, extra_data)
			if extra_data:
				# jsondata = {}
				# jsondata[iuid]=extra_data
				# with open("../data_US.json","a") as file:
				# 	json.dump(jsondata,file,cls=DecimalEncoder)
				instrument.extra_data = extra_data
			print extra_data
			# instrument.save()

	# def get_cookie(self, code):
	# 	res = requests.get(self.url['HK'][0] + code)
	# 	cookies = requests.utils.dict_from_cookiejar(res.cookies)
	# 	return cookies

	def handle_HK(self, code, extra_data):
		s = requests.Session()
		headers = {}
		headers['Referer']=self.url['HK'][1]+'?symbol=0'+code
		s.headers = headers
		s.get(self.url['HK'][0] + code)
		scont = s.get(self.url['HK'][1])
		cont = scont.content.decode('utf-8')
		soup = BeautifulSoup(cont,'html.parser')
		data = soup.findAll('table', class_='mar15T', id='tbETFPortfolio')
		for text in data:
			text = ''.join(text.get_text().split()).encode('ascii')
			text = ''.join(re.split(r"-|,|\n| |\r|&", text)).encode('ascii')
			text = text.split('By')
			for value in text:
				if value.startswith('Sector'):
					extra_data['Sector'] = self.handle_data(value[6:])
				if value.startswith('Market'):
					extra_data['Region'] = self.handle_data(value[6:])
		return extra_data


	def handle_US(self, code, extra_data):
		s = requests.Session()
		scont = s.get(self.url['US'][0]+code)
		cont = scont.content.decode('utf-8')
		soup = BeautifulSoup(cont, 'html.parser')
		data = soup.find_all(class_='chart base-table')
		for text in data:
			value = ''.join(re.split(r"-|,|\n| |\r|&", text.get_text())).encode('ascii')
			value = value.split('Percentage')
			if value[0] == 'Sector':
				extra_data['Sector'] = self.handle_data(value[1])
			if value[0] == 'Country':
				extra_data['Region'] = self.handle_data(value[1])
		return extra_data


	def handle_data(self, value):
		mes = {}
		list = re.findall(r'[0-9]+.[0-9]+|[A-z]+', value)
		for i in range(len(list)):
			if i % 2 == 0:
				key = self.convert_dist(list[i])
				percent = self.handle_number(list[i+1])
				if mes.has_key(key):
					mes[key] = mes[key] + percent
				else:
					mes[key] = percent
		return  mes

	def convert_dist(self, dist):
		if self.dist_map.has_key(dist):
			return self.dist_map[dist]
		else:
			return dist

	def handle_number(self, num):
		return decimal.Decimal(num)/100


