import pandas as pd
import calendar
import datetime
import re

# data = pd.read_csv('US_sector.csv')
# data = pd.read_csv('US_style_qtdrilldown 20181025 441.csv')
# data = pd.read_csv('US_sector_qtdrilldown 20181025 378.csv')
data = pd.read_csv('int_equity_qtdrilldown 20181025 875.csv')

sector_list = data['Name'].unique().tolist()
year_list = data['Year'].unique().tolist()
month_list = data.columns.tolist()[2:]
data = data.set_index(['Name','Year'])

parsed_data = {}

parsed_data = {}
for sector in sector_list:
	temp = data.loc[sector].stack()
	temp_dict = {}
	for row in temp.iteritems():
		print (row)
		year = int(row[0][0])
		month = int(row[0][1].split(' ')[1].replace('M',''))
		date = calendar.monthrange(year, month)[1]
		date_ts = datetime.date(year, month, date)
		trans = str.maketrans('','','()')
		temp_dict[date_ts] = float(row[1].replace(',','').replace('(','-').translate(trans))
	parsed_data[sector] = pd.Series(temp_dict)

result = pd.DataFrame(parsed_data)
print (result.head())
fname = 'parsed/' + 'Int Equity.csv'
result.to_csv(fname)
