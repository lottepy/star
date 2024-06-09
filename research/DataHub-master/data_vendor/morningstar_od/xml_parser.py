import xmltodict
import xml.etree.ElementTree as ET
from lxml import etree
from xmljson import badgerfish as bf

select_codes = [
	'CreditRatingBreakdownCalculated',
	'MaturityRange',
	'CouponRange',
	'CreditQualityBreakdown'


]

_file = 'xmldata/F00000Z9UC.xml'

xpath = '/FundShareClass/Fund/PortfolioList/Portfolio/PortfolioBreakdown/MaturityRange'
code_url = 'http://edw.morningstar.com/GetDictionaryXML.aspx?ClientId=magnumhk&DicType=TYPECODE&Id=Maturity%20Range&Search='
credit_url = 'Credit Quality Breakdown'
xpath2 = '/FundShareClass/Fund/PortfolioList/Portfolio/PortfolioBreakdown/CreditQualityBreakdown'
xpath_e = xpath2.split('/')[1:]
with open(_file) as fd:
	doc = xmltodict.parse(fd.read())
i = 1
for ele in xpath_e:
	if i > 0:
		data = doc.get(ele)
		i -= 1
	else:
		data = data.get(ele)

result = {}

for x in data.get('BreakdownValue'):

	result[x.get('@Type')] = x['#text']

print (result)
