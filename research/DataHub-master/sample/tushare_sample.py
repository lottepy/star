# -*- coding:utf-8 -*-

# https://tushare.pro/document/1?doc_id=40

from lib.commonalgo.data.tushare_client import TushareClient

pro = TushareClient().DataApi()

df = pro.fut_basic(exchange='CZCE', fut_type='1', fields='ts_code,symbol,name,list_date,delist_date')
_