from odps import ODPS
from odps.df import DataFrame
# doc: https://help.aliyun.com/document_detail/34615.html

# o = ODPS('**your-access-id**', '**your-secret-access-key**',
#              project='**your-project**', endpoint='**your-end-point**')

o = ODPS('LTAIdXteU42OrXqy', 'Cggf8jCyJ8KTAKaizDQGJOcFZDGojM',
             project='hft_hk', endpoint='http://service.cn-hongkong.maxcompute.aliyun.com/api')

symbol_list = ['600000.SH','600001.SH','000001.SZ','000002.SZ','000003.SZ']
# sql_str = "select symbol,date,preclose,a1,last from level2_data where day<'20180103' and symbol ='600000.SH' and preclose>0 limit 10;"
# sql_str = "select symbol,date,preclose,a1,last from level2_data where day<'20180107' and symbol ='600000.SH' and preclose>0;"
# sql_str = "select symbol,timestamp,preclose,a1,last from level2_data where day<'20180107' and " \
#           "(symbol ='600000.SH'or symbol ='000001.SZ' or symbol ='000002.SZ')and " \
#           "preclose>0;"
sql_str = "select symbol,timestamp,preclose,a1,last from level2_data where day<'20180130' and " \
          "(symbol ='600000.SH'or symbol ='000001.SZ' or symbol ='000002.SZ')and " \
          "preclose>0;"
# sql_str = "select symbol,timestamp,preclose,a1,last from level2_data where day<'20180103' and symbol in (%s) and preclose>0;" %','.join(['%s'] * len(symbol_list))
# sql_str = "select symbol,timestamp,preclose,a1,last from level2_data where day<'20180107' and symbol in ('600000.SH','600001.SH','000001.SZ','000002.SZ','000003.SZ') and preclose>0;"



# o.execute_sql('select * from dual')  #  同步的方式执行，会阻塞直到SQL执行完成
instance = o.run_sql(sql_str)  # 异步的方式执行
instance.wait_for_success()  # 阻塞直到完成

result = instance.open_reader().to_pandas()
result.to_csv('temp_csv')
print (result.head())


# with odps.execute_sql('select * from dual').open_reader() as reader:
#      for record in reader:
# 	     print (record)
# level2 = DataFrame(o.get_table('level2_data'))

# data = level2.loc[(level2['symbol'] == '60000.SH') &(level2['timestamp'] < 1514876407000)]
#
# data = level2.select(level2['symbol'] == '60000.SH')[['a2', 'a1']].head(5)
# print(data.head(10))