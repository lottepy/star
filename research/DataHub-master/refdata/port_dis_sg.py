from lib.commonalgo.database.db_utility_local import DBSync, DBSync_ITtest
import pandas as pd
import numpy as np
import pycountry
import json

algodb = DBSync()
# recent_data = db.fetch_wt_data_v2(ratio=ratio, region=region, sector=sector,
# 			                     algoclass_name = 'smart_global', algoclass_branch='v52',algoversion=1)
#
# weight = pd.Series(index =[x.split('_')[-1] for x in recent_data['iuid'].values],
# 			                      data=recent_data['weight'].values)

# sector_dis = pd.read_csv('sector.csv',index_col=0).fillna(0).T
# selected_weight = pd.Series(index=sector_dis.index, data=weight).astype('float')
# port_dict[ratio] = selected_weight.dot(sector_dis) /selected_weight.sum()

db = DBSync_ITtest()
port_dict = {}

etf_rating = pd.read_excel('sg_dis/SG-Bond-ETF.xlsx',sheetname='Rating',index_col=0).fillna(0).T
etf_dura = pd.read_excel('sg_dis/SG-Bond-ETF.xlsx',sheetname='Maturity',index_col=0).fillna(0).T
for ratio in range(40,50,10):
	for region in ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"]:
		for sector in ["NON", "TEC", "CNS", "UTL", "HEA"]:
	# for region in ["NON"]:
	# 	for sector in ["NON"]:
			modelid = db.fetch_model_id(algo_type=105,region=region,sector=sector,risk_ratio=ratio, version=2)
			weight_df = db.fetch_model_weight(modelid=modelid)
			# weight_df = algodb.fetch_wt_data_v2(ratio=ratio, region=region, sector=sector,
			#                                   algoclass_name='smart_global', algoclass_branch='v52', algoversion=1)
			weight = pd.Series(index =[x.split('_')[-1] for x in weight_df['iuid'].values],
			                   data=weight_df['weight'].values)
			selected_weight = pd.Series(index=etf_rating.index, data=weight).astype('float')
			port_rating = selected_weight.dot(etf_rating)
			port_dura = selected_weight.dot(etf_dura)
			if port_rating.sum():
				port_rating = port_rating/selected_weight.sum()/100.
				port_dura = port_dura/selected_weight.sum()/100.
				print(modelid)
				db.update_model_dist(model_id=modelid,dist_type=500, data=port_rating[port_rating!=0])
				db.update_model_dist(model_id=modelid,dist_type=600, data=port_dura[port_dura!=0])

# sector_dis = pd.read_csv('sector_adj.csv',index_col=0).fillna(0).T
# region_dis = pd.read_csv('region.csv', index_col=0).fillna(0)
# county_dict = {x: pycountry.countries.lookup(x).alpha_2 for x in region_dis.index.tolist() if x != 'Others'}
# county_dict['Others'] = 'Others'
# region_map = pd.Series(index=region_dis.index, data=pd.Series(county_dict))
# region_df = pd.DataFrame(index=region_map.values, data=region_dis.values, columns=region_dis.columns).T
# for ratio in range(20,100,10):
# 	for region in ["NON", "AUS", "DEU", "HKG", "JPN", "GBR", "CHN"]:
# 		for sector in ["NON", "TEC", "CNS", "UTL", "HEA"]:
# 			modelid = db.fetch_model_id(algo_type=105,region=region,sector=sector,risk_ratio=ratio, version=2)
# 			weight_df = db.fetch_model_weight(modelid=modelid)
# 			# weight_df = algodb.fetch_wt_data_v2(ratio=ratio, region=region, sector=sector,
# 			#                                   algoclass_name='smart_global', algoclass_branch='v52', algoversion=1)
# 			weight = pd.Series(index =[x.split('_')[-1] for x in weight_df['iuid'].values],
# 			                   data=weight_df['weight'].values)
# 			selected_weight = pd.Series(index=region_df.index, data=weight).astype('float')
# 			data = selected_weight.dot(region_df)
# 			sector_data = selected_weight.dot(sector_dis)
# 			if data.sum():
# 				data = data/selected_weight.sum()
# 				sector_data =sector_data/selected_weight.sum()
# 				db.update_model_dist(model_id=modelid,dist_type=100, data=data[data!=0])
# 				db.update_model_dist(model_id=modelid,dist_type=300, data=sector_data[sector_data!=0])

