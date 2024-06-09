from lib.commonalgo.database.db_utility_local import DBSync
import pandas as pd
db = DBSync()
rat = 80
data_new = db.fetch_bt_data(ratio=rat, region='NON', sector="NON", algotype=111, algoversion=10)
data_old = db.fetch_bt_data(ratio=rat, region='NON', sector="NON", algotype=110, algoversion=1)
data_old.index = data_old['history_date']
data_new.index = data_new['history_date']
pct_old= data_old['history_value'].astype('float').pct_change()
pct_new= data_new['history_value'].astype('float').pct_change()
data = (pd.concat([pct_old[:-198],pct_new]).fillna(0.)+1.).cumprod()
db.write_bt_data(ratio=rat, region='NON', sector="NON", algotype=111, algoversion=10,data=data)
print(data_old.head())
print(data_new.head())