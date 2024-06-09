import pandas as pd
from datetime import datetime
from  utils.fx_convertor import *

cross_pair = ['AUDCAD','AUDJPY','AUDNZD','AUDUSD','CADCHF','CADJPY','EURCAD','EURCHF','EURGBP','EURJPY',
 		      'EURUSD','GBPCAD','GBPCHF','GBPJPY','GBPUSD','NOKSEK','NZDJPY','NZDUSD','USDCAD','USDCHF','USDIDR',
              'USDINR','USDJPY','USDKRW','USDNOK','USDSEK','USDSGD','USDTHB','USDTWD']
major_pair = ['AUDUSD','EURUSD','GBPUSD','NZDUSD','USDCAD','USDCHF','USDIDR','USDINR','USDJPY',
              'USDKRW','USDNOK','USDSEK','USDSGD','USDTHB','USDTWD']

cross_pair_holding = [10408.32948,49876.12625,248772.0927,79134.16649,4320.063081,-71798.5651,9386.350653,30187.63128, \
                      19809.60005,759.4043487,-360.2209597,-365.9624398,2857.781022,-7888.651195,-14010.12568,673.1383419, \
                      66780.78633,-223224.2193,10000, 1882.47238,-76609.90658,-113695.4291,-3835.780401,-71098.35441,\
                      -14825.3012,-9141.69193,-879.3095984,-18532.83395,10000]
major_pair_weight = [0.23715801948365678, 0.06181192087012339, -0.04382837659664156, -0.24339890161144084, 0.07219190946014971,\
                     0.0394538227666265, -0.07183948068502484,-0.10661572304980954,0.00697545757602068,-0.06667112761768683,\
                     -0.01396933877053231,-0.00850524412111962,-0.0008245558274431426,-0.017378812036528104,0.009377309527196247]
cross_pair_last_price = [0.8733,70.365,1.0416,0.6515,0.7191,80.669,1.4775,1.0646,0.8603,118.99,1.1026,1.7178,1.2374,138.41,\
                         1.2823,1.0226,67.389,0.6246,1.3407,0.9649,14550,72.61,107.89,1199.95,9.3939,9.605,1.3932,31.522,30.08]
portfolio_value = 1066403.9585126005
real_holding = [388190.71492000006, 59782.76537200001, -36449.157215815, -415564.44472632, 76985.7380208869,
                42073.712776785054, -76609.90658, -113695.4291, 7438.655571505162, -71098.35441,
                -14896.958162699199, -9070.025998877987, -879.3095984, -18532.83395, 10000.0]
path = r"\\192.168.9.170\share\alioss\0_DymonFx\target_holding\backtest_u_0_strategy_id_global_20200428.csv"
target_holding = [10408.32948, 49876.12625,	248772.0927, 79134.16649, 4320.063081,-71798.5651,9386.350653,30187.63128,\
                  19809.60005, 759.4043487, -360.2209597,-365.9624398,2857.781022,-7888.651195,	-14010.12568,673.1383419,\
                  66780.78633,-223224.2193,0,1882.47238,-76609.90658,-113695.4291,-3835.780401,-71098.35441,-14825.3012,\
                  -9141.69193,-879.3095984,-18532.83395,0]
holding_df = pd.DataFrame({'ccp': cross_pair, 'holding':target_holding})
cross_pair_target = pd.DataFrame({'ccp': cross_pair, 'holding':cross_pair_holding})
major_pair_target = pd.DataFrame({'ccp': major_pair, 'TargetRatio':major_pair_weight})
last_price_df = pd.DataFrame({'ccp': cross_pair, 'last_price':cross_pair_last_price})
T150_price_df = pd.DataFrame({'ccp': cross_pair, 'T150_price':cross_pair_last_price})
today_list = [datetime(2020,4,27),datetime(2020,4,28),datetime(2020,4,29),datetime(2020,4,30), datetime(2020,5,1)]
yesterday_list = [datetime(2020,4,24),datetime(2020,4,27),datetime(2020,4,28),datetime(2020,4,29), datetime(2020,4,30)]

def _compare(array_a: np.array, array_b: np.array, threshold=1e-5):
    return np.sum(np.abs(array_a-array_b)) < threshold

def test_convert_cross_target_to_major():
    major_target1 = convert_cross_target_to_major(cross_pair_target, T150_price_df)[0]
    major_target1.set_index('ccp', inplace=True)
    major_target1 = major_target1.reindex(major_pair_target['ccp'].tolist())
    assert _compare(major_target1['TargetRatio'].values, major_pair_target['TargetRatio'].values)

def test_weight_to_holding():
    target_df = weight_to_holding(major_pair_target, last_price_df, portfolio_value)
    assert _compare(target_df['target_holding'].values, np.array(real_holding))

def test_get_holding_from_csv():
    result_df = pd.read_csv(path)
    holding = get_holding_from_csv(result_df)
    holding.set_index('ccp',inplace=True)
    holding = holding.reindex(holding_df['ccp'].tolist())
    assert _compare(holding['holding'].values, holding_df['holding'].values, threshold=10)

def test_get_fx_yesterday():
    for i in range(len(today_list)):
        assert get_fx_yesterday(today_list[i]) == yesterday_list[i]