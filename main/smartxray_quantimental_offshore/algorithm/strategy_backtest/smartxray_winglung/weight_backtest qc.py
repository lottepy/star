from os import makedirs, listdir
from os.path import exists, join
import pandas as pd
import numpy as np

from scipy.stats import norm

from datetime import datetime, timedelta
from algorithm.addpath import config_path, data_path, result_path
from algorithm.data_preparation import dm_data_sourcing
from algorithm.data_preparation.foropt_preparation import concat_sector_allocation, concat_country_allocation, \
                                                          concat_credit_quality, concat_price, concat_index_mc
from algorithm.update_database_and_result_analysis.performance_metrics import metrics
from algorithm.strategy_backtest.smartxray_winglung.run_fake_strategy import run_fake_strategy 
from algorithm.strategy_backtest.smartxray_winglung import s2_funds_pool as s2
from algorithm.strategy_backtest.smartxray_winglung import s3_weighting as s3
from algorithm.strategy_backtest.smartxray_winglung import s4_back_testing_qc as s4
from algorithm.strategy_backtest.smartxray_winglung import s5_benchmark as s5
from algorithm.update_database_and_result_analysis.risk_matching import calculate_2Y_vol
from algorithm.update_database_and_result_analysis.post2dadatabase_helper import make_duplicate_007_and_013
from datamaster import dm_client
dm_client.start()

def projection(name, backtesting_info):
    fields = ['cumulative return', 'annualized return', 'annualized volatility', 'adjusted return',
              'maximum drawdown', 'sharpe ratio']
    risk_matrix = pd.DataFrame(index=[name], columns=fields)
    backtesting_info.columns = ['nav']
    backtesting_info['daily_return'] = backtesting_info['nav'].pct_change()
    backtesting_info['daily_return'].iloc[0] = 0
    risk_matrix.loc[name, 'cumulative return'] = backtesting_info['nav'].iloc[-1] - 1
    annualized_return = (backtesting_info['nav'].iloc[-1] / 1) ** (250 / backtesting_info['nav'].shape[0]) - 1
    risk_matrix.loc[name, 'annualized return'] = annualized_return
    annualized_volatility = np.std(backtesting_info['daily_return']) * np.sqrt(250)    #%%%changed
    risk_matrix.loc[name, 'annualized volatility'] = annualized_volatility
    risk_matrix.loc[name, 'adjusted return'] = annualized_return / annualized_volatility

    draw_down = 0.0
    max_value = backtesting_info['nav'].iloc[0]
    for ind, value in backtesting_info['nav'].items():
        if draw_down < (max_value - value) / max_value:
            draw_down = (max_value - value) / max_value
        max_value = max(max_value, value)

    risk_matrix.loc[name, 'maximum drawdown'] = draw_down
    risk_matrix.loc[name, 'sharpe ratio'] = annualized_return / annualized_volatility    #%%%

    t_value = [0., 0., 0., 0., 0.]
    projection = [0., 0., 0., 0., 0.]
    quantile = [0.05, 0.25, 0.5, 0.75, 0.95]
    term = range(0, 121, 1)

    for algo in np.arange(1, 2, 1):
        randomAdj = np.log(algo) / 100

        ann_ret = risk_matrix.loc[name, 'annualized return'] / 12
        ann_vol = risk_matrix.loc[name, 'annualized volatility'] / 12

        for i in range(0, 5, 1):
            t_value[i] = norm.ppf(quantile[i] + randomAdj, 0, 1)

        list_portfolio_projection = []
        for t in term:
            list_monthly_projection = []
            for i in range(0, 5, 1):
                projection[i] = np.exp(t_value[i] * ann_vol * np.sqrt(t) + t * ann_ret)
                list_monthly_projection.append(projection[i])
            list_portfolio_projection.append(list_monthly_projection)

        portfolio_projection = pd.DataFrame(index=list(term), data=list_portfolio_projection,
                                            columns=[0.05, 0.25, 0.5, 0.75, 0.95])
        portfolio_projection = portfolio_projection.T
        portfolio_projection['risk_ratio'] = name[:3]
        portfolio_projection['sector'] = name[4:6]
        portfolio_projection['region'] = name[7:10]
        portfolio_projection_single = portfolio_projection.iloc[:, :121]

    return portfolio_projection_single

def distribution(name, weight_info, timestamp, sector_allocation, country_allocation, credit_quality_allocation):
    risk_ratio = float(name[:4])
    all_fund_list = weight_info.index.tolist()

    sector_overall_df_list = []
    ca_overall_df_list = []
    cqa_overall_df_list = []


    for secid in all_fund_list:
        sector_df_tmp = sector_allocation.loc[secid, :]
        sector_overall_df_list.append(sector_df_tmp)

        ca_df_tmp = country_allocation.loc[secid, :]
        ca_overall_df_list.append(ca_df_tmp)

        cqa_df_tmp = credit_quality_allocation.loc[secid, :]
        cqa_overall_df_list.append(cqa_df_tmp)

    sector_overall_df = pd.concat(sector_overall_df_list, axis = 1)
    country_overall_df = pd.concat(ca_overall_df_list, axis = 1)
    credit_quality_overall_df = pd.concat(cqa_overall_df_list, axis = 1)

    equity_ratio = risk_ratio

    sector_overall_df.columns = all_fund_list
    sector_overall_df = sector_overall_df.fillna(0)
    sector_exposure_df = np.dot(np.array(sector_overall_df.values), np.array(weight_info.values)) / equity_ratio
    sector_exposure_df /= sector_exposure_df.sum()
    sector_exposure_df = pd.DataFrame(sector_exposure_df)
    sector_exposure_df.index = list(sector_allocation.columns)

    country_overall_df.columns = all_fund_list
    country_overall_df = country_overall_df.fillna(0)
    country_exposure_df = np.dot(np.array(country_overall_df.values), np.array(weight_info.values)) / equity_ratio
    country_exposure_df /= country_exposure_df.sum()
    country_exposure_df = pd.DataFrame(country_exposure_df)
    country_exposure_df.index = list(country_allocation.columns)

    credit_quality_overall_df.columns = all_fund_list
    credit_quality_overall_df = credit_quality_overall_df.fillna(0)
    credit_quality_exposure_df = np.dot(np.array(credit_quality_overall_df.values), np.array(weight_info.values)) / equity_ratio
    credit_quality_exposure_df /= credit_quality_exposure_df.sum()
    credit_quality_exposure_df = pd.DataFrame(credit_quality_exposure_df)
    credit_quality_exposure_df.index = list(credit_quality_allocation.columns)

    basic_materials = round(float(sector_exposure_df.loc['101']) * 100, 2)
    consumer_cyclical = round(float(sector_exposure_df.loc['102']) * 100, 2)
    financial_services = round(float(sector_exposure_df.loc['103']) * 100, 2)
    real_estate = round(float(sector_exposure_df.loc['104']) * 100, 2)
    consumer_defensive = round(float(sector_exposure_df.loc['205']) * 100, 2)
    healthcare = round(float(sector_exposure_df.loc['206']) * 100, 2)
    utilities = round(float(sector_exposure_df.loc['207']) * 100, 2)
    communication_services = round(float(sector_exposure_df.loc['308']) * 100, 2)
    energy = round(float(sector_exposure_df.loc['309']) * 100, 2)
    industrials = round(float(sector_exposure_df.loc['310']) * 100, 2)
    technology = round(100.0 - basic_materials - consumer_cyclical - financial_services - real_estate -
                       consumer_defensive - healthcare - utilities - communication_services - energy - industrials, 2)

    USA = round(float(country_exposure_df.loc['USA']) * 100, 2)
    CHN = round(float(country_exposure_df.loc['CHN']) * 100, 2)
    JPN = round(float(country_exposure_df.loc['JPN']) * 100, 2)
    HKG = round(float(country_exposure_df.loc['HKG']) * 100, 2)
    GBR = round(float(country_exposure_df.loc['GBR']) * 100, 2)
    DEU = round(float(country_exposure_df.loc['DEU']) * 100, 2)
    NLD = round(float(country_exposure_df.loc['NLD']) * 100, 2)
    IDN = round(float(country_exposure_df.loc['IDN']) * 100, 2)
    IND = round(float(country_exposure_df.loc['IND']) * 100, 2)
    CAN = round(float(country_exposure_df.loc['CAN']) * 100, 2)
    SGP = round(float(country_exposure_df.loc['SGP']) * 100, 2)
    KOR = round(float(country_exposure_df.loc['KOR']) * 100, 2)
    FRA = round(float(country_exposure_df.loc['FRA']) * 100, 2)
    AUS = round(float(country_exposure_df.loc['AUS']) * 100, 2)
    ITA = round(float(country_exposure_df.loc['ITA']) * 100, 2)
    PHL = round(float(country_exposure_df.loc['PHL']) * 100, 2)
    MEX = round(float(country_exposure_df.loc['MEX']) * 100, 2)
    ESP = round(float(country_exposure_df.loc['ESP']) * 100, 2)
    THA = round(float(country_exposure_df.loc['THA']) * 100, 2)
    CHE = round(float(country_exposure_df.loc['CHE']) * 100, 2)
    OTHER = round(100.0 - (USA + CHN + JPN + HKG + GBR + DEU + NLD + IDN + IND + CAN + SGP + KOR + FRA + AUS + ITA +
                           PHL + MEX + ESP + THA + CHE), 2)

    Government = round(float(credit_quality_exposure_df.loc['1']) * 100, 2)
    AAA = round(float(credit_quality_exposure_df.loc['2']) * 100, 2)
    AA = round(float(credit_quality_exposure_df.loc['3']) * 100, 2)
    A = round(float(credit_quality_exposure_df.loc['4']) * 100, 2)
    BBB = round(float(credit_quality_exposure_df.loc['5']) * 100, 2)
    BB = round(float(credit_quality_exposure_df.loc['6']) * 100, 2)
    B = round(float(credit_quality_exposure_df.loc['7']) * 100, 2)
    Below_B = round(float(credit_quality_exposure_df.loc['8']) * 100, 2)
    Not_Rated = round(float(credit_quality_exposure_df.loc['9']) * 100, 2)
    IG = Government + AAA + AA + A + BBB
    HY = round(100.0 - IG, 2)

    fund_type_dict = [
        {
            "FundSectorName": "Basic Materials",
            "FundSectorRate": basic_materials
        },
        {
            "FundSectorName": "Consumer Cyclical",
            "FundSectorRate": consumer_cyclical
        },
        {
            "FundSectorName": "Financial Services",
            "FundSectorRate": financial_services
        },
        {
            "FundSectorName": "Real Estate",
            "FundSectorRate": real_estate
        },
        {
            "FundSectorName": "Consumer Defensive",
            "FundSectorRate": consumer_defensive
        },
        {
            "FundSectorName": "Healthcare",
            "FundSectorRate": healthcare
        },
        {
            "FundSectorName": "Utilities",
            "FundSectorRate": utilities
        },
        {
            "FundSectorName": "Communication Services",
            "FundSectorRate": communication_services
        },
        {
            "FundSectorName": "Energy",
            "FundSectorRate": energy
        },
        {
            "FundSectorName": "Industrials",
            "FundSectorRate": industrials
        },
        {
            "FundSectorName": "Technology",
            "FundSectorRate": technology
        },
        {
            "FundRegionName": "USA",
            "FundRegionRate": USA
        },
        {
            "FundRegionName": "China",
            "FundRegionRate": CHN
        },
        {
            "FundRegionName": "Japan",
            "FundRegionRate": JPN
        },
        {
            "FundRegionName": "Hong Kong",
            "FundRegionRate": HKG
        },
        {
            "FundRegionName": "UK",
            "FundRegionRate": GBR
        },
        {
            "FundRegionName": "Germany",
            "FundRegionRate": DEU
        },
        {
            "FundRegionName": "Netherlands",
            "FundRegionRate": NLD
        },
        {
            "FundRegionName": "Indonesia",
            "FundRegionRate": IDN
        },
        {
            "FundRegionName": "India",
            "FundRegionRate": IND
        },
        {
            "FundRegionName": "Canada",
            "FundRegionRate": CAN
        },
        {
            "FundRegionName": "Singapore",
            "FundRegionRate": SGP
        },
        {
            "FundRegionName": "South Korea",
            "FundRegionRate": KOR
        },
        {
            "FundRegionName": "France",
            "FundRegionRate": FRA
        },
        {
            "FundRegionName": "Australia",
            "FundRegionRate": AUS
        },
        {
            "FundRegionName": "Italy",
            "FundRegionRate": ITA
        },
        {
            "FundRegionName": "Philippines",
            "FundRegionRate": PHL
        },
        {
            "FundRegionName": "Mexico",
            "FundRegionRate": MEX
        },
        {
            "FundRegionName": "Spain",
            "FundRegionRate": ESP
        },
        {
            "FundRegionName": "Thailand",
            "FundRegionRate": THA
        },
        {
            "FundRegionName": "Switzerland",
            "FundRegionRate": CHE
        },
        {
            "FundRegionName": "Others",
            "FundRegionRate": OTHER
        },
        {
            "FundBondLevel": "Investment Grade Bond",
            "FundBondRate": IG
        },
        {
            "FundBondLevel": "High Yield Bond",
            "FundBondRate": HY
        }
    ]

    data_dict = {}
    fund_type_dict_all = {}
    # for ts in list(data_dict.get('weight').keys()):
    fund_type_dict_all[timestamp] = fund_type_dict
    data_dict['fund_type'] = fund_type_dict_all

    distribution_dict = [
        {300:
            {
                "Basic Materials": basic_materials,
                "Consumer Cyclical": consumer_cyclical,
                "Financial Services": financial_services,
                "Real Estate": real_estate,
                "Consumer Defensive": consumer_defensive,
                "Healthcare": healthcare,
                "Utilities": utilities,
                "Communication Services": communication_services,
                "Energy": energy,
                "Industrials": industrials,
                "Technology": technology,
            }
        },
        {200:
            {
                "USA": USA,
                "China": CHN,
                "Japan": JPN,
                "Hong Kong": HKG,
                "UK": GBR,
                "Germany": DEU,
                "Netherlands": NLD,
                "Indonesia": IDN,
                "India": IND,
                "Canada": CAN,
                "Singapore": SGP,
                "South Korea": KOR,
                "France": FRA,
                "Australia": AUS,
                "Italy": ITA,
                "Philippines": PHL,
                "Mexico": MEX,
                "Spain": ESP,
                "Thailand": THA,
                "Switzerland": CHE,
                "Others": OTHER
            },
        },
        {500:
            {
                "Investment Grade Bond": IG,
                "High Yield Bond": HY
            }
        }
    ]

    if round(pd.DataFrame.from_dict(distribution_dict[0][300], orient='index').sum(), 2)[0] != 100.0:
        print('*' * 40)
        print('Wrong distribution on sector exposure!')
        print(name)
        print(round(pd.DataFrame.from_dict(distribution_dict[0][300], orient='index').sum(), 2)[0])
        print('*' * 40)

    if round(pd.DataFrame.from_dict(distribution_dict[1][200], orient='index').sum(), 2)[0] != 100.0:
        print('*' * 40)
        print('Wrong distribution on country exposure!')
        print(name)
        print(round(pd.DataFrame.from_dict(distribution_dict[1][200], orient='index').sum(), 2)[0])
        print('*' * 40)

    if round(pd.DataFrame.from_dict(distribution_dict[2][500], orient='index').sum(), 2)[0] != 100.0:
        print('*' * 40)
        print('Wrong distribution on credit quality exposure!')
        print(name)
        print(round(pd.DataFrame.from_dict(distribution_dict[2][500], orient='index').sum(), 2)[0])
        print('*' * 40)

    data_dict['distributions'] = distribution_dict

    return data_dict

def weighting_backtesting(start_date, end_date, output_path, test_dict, type, foropt_prepare, update_benchmark):

    original_secid_mapping = pd.read_excel(join(config_path, 'wrong_iuid_mapping_WingLung.xlsx')) 
    original_secid_mapping = pd.concat([original_secid_mapping[['iuid','Product Code']], original_secid_mapping[['iuid.1', 'Product Code.1']].rename({'iuid.1':'iuid', 'Product Code.1':'Product Code'}, axis=1)])
    original_secid_mapping = original_secid_mapping.set_index('Product Code')
    original_secid_mapping.columns = ['old_iuid']

    new_secid_mapping = pd.read_excel(join(config_path, 'correct_iuid_mapping_WingLung.xlsx'))
    new_secid_mapping = new_secid_mapping[['iuid', 'Product Code']]
    new_secid_mapping = new_secid_mapping.set_index('Product Code')
    new_secid_mapping.columns = ['new_iuid']

    iuid_mapping = original_secid_mapping.merge(new_secid_mapping, how='left', left_index=True, right_index=True)
    iuid_mapping = iuid_mapping.set_index('old_iuid')
    iuid_mapping = iuid_mapping.iloc[:,0]
    
    secid_mapping = iuid_mapping.copy()
    secid_mapping.index = secid_mapping.index.str[6:]
    secid_mapping = secid_mapping.str[6:]

    if exists(output_path):
        pass
    else:
        makedirs(output_path)
        
    

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    # =====================
    # step 1: Bundle injection
    # =====================

    if type == 'skip_download_fake_strategy':
        pass
    elif type == 'skip_download':
        run_fake_strategy(start_dt, end_dt, 'smartxray_wl')
    else:
        dm_data_sourcing.bundle_download()
        run_fake_strategy(start_dt, end_dt, 'smartxray_wl')

    # ====================
    # data loading
    # ====================
    foropt_path = join(data_path, 'foropt')
    
    if foropt_prepare:
        print('*' * 60)
        print('Start Foropt Preparation..')
        print('*' * 60)
        concat_price(output_path = foropt_path)
        concat_sector_allocation(output_path=foropt_path)
        concat_country_allocation(output_path = foropt_path)
        concat_credit_quality(output_path = foropt_path)
        concat_index_mc(output_path=foropt_path)
        print('*' * 60)
        print('Finish Foropt Preparation..')
        print('*' * 60)

    sector_allocation_df = pd.read_csv(join(foropt_path, 'sector_allocation.csv'),
                                       parse_dates=[0], index_col=0)
    country_allocation_df = pd.read_csv(join(foropt_path, 'country_allocation.csv'),
                                       parse_dates=[0], index_col=0)
    credit_quality_df = pd.read_csv(join(foropt_path, 'credit_quality.csv'),
                                       parse_dates=[0], index_col=0)
    price_df = pd.read_csv(join(foropt_path, 'price.csv'), parse_dates=[0], index_col=0)

    # invest_length_df = pd.read_csv(join(config_path, 'invest_length.csv'), index_col=0)

    # white_list
    # summary_df = pd.read_csv(join(data_path, 'cn_fund_full.csv'), index_col=0)
    # summary_df = summary_df[['wind_code', 'iuid']]
    
    old_bond_benchmark = 'LGTRTRUU Index'
    
    old_bond_benchmark_df = dm_data_sourcing.benchmark_download([old_bond_benchmark], start_date= start_date, end_date = end_date)
    old_bond_benchmark_df = old_bond_benchmark_df/old_bond_benchmark_df.iloc[0]
    old_bond_benchmark_df.index = pd.to_datetime(old_bond_benchmark_df.index)

    equity_benchmark = 'MXWD Index'
    bond_benchmark = 'LG13TRUU Index'
    
    index_file_list = listdir( join(data_path, 'benchmark', 'bundles'))
    index_df = [pd.read_csv(join(data_path, 'benchmark', 'bundles', ele), index_col=[0], parse_dates=[0], usecols=['date', 'open']) for ele in index_file_list]
    index_df = pd.concat(index_df, axis=1)
    index_df.columns = [ele[:-4] for ele in index_file_list]
    index_df = index_df[[equity_benchmark, bond_benchmark]]

    # index_df = dm_data_sourcing.benchmark_download([equity_benchmark, bond_benchmark], start_date= start_date, end_date = end_date)
    index_df = index_df.ffill()
    index_df[equity_benchmark] = index_df[equity_benchmark]/index_df[equity_benchmark].bfill()[0]
    index_df[bond_benchmark] = index_df[bond_benchmark]/index_df[bond_benchmark].bfill()[0]

    # index_df['MXWDret'] = index_df['MXWD Index'].pct_change().fillna(0)
    # index_df['LGTRTRUUret'] = index_df['LGTRTRUU Index'].pct_change().fillna(0)
    index_df.index = pd.to_datetime(index_df.index)
    index_df.index = [c.date() for c in index_df.index]

    equity_pct_array = np.array(test_dict['equity_pct'])
    
    risk_matching_df_tmp = index_df.copy()
    risk_matching_df_tmp.index = pd.to_datetime(risk_matching_df_tmp.index)
    risk_matching_df_tmp = risk_matching_df_tmp[(risk_matching_df_tmp.index >= start_date) & (risk_matching_df_tmp.index <= end_date)]
    
    risk_matching_df_tmp = old_bond_benchmark_df.merge(risk_matching_df_tmp, how='outer', left_index=True, right_index=True).ffill()
    old_bond_benchmark_df = risk_matching_df_tmp['LG13TRUU Index']
    
    risk_matching_df_tmp = risk_matching_df_tmp/risk_matching_df_tmp.iloc[0]
    
    risk_matching_df = risk_matching_df_tmp[equity_benchmark].to_frame().dot(equity_pct_array.reshape(1,-1)) + \
                        risk_matching_df_tmp[old_bond_benchmark].to_frame().dot((1-equity_pct_array).reshape(1,-1))
    
    risk_matching_df.to_csv(join(result_path, 'todatabase', 'risk_matching_benchmark.csv'))


    # ===================================================
    # step 2: Constructing funds pool
    # ===================================================
    # generate_expected_returns(return_df)
    # s2.funds_pool(start_date = start_date, end_date = end_date, model_freq = 'hrz_6', price_df = price_df)
    
    # ===================================================
    # step 2.1: Generate Benchmark
    # ===================================================
    if update_benchmark:
        print('*' * 60)
        print('Start Generating Benchmark..')
        print('*' * 60)
        s5.benchmark_generator(cash=100000000, start_dt=pd.to_datetime(start_date), end_dt=pd.to_datetime(end_date))
        print('*' * 60)
        print('Finish Generating Benchmark..')
        print('*' * 60)
    

    # =================================
    # step 3: Weighting and backtesting
    # =================================
    recent_weight_list = []
    recent_sa_list = []
    recent_ca_list = []
    recent_cqa_list = []
    return_list = []
    rebalance_list = []
    post2database_list = []

    projection_path = join(output_path, 'projection')
    if exists(projection_path):
        pass
    else:
        makedirs(projection_path)

    weight_df_list = []
    name_list = []
    for equity_pct in test_dict['equity_pct']:
        byequity_pct_list = []
        for region_preference in test_dict['region_preference']:
            for sector_preference in test_dict['sector_preference']:
                info = str(round(equity_pct, 2)) + '_' + sector_preference + '_' + region_preference
                name_list.append(info)
                # ===========================
                # calculate weights
                # ===========================

                if equity_pct < 0.15:
                    weight_overall_tmp, portfolio_feature_tmp = s3.compute_weights(start_date=start_date,
                                                                                    end_date=end_date,
                                                                                    equity_pct=equity_pct,
                                                                                    sector_preference='A1',
                                                                                    region_preference='B1',
                                                                                    lower_bound=0.03,
                                                                                    price_df=price_df)
                else:
                    weight_overall_tmp, portfolio_feature_tmp = s3.compute_weights(start_date=start_date,
                                                                                    end_date=end_date,
                                                                                    equity_pct=equity_pct,
                                                                                    sector_preference=sector_preference,
                                                                                    region_preference=region_preference,
                                                                                    lower_bound=0.03,
                                                                                    price_df=price_df)

                recent_weight_ori_tmp = pd.DataFrame(weight_overall_tmp.iloc[:, -1].dropna())
                recent_weight_ori_tmp.columns = [info]

                weight_overall_tmp_copy = weight_overall_tmp.copy()
                weight_overall_round4_df_list = []
                for i in range(weight_overall_tmp_copy.shape[1]):
                    weight_sub = pd.Series(weight_overall_tmp_copy.iloc[:, i]).dropna()
                    weight_sub = weight_sub.round(4)
                    weight_sub[-1] = round(weight_sub[-1] + 1.0 - weight_sub.sum(), 4)
                    weight_overall_round4_df_list.append(weight_sub)

                weight_overall_round4_df = pd.concat(weight_overall_round4_df_list, axis=1)

                recent_weight_tmp = pd.DataFrame(weight_overall_round4_df.iloc[:, -1].dropna())
                recent_date_tmp = recent_weight_tmp.columns[0]
                recent_timestamp_tmp = int((recent_date_tmp + timedelta(hours=8)).timestamp() * 1000)
                recent_weight_tmp.columns = [info]
                recent_weight_list.append(recent_weight_tmp)

                # ================================
                # rebalance record
                # ================================
                rebalance_df_tmp = pd.DataFrame()
                rebalance_dict = {}
                turnover_dict = {}
                for date in weight_overall_tmp.columns:
                    rebalance_dict[str(date.date())] = weight_overall_tmp[date].dropna().to_dict()
                    if date != weight_overall_tmp.columns[0]:
                        turnover_dict[str(date.date())] = portfolio_feature_tmp.loc[str(date.date()), 'turnover']
                rebalance_df_tmp['risk_ratio'] = [int(equity_pct * 100)]
                rebalance_df_tmp['sector_preference'] = [sector_preference]
                rebalance_df_tmp['region_preference'] = [region_preference]
                rebalance_df_tmp['weight'] = [rebalance_dict]
                rebalance_df_tmp['turnover'] = [turnover_dict]
                rebalance_list.append(rebalance_df_tmp)

                # ================================
                # portfolio features
                # ================================
                # pf_df_tmp = pd.DataFrame()
                # pf_df_tmp['risk_ratio'] = [int(equity_pct * 100)]
                # pf_df_tmp['sector'] = [sector_preference]
                # pf_df_tmp['invest_length'] = [invest_length]
                # portfolio_feature_tmp_tmp = portfolio_feature_tmp.iloc[-1, :]
                # pf_df_tmp['real_beta'] = [portfolio_feature_tmp_tmp['real_beta']]
                # pf_df_tmp['real_equity_pct'] = [portfolio_feature_tmp_tmp['real_equity_pct']]
                # portfolio_feature_list.append(pf_df_tmp)

                # ===========================
                # backtesting
                # ===========================
                save_dm_to_local =  False
                use_local_dm =  True
                backtest_result_tmp = s4.index_backtesting(start=start_date, end=end_date,
                                                            equity_pct = equity_pct,
                                                            sector_preference = region_preference, region_preference= region_preference,
                                                            commission_fee=0, initial_capital=1000000,
                                                            rebalance_freq='6M', model_freq='12M',
                                                            save_dm_to_local=save_dm_to_local, use_local_dm=use_local_dm)

                backtest_result_tmp['portfolio_value'] = backtest_result_tmp['algorithm_period_return']

                backtest_result_tmp = backtest_result_tmp[['portfolio_value']]
                backtest_result_tmp[info] = backtest_result_tmp['portfolio_value'] / \
                                            backtest_result_tmp['portfolio_value'].bfill()[0]
                backtest_result_tmp = backtest_result_tmp[[info]]
                backtest_result_tmp_copy = backtest_result_tmp.copy()
                projection_df_tmp = projection(info, backtest_result_tmp_copy)
                projection_df_tmp.to_csv(join(projection_path, info + '.csv'))

                backtest_result_tmp.index = [c.date() for c in backtest_result_tmp.index]
                # return_list.append(backtest_result_tmp)
                byequity_pct_list.append(backtest_result_tmp)
                # ===========================
                # post2database
                # ===========================
                post2database_df_tmp = pd.DataFrame()
                post2database_backtest_df = backtest_result_tmp.copy()
                post2database_backtest_df = round(post2database_backtest_df, 6)
                post2database_backtest_df.index = [str(c) for c in post2database_backtest_df.index]
                backtest_dict_tmp = post2database_backtest_df.transpose().iloc[0,].to_dict()

                weight_overall_tmp.index = weight_overall_tmp.index.map(lambda x:secid_mapping[x] if x in secid_mapping.index else x)
                weight_df_list.append(weight_overall_tmp)

                weight_dict_tmp = {}
                for date in weight_overall_round4_df.columns:
                    dt_timestamp_tmp = int((date + timedelta(hours = 8)).timestamp() * 1000)
                    weight_df_tmp = weight_overall_round4_df[[date]].dropna()
                    # weight_df_tmp.index = summary_df.loc[weight_df_tmp.index, 'iuid']
                    weight_df_tmp.index = ['HK_20_' + c for c in weight_df_tmp.index]
                    weight_df_tmp = weight_df_tmp.reset_index()
                    weight_df_tmp = weight_df_tmp.replace('HK_20_F0GBR04SSN', 'HK_20_F0GBR053YB')
                    weight_df_tmp = weight_df_tmp.replace('HK_20_F0GBR04AMI', 'HK_20_F0GBR053E5')
                    weight_df_tmp = weight_df_tmp.replace('HK_20_F0GBR04K8L', 'HK_20_F0GBR053DP')
                    weight_df_tmp = weight_df_tmp.replace('HK_20_F00000T2AE', 'HK_20_F00000VTAH')
                    weight_df_tmp = weight_df_tmp.replace('HK_20_F0GBR04MRU', 'HK_20_F0GBR04MRX')
                    weight_df_tmp['index'] = weight_df_tmp['index'].map(lambda x:iuid_mapping[x] if x in iuid_mapping.index else x)
                    
                    weight_df_tmp = weight_df_tmp.set_index('index')
                    weight_dict_tmp[dt_timestamp_tmp] = weight_df_tmp.iloc[:, 0].to_dict()

                
                 # sa_df_tmp = sector_allocation_df[sector_allocation_df.index >= recent_date_tmp]
                sa_df_tmp = sector_allocation_df.copy()
                sa_df_tmp = sa_df_tmp.loc[sa_df_tmp.index[-1],]
                # ca_df_tmp = country_allocation_df[country_allocation_df.index >= recent_date_tmp]
                ca_df_tmp = country_allocation_df.copy()
                ca_df_tmp = ca_df_tmp.loc[ca_df_tmp.index[-1],]
                # cqa_df_tmp = credit_quality_df[credit_quality_df.index >= recent_date_tmp]
                cqa_df_tmp = credit_quality_df.copy()
                cqa_df_tmp = cqa_df_tmp.loc[cqa_df_tmp.index[-1],]


                data_dict_tmp = distribution(name = info, weight_info=recent_weight_tmp, timestamp = recent_timestamp_tmp,
                                sector_allocation=sa_df_tmp.set_index('Funds'),
                                country_allocation = ca_df_tmp.set_index('Funds'),
                                credit_quality_allocation = cqa_df_tmp.set_index('Funds'))

                recent_sa_df_tmp = pd.DataFrame(data_dict_tmp['distributions'][0])
                recent_sa_df_tmp.columns = [info]
                recent_ca_df_tmp = pd.DataFrame(data_dict_tmp['distributions'][1])
                recent_ca_df_tmp = (recent_ca_df_tmp/recent_ca_df_tmp.sum()) * 100
                recent_ca_df_tmp.columns = [info]
                recent_cqa_df_tmp = pd.DataFrame(data_dict_tmp['distributions'][2])
                recent_cqa_df_tmp.columns = [info]
                recent_sa_list.append(recent_sa_df_tmp)
                recent_ca_list.append(recent_ca_df_tmp)
                recent_cqa_list.append(recent_cqa_df_tmp)

                post2database_df_tmp['algo_type_id'] = [223]
                post2database_df_tmp['risk_ratio'] = [int(equity_pct * 100)]
                post2database_df_tmp['sector'] = [sector_preference]
                post2database_df_tmp['invest_length'] = [region_preference]
                post2database_df_tmp['backtesting'] = [backtest_dict_tmp]
                post2database_df_tmp['weight'] = [weight_dict_tmp]
                post2database_df_tmp['fund_type'] = [data_dict_tmp['fund_type']]
                post2database_df_tmp['distributions'] = [data_dict_tmp['distributions']]
                post2database_list.append(post2database_df_tmp)

                print('Successfully output data for strategy', info, '!')

        benchmark_df = pd.read_csv(join(result_path, 'benchmark', str(equity_pct), 'pv.csv'), index_col=[0], parse_dates=[0])
        
        benchmark_df = benchmark_df[(benchmark_df.index >= start_date) & (benchmark_df.index <= end_date)]
        benchmark_df = benchmark_df/benchmark_df.iloc[0]

        info_bm = str(round(equity_pct, 2)) + '_' + 'benchmark'
        benchmark_df.columns = [info_bm]
        # benchmark_df[info_bm] = benchmark_df[info_bm].cumprod()
        # benchmark_df[info_bm] = benchmark_df[info_bm] / benchmark_df[info_bm][0]
        # benchmark_df.index = [c.date() for c in benchmark_df.index]
        byequity_pct_df = pd.concat(byequity_pct_list, axis=1)
        # benchmark_df.index = [c.date() for c in benchmark_df.index]
        return_withbm_df = pd.merge(byequity_pct_df, benchmark_df, left_index=True, right_index=True, how = 'outer').ffill()
        return_list.append(return_withbm_df)

    # =====================================
    # step 4: analyze and output the result
    # =====================================
    
    # Make 0.07_A1_B2, 0.07_A1_B3, etc...
    # make_duplicate_007_and_013()
    
    writer = pd.ExcelWriter(join(result_path, 'todatabase', 'weight_hist.xlsx'))
    for i in range(len(weight_df_list)):
        weight_df = weight_df_list[i]
        weight_df.to_excel(writer, sheet_name=str(round(test_dict['equity_pct'][i], 2)) + '_' + sector_preference + '_' + region_preference)
        
    writer.save()
    
    recent_weight_overall_df = pd.concat(recent_weight_list, axis=1).fillna(0)
    recent_weight_overall_df.index = recent_weight_overall_df.index.map(lambda x:secid_mapping[x] if x in secid_mapping.index else x)
    recent_weight_overall_df.index.name = 'ms_secid'
    recent_weight_overall_df = recent_weight_overall_df.reset_index()
    recent_weight_overall_df.index = recent_weight_overall_df['ms_secid'].map(lambda x: 'HK_20_' + x)

    recent_weight_overall_df_revised = recent_weight_overall_df.copy()
    recent_weight_overall_df_revised['ms_secid'] = ['HK_20_' + c for c in recent_weight_overall_df_revised['ms_secid']]
    recent_weight_overall_df_revised = recent_weight_overall_df_revised.replace('HK_20_F0GBR04SSN', 'HK_20_F0GBR053YB')
    recent_weight_overall_df_revised = recent_weight_overall_df_revised.replace('HK_20_F0GBR04AMI', 'HK_20_F0GBR053E5')
    recent_weight_overall_df_revised = recent_weight_overall_df_revised.replace('HK_20_F0GBR04K8L', 'HK_20_F0GBR053DP')
    recent_weight_overall_df_revised = recent_weight_overall_df_revised.replace('HK_20_F00000T2AE', 'HK_20_F00000VTAH')
    recent_weight_overall_df_revised = recent_weight_overall_df_revised.replace('HK_20_F0GBR04MRU', 'HK_20_F0GBR04MRX')
    recent_weight_overall_df_revised = recent_weight_overall_df_revised.set_index('ms_secid')

    recent_sa_df = pd.concat(recent_sa_list, axis = 1).fillna(0)
    recent_ca_df = pd.concat(recent_ca_list, axis = 1).fillna(0)
    recent_cqa_df = pd.concat(recent_cqa_list, axis = 1).fillna(0)

    return_overall_df = pd.concat(return_list, axis=1).ffill()

    post2database_df = pd.concat(post2database_list, axis=0)
    post2database_df = post2database_df.rename(columns = {'invest_length': 'region'})
    rebalance_overall_df = pd.concat(rebalance_list, axis = 0)
    
    metrics_input = return_overall_df.copy()
    metrics_input['YYYY'] = metrics_input.index.map(lambda x: x.year)
    metrics_input = metrics_input - 1
    
    report = metrics(metrics_input , 365)

    metrics_df = pd.DataFrame()
    metrics_df['total_return'] = return_overall_df.iloc[-1, :] - 1
    metrics_df['max_drawdown'] = (return_overall_df / return_overall_df.cummax() - 1).min()
    metrics_df['annualized_return'] = return_overall_df.iloc[-1, :] ** (250 / len(return_overall_df)) - 1
    metrics_df['annualized_volatility'] = (return_overall_df.pct_change()).std() * (250 ** 0.5)
    metrics_df['Sharpe_ratio'] = metrics_df['annualized_return'] / metrics_df['annualized_volatility']

    metrics_df.to_csv(join(output_path, 'metrics.csv'),index = True)

    recent_weight_overall_df_revised.to_csv(join(output_path, 'weight_overall.csv'), index=True)
    return_overall_df.to_csv(join(output_path, 'backtest_overall.csv'), index=True)
    post2database_df.to_csv(join(output_path, 'post2database.csv'), index=False)
    # post2database_df.to_excel(os.path.join(addpath.data_path, 'todatabase/post2database.xlsx'), index=False)
    # portfolio_feature_df.to_csv(join(output_path, 'portfolio_features.csv'), index = False)
    rebalance_overall_df.to_csv(join(output_path, 'rebalance_info.csv'), index = False)

    # ====================================
    #  construct portfolio list excel file
    # ====================================
    portfolio_dict = {'0.07_A1_B1': 'conservative_portfolio',
                      '0.17_A1_B1': 'stable_portfolio',
                      '0.27_A1_B1': 'balanced_portfolio',
                      '0.45_A1_B1': 'growth_portfolio',
                      '0.65_A1_B1': 'aggressive_portfolio'
                      }

    basic_list = list(portfolio_dict.keys())
    # comparison_list = [
    #                   '0.2_A1_B3', '0.2_benchmark', '0.2_beta_benchmark',
    #                   '0.4_A1_B3', '0.4_benchmark', '0.4_beta_benchmark',
    #                   '0.5_A1_B3', '0.5_benchmark', '0.5_beta_benchmark',
    #                   '0.6_A1_B3', '0.6_benchmark', '0.6_beta_benchmark',
    #                   '0.7_A1_B3', '0.7_benchmark', '0.7_beta_benchmark']
    benchmark_dict = {'0.07_benchmark': 'conservative_benchmark',
                       '0.17_benchmark': 'stable_benchmark',
                       '0.27_benchmark': 'balanced_benchmark',
                       '0.45_benchmark': 'growth_benchmark',
                       '0.65_benchmark': 'aggressive_benchmark'}
    bm_list = list(benchmark_dict.keys())
    comparison_list = basic_list + bm_list

    return_basic_df = return_overall_df[basic_list].rename(columns = portfolio_dict).rename(columns = benchmark_dict)
    return_comparison_df_tmp = return_overall_df[comparison_list].rename(columns = portfolio_dict).rename(columns = benchmark_dict)

    return_comparison_df = pd.merge(return_comparison_df_tmp, index_df[[equity_benchmark]], left_index=True, right_index=True, how = 'outer').ffill()
    return_comparison_df = pd.merge(return_comparison_df, index_df[[bond_benchmark]], left_index=True, right_index=True, how = 'outer').ffill()

    return_comparison_df.index = pd.to_datetime(return_comparison_df.index)
    return_comparison_df = return_comparison_df[
        np.logical_and(return_comparison_df.index >= start_dt, return_comparison_df.index <= end_dt)]
    return_comparison_df.index = [c.date() for c in return_comparison_df.index]
    return_comparison_df[equity_benchmark] = return_comparison_df[equity_benchmark]/return_comparison_df[equity_benchmark].bfill()[0]
    return_comparison_df[bond_benchmark] = return_comparison_df[bond_benchmark]/return_comparison_df[bond_benchmark].bfill()[0]

    metrics_comparison_df = metrics_df.loc[comparison_list, :].transpose()\
        .rename(columns = portfolio_dict).rename(columns = benchmark_dict).transpose()

    recent_weight_basic_df = recent_weight_overall_df[['ms_secid'] + basic_list].dropna(subset = basic_list, how = 'all')
    recent_weight_basic_df = recent_weight_basic_df.replace(0, np.nan).dropna(subset = basic_list, how = 'all').fillna(0)
    recent_weight_basic_df = recent_weight_basic_df.set_index('ms_secid').rename(columns = portfolio_dict).rename(columns = benchmark_dict)
    # recent_weight_basic_df_copy = recent_weight_basic_df.copy()
    # recent_weight_basic_df['wind_code'] = summary_df.loc[recent_weight_basic_df.index, 'wind_code']
    # recent_weight_basic_df.index.name = 'wind_code'
    # recent_weight_basic_df = recent_weight_basic_df.reset_index().set_index('ms_secid')

    rebalance_dates_df = pd.read_csv(join(config_path, 'rebalance_dates.csv'), parse_dates=[0])
    dates_list = rebalance_dates_df[np.logical_and(rebalance_dates_df['rebalance_dates'] >= start_dt,
                                                   rebalance_dates_df['rebalance_dates'] <= end_dt)]['rebalance_dates'].tolist()
    dates_list.sort()
    filename = str(dates_list[-1].date()) + '.csv'
    pool_df = pd.read_csv(join(data_path, 'pool', '12M', filename), index_col='ms_secid')
    pool_df.index = pool_df.index.map(lambda x:secid_mapping[x] if x in secid_mapping.index else x)
    recent_weight_basic_df['class'] = pool_df.loc[recent_weight_basic_df.index, 'class']
    recent_weight_basic_df = recent_weight_basic_df.reset_index().set_index('class')
    # class_order = ['Global', 'China', 'US', 'European DM', 'APAC', 'Emerging Market',
    #                'Global IG', 'US IG', 'Other IG', 'High Yield']
    class_order = ['Global', 'China', 'US', 'European DM', 'APAC', 'Emerging Market',
                   'Global IG', 'Global IG 2', 'US IG', 'Other IG', 'High Yield']
    loop_class = pd.Index(class_order).drop('Global IG 2').tolist()
    recent_weight_basic_df = recent_weight_basic_df.loc[class_order, :]
    recent_weight_basic_df = recent_weight_basic_df.reset_index()
    eng_name_df_list = []
    
    for clas in loop_class:
        eng_name_df_tmp = pd.read_excel(join(config_path, 'white_list_Wing_Lung.xlsx'), sheet_name = clas)
        eng_name_df_tmp = eng_name_df_tmp[['FUND ENG NAME', 'ms_secid']]
        eng_name_df_list.append(eng_name_df_tmp)
    eng_name_df = pd.concat(eng_name_df_list, axis = 0).drop_duplicates()
    recent_weight_basic_df = pd.merge(eng_name_df, recent_weight_basic_df, left_on='ms_secid', right_on = 'ms_secid', how = 'right')
    recent_weight_basic_df['iuid'] = recent_weight_basic_df['ms_secid'].map(lambda x: 'HK_20_' + x)
    recent_weight_basic_df = recent_weight_basic_df.replace('HK_20_F0GBR04SSN', 'HK_20_F0GBR053YB')
    recent_weight_basic_df = recent_weight_basic_df.replace('HK_20_F0GBR04AMI', 'HK_20_F0GBR053E5')
    recent_weight_basic_df = recent_weight_basic_df.replace('HK_20_F0GBR04K8L', 'HK_20_F0GBR053DP')
    recent_weight_basic_df = recent_weight_basic_df.replace('HK_20_F00000T2AE', 'HK_20_F00000VTAH')
    recent_weight_basic_df = recent_weight_basic_df.replace('HK_20_F0GBR04MRU', 'HK_20_F0GBR04MRX')
    recent_weight_basic_df = recent_weight_basic_df.set_index('iuid')

    recent_sa_basic_df = recent_sa_df[basic_list]
    recent_ca_basic_df = recent_ca_df[basic_list]
    recent_cqa_basic_df = recent_cqa_df[basic_list]
    
    value = pd.read_csv( join(result_path, 'todatabase', 'backtest_overall.csv')  , index_col=0)
    benchmark = pd.read_csv( join(result_path, 'todatabase', 'risk_matching_benchmark.csv') , index_col=0)
    value_risk_metrics = calculate_2Y_vol(pv=value, benchmark=benchmark)
    value_risk_metrics.to_csv( join(result_path, 'todatabase', 'risk_matching.csv') )

    writer = pd.ExcelWriter(join(output_path, 'CMB Wing Lung Bank Algorithm Output.xlsx'))
    return_basic_df.to_excel(writer, sheet_name='Performance')
    return_comparison_df.to_excel(writer, sheet_name='Comparison with benchmark')
    metrics_comparison_df.to_excel(writer, sheet_name='Performance Stats')
    recent_weight_basic_df.to_excel(writer, sheet_name='Weighting')
    recent_sa_basic_df.to_excel(writer, sheet_name='Sector Distribution')
    recent_ca_basic_df.to_excel(writer, sheet_name='Country Distribution')
    recent_cqa_basic_df.to_excel(writer, sheet_name='Credit Distribution')
    value_risk_metrics.to_excel(writer, sheet_name='波動指標')
    # primary_backup_df.to_excel(writer, sheet_name='主备选基金')
    writer.save()


if __name__ == '__main__':
    start = datetime.now()
    equity_pct_df = pd.read_csv(join(config_path, 'equity_pct.csv'))
    test_dict = {
            'equity_pct': equity_pct_df['equity_pct'].tolist(),
            # 'sector_preference': ['A1', 'A2', 'A3', 'A4', 'A5'],
            # 'region_preference': ['B1', 'B2', 'B3', 'B4', 'B5']
            'sector_preference': ['A1'],
            'region_preference': ['B1']
                 }

    # if type == 'skip_bundle', then skip downloading and injecting bundles; if type == 'skip_download_bundle', then skip downloading bundles.
    weighting_backtesting(start_date = '2016-02-29', end_date = '2020-12-31', output_path=join(result_path, 'todatabase'),
                          test_dict=test_dict, type = 'skip_download_fake_strategy', foropt_prepare=False, update_benchmark=False)
    end = datetime.now()
    print((end - start).seconds)