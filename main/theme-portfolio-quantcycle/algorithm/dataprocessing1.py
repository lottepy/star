

#处理财务数据成港股格式
# import pandas as pd
# import numpy
# import os
# import warnings
# warnings.filterwarnings('ignore')
# import datetime


#设置路径
# read_path = r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/us_data/Quarterly Financial csv'
# save_path = r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/us_data/financials'
# os.chdir(read_path)
# csv_name_list = os.listdir()
#csv_name_list.remove('desktop.ini')

# sample=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\hk_data\financials\1 HK Equity.csv')
# sample.loc[:, 'date'] = pd.to_datetime(sample.iloc[:,0], format='%Y/%m/%d', errors='ignore')
# sample.index=sample.loc[:,'date']
# sample=sample.drop(columns=sample.columns[0])
# sample=sample.drop(columns=['date'])
# sample=pd.DataFrame(columns=['ANNOUNCEMENT_DT','BS_TOT_ASSET','BS_TOT_LIAB2','TOT_EQUITY','SALES_REV_TURN',\
#                              'EBIT','NET_INCOME','CF_CASH_FROM_OPER','EBITDA','NET_OPER_INCOME',\
#                              'CF_CAP_EXPEND_PARTY_ADD','IS_INT_EXPENSE'])
#
# for csv_name in csv_name_list:
#     print(csv_name)
#     datain=pd.d_csv(read_path+'\\'+csv_name)
#     datain.loc[:, 'date'] = pd.to_datetime(datain['datadate'], format='%Y/%m/%d', errors='ignore')
#     datain.index=datain.loc[:,'date']
#
#     dataout = pd.DataFrame(index=datain.index,columns=sample.columns)
#     dataout.loc[:, 'ANNOUNCEMENT_DT']=datain.loc[:, 'rdq']
#     dataout.loc[:, 'BS_TOT_ASSET'] = datain.loc[:, 'atq']
#     dataout.loc[:, 'BS_TOT_LIAB2'] = datain.loc[:, 'ltq']
#     dataout.loc[:, 'TOT_EQUITY'] = datain.loc[:, 'teqq']
#     dataout.loc[:, 'LONGTERM_DEBT'] = datain.loc[:, 'dlttq']
#     dataout.loc[:, 'LONGTERM_LIAB'] = datain.loc[:, 'lltq']
#     dataout.loc[:, 'CUR_ASSET'] = datain.loc[:, 'actq']
#     dataout.loc[:, 'CUR_LIAB'] = datain.loc[:, 'lctq']
#     dataout.loc[:, 'INVENTORY'] = datain.loc[:, 'invtq']
#     dataout.loc[:, 'CASH'] = datain.loc[:, 'chq']
#     dataout.loc[:, 'CASHnSTINVES'] = datain.loc[:, 'cheq']
#
#     dataout.loc[:, 'SALES_REV_TURN_Q'] = datain.loc[:, 'saleq']
#     dataout.loc[:, 'REVENUE_TOT_Q'] = datain.loc[:, 'revtq']
#     dataout.loc[:, 'COST_SOLD_Q'] = datain.loc[:, 'cogsq']
#     dataout.loc[:, 'RnD_EXPENSE_Q'] = datain.loc[:, 'xrdq']
#     dataout.loc[:, 'DEPRnAMOR_Q'] = datain.loc[:, 'dpq']
#     #dataout.loc[:, 'RnD_PROCESS_Q'] = datain.loc[:, 'rdipq']
#     dataout.loc[:, 'OPER_EXPENSE_Q'] = datain.loc[:, 'xoprq']
#     dataout.loc[:, 'INTEREST_EXP_Q'] = datain.loc[:, 'xintq']
#     dataout.loc[:, 'TAX_Q'] = datain.loc[:, 'txtq']
#     # dataout.loc[:, 'EBIT_Q'] = datain.loc[:, 'oibdpq']
#     # dataout.loc[:, 'EBT_Q'] = datain.loc[:, 'oiadpq']
#     dataout.loc[:, 'NET_INCOME_Q'] = datain.loc[:, 'niq']
#     dataout.loc[:, 'COM_INCOME_Q'] = datain.loc[:, 'ciq']
#     dataout.loc[:, 'CF_CASH_FROM_OPER_Q'] = dataout.loc[:, 'REVENUE_TOT_Q']-\
#                                             dataout.loc[:, 'OPER_EXPENSE_Q']-dataout.loc[:, 'TAX_Q']
#
#
#     dataout.loc[:, 'SALES_REV_TURN'] = dataout.loc[:, 'SALES_REV_TURN_Q']+dataout.loc[:, 'SALES_REV_TURN_Q'].shift()+dataout.loc[:, 'SALES_REV_TURN_Q'].shift(2)+dataout.loc[:, 'SALES_REV_TURN_Q'].shift(3)
#     dataout.loc[:, 'REVENUE_TOT'] = datain.loc[:, 'revtttm']
#     dataout.loc[:, 'COST_SOLD'] = datain.loc[:, 'cogsttm']
#     dataout.loc[:, 'RnD_EXPENSE'] = datain.loc[:, 'xrdttm']
#     dataout.loc[:, 'DEPRnAMOR'] = datain.loc[:, 'dpttm']
#     #dataout.loc[:, 'RnD_PROCESS'] = datain.loc[:, 'rdipttm']
#     dataout.loc[:, 'OPER_EXPENSE'] = datain.loc[:, 'xoprttm']
#     dataout.loc[:, 'INTEREST_EXP'] = datain.loc[:, 'xintttm']
#     dataout.loc[:, 'TAX'] = datain.loc[:, 'txtttm']
#     # dataout.loc[:, 'EBIT'] = datain.loc[:, 'oibdpttm']
#     # dataout.loc[:, 'EBT'] = datain.loc[:, 'oiadpttm']
#     dataout.loc[:, 'NET_INCOME'] = datain.loc[:, 'nittm']
#     dataout.loc[:, 'COM_INCOME'] = datain.loc[:, 'cittm']
#     dataout.loc[:, 'CF_CASH_FROM_OPER'] = dataout.loc[:, 'REVENUE_TOT']-dataout.loc[:, 'OPER_EXPENSE']-dataout.loc[:, 'TAX']
#
#
#
#     dataout=dataout.resample('Q').last()
#     # flowitem=['SALES_REV_TURN','EBIT','NET_INCOME','CF_CASH_FROM_OPER']
#     # dataout.loc[:,flowitem]=dataout.loc[:,flowitem]+dataout.loc[:,flowitem].shift()+dataout.loc[:,flowitem].shift(2)\
#     #                         +dataout.loc[:,flowitem].shift(3)
#     dataout.to_csv(save_path+'\\'+csv_name,index=True)




'''
#输出港股通symbol_list
symbol_list=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\config\CN_Quality\symbol_list_raw.csv')
data=pd.DataFrame()
for symbol in symbol_list.loc[:,'symbol']:
    filename = r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\data\cn_data\trading'+'\\'+symbol+'.csv'
    if os.path.exists(filename):
        symbol=list([symbol])
        symbol = pd.DataFrame(symbol)
        data=pd.concat([data,symbol],axis=0)
data.columns = list(['symbol'])                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
data.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\config\CN_Quality\symbol_list.csv',index=False)


#downloan financial data from datamater for replicating caixin factors
import datetime
from datamaster import dm_client

dm_client.start()

portfolio_name = "CN_caixin"
download_start = '2020-1-1'
download_end = '2020-8-30'

symbol_list=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\config\CN_Quality\symbol_list.csv')
data=pd.DataFrame()
CHOICE_FINANCIALS = {'bbg_announcement_dt': 'ANNOUNCEMENT_DT',
                     'choice_totalasset': 'BS_TOT_ASSET',
                     'choice_totalliab': 'BS_TOT_LIAB2',
                     'choice_stkhldereq': 'TOT_EQUITY',
                     'choice_operarevenue': 'SALES_REV_TURN',
                     'choice_optoebt': 'EBIT',
                     'bbg_net_oper_income': 'NET_OPER_INCOME',
                     'choice_teofholder': 'NET_INCOME',
                     'choice_opercashflow': 'CF_CASH_FROM_OPER',
                     'bbg_cf_cap_expend_prpty_add': 'CF_CAP_EXPEND_PRPTY_ADD',
                     'bbg_is_int_expense': 'IS_INT_EXPENSE',
                     'bbg_ebitda': 'EBITDA',
                     'choice_gpmargin':'GP_SALEREV_Ratio',
                     'choice_profitbetax':'EBT',
                     'choice_qfa_opprofit':'OPPROFIT',
                     'choice_ltborrow':'LONGTERM_BORROW',
                     'choice_cash':'CASH',

                     'choice_shortterm_borrow': 'SHORTTERM_BORROW',
                     'choice_inventory':'INVENTORY',
                     'choice_settlement_provisions': 'DEPOSIT_RESERVATION',
                     'choice_lending_money': 'FUND_LENDED',
                     'choice_fund_provide': 'FUND_PROVIDED',
                     'choice_trans_fin_assets': 'TRANS_FIN_ASSETS',
                     'choice_fair_value_profits_equity': 'FAIR_VALUE_PROFITS_EQUITY',
                     'choice_deriv_fin_assets': 'DERIV_FIN_ASSETS',
                     'choice_buy_sell_fin_assets': 'BUY_SOLD_FIN_ASSETS',
                     'choice_amortized_cost_equity': 'AMORTIZED_COST_FIN_EQUITY',
                     'choice_fair_value_otherprofits_equity': 'FAIR_VALUE_OTHERPROFITS_FIN_EQUITY',
                     'choice_contract_equity': 'CONTRACT_EQUITY',
                     'choice_onsale_equity': 'ONSALE_EQUITY',
                     'choice_noncurrent_assets_1y': 'NONCURRENT_ASSETS_1Y',
                     'choice_issue_loans_advances': 'ISSUE_LOANSNADVANCES',
                     'choice_creditor_invest': 'CREDITOR_INVEST',
                     'choice_creditor_invest_other': 'CREDITOR_INVEST_OTHER',
                     'choice_amortized_cost_noncurrent_equity': 'AMORTIZED_COST_NONCURRENT_FIN_EQUITY',
                     'choice_fair_value_otherprofits_noncurrenct_equity': 'FAIR_VALUE_OTHERPROFITS_NONCURRENCT_FIN_EQUITY',
                     'choice_onsale_fin_assets': 'ONSALE_FIN_ASSETS',
                     'choice_hold_to_maturity_invest': 'HOLD_TO_MATURITY_INVEST',
                     'choice_longterm_equity_invest': 'LONGTERM_EQUITY_INVEST',
                     'choice_invest_real_estate': 'REAL_ESTATE_INVEST',
                     'choice_equity_invest_other': 'EQUITY_INVEST_OTHER',
                     'choice_noncurrent_equity_other': 'NONCURRENT_EQUITY_OTHER'
                     }


for symbol in symbol_list.loc[:,'symbol']:
    print(symbol)
    res3 = dm_client.historical(symbols=symbol, start_date=download_start,end_date=download_end,\
                                fields=','.join(list(CHOICE_FINANCIALS.keys())))
    res_output_3 = pd.DataFrame(res3['values'][symbol], columns=res3['fields'])
    res_output_3['date'] = pd.to_datetime(res_output_3['date'])

    res_output_3.rename(columns=CHOICE_FINANCIALS, inplace=True)

    res_output_3.set_index('date', inplace=True)
    res_output_3.to_csv(r'D:\AQUMON\ThemeProject\data\financials_new\\' + symbol + '.csv')


'''
# CHOICE_FINANCIALS = {'bbg_announcement_dt': 'ANNOUNCEMENT_DT',
#                      'choice_totalasset': 'BS_TOT_ASSET',
#                      'choice_totalliab': 'BS_TOT_LIAB2',
#                      'choice_stkhldereq': 'TOT_EQUITY',
#                      'choice_operarevenue': 'SALES_REV_TURN',
#                      'choice_optoebt': 'EBIT',
#                      'bbg_net_oper_income': 'NET_OPER_INCOME',
#                      'choice_teofholder': 'NET_INCOME',
#                      'choice_opercashflow': 'CF_CASH_FROM_OPER',
#                      'bbg_cf_cap_expend_prpty_add': 'CF_CAP_EXPEND_PRPTY_ADD',
#                      'bbg_is_int_expense': 'IS_INT_EXPENSE',
#                      'bbg_ebitda': 'EBITDA',
#                      'choice_gpmargin':'GP_SALEREV_Ratio',
#                      'choice_profitbetax':'EBT',
#                      'choice_qfa_opprofit':'OPPROFIT',
#                      'choice_ltborrow':'LONGTERM_BORROW',
#                      'choice_cash':'CASH',
#
#                      'choice_trans_fin_assets1':'SPECIFIC_TRANS_FIN_ASSETS',
#                      'choice_central_bank_borrow':'CENTRAL_BANK_BORROW',
#                      'choice_borrow_fund':'FUND_BORROWED',
#                      'choice_deriv_liab':'DERIV_FIN_LIAB',
#                      'choice_sale_repurchase_asset':'SOLD_REPUR_FIN_ASSETS',
#                      'choice_bill_receivable':'BILL_RECEIVABLE',
#                      'choice_accounts_receivable':'ACCOUNTS_RECEIVABLE',
#                      'choice_shortterm_bond':'SHORTTERM_BOND',
#                      'choice_deposits_interbank':'DEPOSIT_INTERBANK',
#                      'choice_fair_value_profits_equity':'FAIR_VALUE_PROFITS_EQUITY',
#                      'choice_fair_value_profits_equity1':'SPECIFIC_FAIR_VALUE_PROFITS_EQUITY',
#                      'choice_inventory':'INVENTORY',
#                      'choice_fair_value_profits_liab':'FAIR_VALUE_PROFITS_LIAB',
#                      'choice_fair_value_profits_liab1': 'SPECIFIC_FAIR_VALUE_PROFITS_LIAB',
#                      'choice_fund_provide': 'FUND_PROVIDED',
#                      'choice_preferred_stock': 'PREFERRED_STOCK',
#                      'choice_sustainable_debt': 'SUSTAINABLE_DEBT',
#                      'choice_noncurrent_assets_1y': 'NONCURRENT_ASSETS_1Y',
#                      'choice_onsale_equity': 'ONSALE_EQUITY',
#                      'choice_amortized_cost_equity': 'AMORTIZED_COST_FIN_EQUITY',
#                      'choice_fair_value_otherprofits_equity': 'FAIR_VALUE_OTHERPROFITS_FIN_EQUITY',
#                      'choice_amortized_cost_liab': 'AMORTIZED_COST_FIN_LIAB',
#                      'choice_contract_equity': 'CONTRACT_EQUITY',
#                      'choice_amortized_cost_noncurrent_equity': 'AMORTIZED_COST_NONCURRENT_FIN_EQUITY',
#                      'choice_fair_value_otherprofits_noncurrenct_equity': 'FAIR_VALUE_OTHERPROFITS_NONCURRENCT_FIN_EQUITY',
#                      'choice_amortized_cost_noncurrent_liab': 'AMORTIZED_COST_NONCURRENT_FIN_LIAB',
#                      'choice_notes&accounts_receivable': 'NOTESNACCOUNTS_RECEIVABLE',
#                      'choice_creditor_invest': 'CREDITOR_INVEST',
#                      'choice_creditor_invest_other': 'CREDITOR_INVEST_OTHER',
#                      'choice_equity_invest_other': 'EQUITY_INVEST_OTHER',
#                      'choice_noncurrent_equity_other': 'NONCURRENT_EQUITY_OTHER',
#                      'choice_notes&accounts_payable': 'NOTESNACCOUNTS_PAYABLE',
#                      'choice_trans_fin_assets': 'TRANS_FIN_ASSETS',
#                      'choice_trans_fin_liab': 'TRANS_FIN_LIAB',
#
#                      'choice_total_other_payable': 'TOTAL_OTHER_PAYABLE',
#                      'choice_lease_liab': 'LEASE_LIAB',
#                      'choice_onsale_fin_assets': 'ONSALE_FIN_ASSETS',
#                      'choice_hold_to_maturity_invest': 'HOLD_TO_MATURITY_INVEST',
#                      'choice_invest_real_estate': 'REAL_ESTATE_INVEST',
#                      'choice_longterm_equity_invest': 'LONGTERM_EQUITY_INVEST',
#                      'choice_lending_money': 'FUND_LENDED',
#                      'choice_deriv_fin_assets': 'DERIV_FIN_ASSETS',
#                      'choice_buy_sell_fin_assets': 'BUY_SOLD_FIN_ASSETS',
#                      'choice_issue_loans_advances': 'ISSUE_LOANS_ADVANCES',
#                      'choice_settlement_provisions': 'DEPOSIT_RESERVATION',
#                      'choice_shortterm_borrow': 'SHORTTERM_BORROW',
#                      'choice_trans_fin_liab1': 'SPECIFIC_TRANS_FIN_LIAB',
#                      'choice_notes_payable': 'NOTES_PAYABLE',
#                      'choice_accounts_payable': 'ACCOUNTS_PAYABLE',
#                      'choice_interest_payable': 'INTEREST_PAYABLE',
#                      'choice_dividends_payable': 'DIVIDENDS_PAYABLE',
#                      'choice_other_payables': 'OTHER_PAYABLES',
#                      'choice_noncurrent_liab_1y': 'NONCURRENT_LIAB_1Y',
#                      'choice_bonds_payable': 'BONDS_PAYABLE',
#                      'choice_fixed_assets_cash_payable': 'FIXED_ASSETS_CASH_PAYABLE',
#                      'choice_qfa_net_cash_flows_fin_act': 'QFA_NET_CASH_FLOWS_FIN_ACT',
#                      'choice_divannuaccum': 'DIVANNUACCUM',
#                      'choice_divcashpsbftaxp': 'DIVCASHPSBFTAXP',
#                      'choice_oper_cost_quarter': 'OPER_COST_QUARTER',
#                      'choice_interest_income_quarter': 'INTEREST_INCOME_QUARTER',
#                      'choice_interest_payments_quarter': 'INTEREST_PAYMENTS_QUARTER',
#                      'choice_profit_tot_qurater': 'PROFIT_TOT_QURATER',
#                      'choice_research_cost_qurater': 'RESEARCH_COST_QURATER',
#                      'choice_oper_income_qurater': 'OPER_INCOME_QURATER'
#                      }
#
# #处理数据用以复制财新指数
# symbol_list=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio-master\config\CN_Quality\symbol_list.csv')
# dataBS=pd.read_csv(r'D:\AQUMON\ThemeProject\data\CNStockfinancial\balancesheet\balancesheet.csv',dtype={'Stkcd':str})
# dataCF=pd.read_csv(r'D:\AQUMON\ThemeProject\data\CNStockfinancial\cashflow\cashflow.csv',dtype={'Stkcd':str})
# dataIS=pd.read_csv(r'D:\AQUMON\ThemeProject\data\CNStockfinancial\incomestament\incomestatement.csv',dtype={'Stkcd':str})
#
# for symbol in symbol_list.loc[:,'symbol']:
#     print(symbol)
#     symbol_raw=symbol
#     symbol=symbol[0:6]
#     dfBS = dataBS[dataBS.loc[:, 'Stkcd'] == symbol][dataBS.loc[:, 'Typrep'] == 'A']
#     dfBS.loc[:,'date']=pd.to_datetime(dfBS.loc[:,'Accper'], format='%Y-%m-%d', errors='ignore')
#     dfBS.set_index('date', inplace=True)
#     dfCF=dataCF[dataCF.loc[:, 'Stkcd'] == symbol][dataCF.loc[:, 'Typrep'] == 'A']
#     dfCF.loc[:, 'date'] = pd.to_datetime(dfCF.loc[:,'Accper'], format='%Y-%m-%d', errors='ignore')
#     dfCF.set_index('date', inplace=True)
#     dfIS=dataIS[dataIS.loc[:, 'Stkcd'] == symbol][dataIS.loc[:, 'Typrep'] == 'A']
#     dfIS.loc[:, 'date'] = pd.to_datetime(dfIS.loc[:,'Accper'], format='%Y-%m-%d', errors='ignore')
#     dfIS.set_index('date', inplace=True)
#     dataout = pd.DataFrame(index=dfBS.index,columns=list(CHOICE_FINANCIALS.values()))
#
#     dataout.loc[:,'SPECIFIC_TRANS_FIN_ASSETS']=dfBS.loc[:,'A001107000']
#     dataout.loc[:,'CENTRAL_BANK_BORROW']=dfBS.loc[:,'A0b2102000']
#     dataout.loc[:,'FUND_BORROWED']=dfBS.loc[:,'A0f2104000']
#     dataout.loc[:,'DERIV_FIN_LIAB']=dfBS.loc[:,'A0f2106000']
#     dataout.loc[:,'SOLD_REPUR_FIN_ASSETS']=dfBS.loc[:,'A0f2110000']
#     dataout.loc[:,'BILL_RECEIVABLE']=dfBS.loc[:,'A001110000']
#     dataout.loc[:,'ACCOUNTS_RECEIVABLE']=dfBS.loc[:,'A001111000']
#     dataout.loc[:,'DEPOSIT_INTERBANK']=dfBS.loc[:,'A0b2103000']
#     dataout.loc[:,'INVENTORY']=dfBS.loc[:,'A001123000']
#     dataout.loc[:,'NONCURRENT_ASSETS_1Y']=dfBS.loc[:,'A001124000']
#     dataout.loc[:,'EQUITY_INVEST_OTHER']=dfBS.loc[:,'A003112000']
#     dataout.loc[:,'TRANS_FIN_ASSETS']=dfBS.loc[:,'A001107000']
#     dataout.loc[:,'TRANS_FIN_LIAB']=dfBS.loc[:,'A002105000']
#     dataout.loc[:, 'CURRENT_ASSET'] = dfBS.loc[:, 'A001100000']
#     dataout.loc[:, 'ONSALE_FIN_ASSETS'] = dfBS.loc[:, 'A001202000']
#     dataout.loc[:, 'HOLD_TO_MATURITY_INVEST'] = dfBS.loc[:, 'A001203000']
#     dataout.loc[:, 'REAL_ESTATE_INVEST'] = dfBS.loc[:, 'A001211000']
#     dataout.loc[:, 'LONGTERM_EQUITY_INVEST'] = dfBS.loc[:, 'A001205000']
#     dataout.loc[:, 'BUSINESS_REPUTATION'] = dfBS.loc[:, 'A001220000']
#     dataout.loc[:, 'FUND_LENDED'] = dfBS.loc[:, 'A0f1106000']
#     dataout.loc[:, 'DERIV_FIN_ASSETS'] = dfBS.loc[:, 'A0f1108000']
#     dataout.loc[:, 'BUY_SOLD_FIN_ASSETS'] = dfBS.loc[:, 'A0f1122000']
#     dataout.loc[:, 'ISSUE_LOANS_ADVANCES'] = dfBS.loc[:, 'A0b1201000']
#     dataout.loc[:, 'DEPOSIT_RESERVATION'] = dfBS.loc[:, 'A0d1102000']
#     dataout.loc[:, 'SHORTTERM_BORROW'] = dfBS.loc[:, 'A002101000']
#     dataout.loc[:, 'SPECIFIC_TRANS_FIN_LIAB'] = dfBS.loc[:, 'A002105000']
#     dataout.loc[:, 'NOTES_PAYABLE'] = dfBS.loc[:, 'A002107000']
#     dataout.loc[:, 'ACCOUNTS_PAYABLE'] = dfBS.loc[:, 'A002108000']
#     dataout.loc[:, 'INTEREST_PAYABLE'] = dfBS.loc[:, 'A002114000']
#     dataout.loc[:, 'DIVIDENDS_PAYABLE'] = dfBS.loc[:, 'A002115000']
#     dataout.loc[:, 'NONCURRENT_LIAB_1Y'] = dfBS.loc[:, 'A002125000']
#     dataout.loc[:, 'CASH'] = dfBS.loc[:, 'A001101000']
#     dataout.loc[:, 'CURRENT_LIABILITY'] = dfBS.loc[:, 'A002100000']
#     dataout.loc[:, 'LONGTERM_BORROW'] = dfBS.loc[:, 'A002201000']
#     dataout.loc[:, 'BONDS_PAYABLE'] = dfBS.loc[:, 'A002203000']
#     dataout.loc[:, 'CF_CASH_FROM_OPER']= dfCF.loc[:, 'C001000000']
#     dataout.loc[:, 'FIXED_ASSETS_CASH_PAYABLE'] = dfCF.loc[:, 'C002006000']
#     dataout.loc[:, 'QFA_NET_CASH_FLOWS_FIN_ACT'] = dfCF.loc[:, 'C003000000']
#     dataout.loc[:, 'OPER_COST_QUARTER'] = dfIS.loc[:, 'B001201000']
#     dataout.loc[:, 'INTEREST_INCOME_QUARTER'] = dfIS.loc[:, 'Bbd1102101']
#     dataout.loc[:, 'INTEREST_PAYMENTS_QUARTER'] = dfIS.loc[:, 'Bbd1102203']
#     dataout.loc[:, 'OPER_REVENUE'] = dfIS.loc[:, 'B001300000']
#     dataout.loc[:, 'PROFIT_TOT_QURATER'] = dfIS.loc[:, 'B001000000']
#     dataout.loc[:, 'TAX'] = dfIS.loc[:, 'B002100000']
#     dataout.loc[:, 'NET_INCOME'] = dfIS.loc[:, 'B002000000']
#     dataout.loc[:, 'OPER_INCOME_QURATER'] = dfIS.loc[:, 'B001101000']
#     dataout=dataout.sort_index()
#     dataout = dataout.resample('Q').last()
#     dataout.loc[:,'date']=dataout.index
#     dataout['YYYY'] = dataout['date'].map(lambda x: x.year)
#     dataout['MM'] = dataout['date'].map(lambda x: x.month)
#     dataout.set_index('YYYY', inplace=True)
#     temp_columnlist=['CF_CASH_FROM_OPER','FIXED_ASSETS_CASH_PAYABLE','QFA_NET_CASH_FLOWS_FIN_ACT',\
#                                   'OPER_COST_QUARTER','INTEREST_INCOME_QUARTER','INTEREST_PAYMENTS_QUARTER',\
#                                   'OPER_REVENUE','PROFIT_TOT_QURATER','TAX','NET_INCOME','OPER_INCOME_QURATER','MM']
#     temp=dataout.loc[:,temp_columnlist]
#     temp_new=pd.DataFrame(columns=temp.columns,index=temp.index)
#     temp_new.loc[:,'MM']=temp.MM
#     temp_new[temp.MM == 12] = temp[temp.MM == 12] - temp[temp.MM == 9]
#     temp_new[temp.MM == 9] = temp[temp.MM == 9] - temp[temp.MM == 6]
#     temp_new[temp.MM == 6] = temp[temp.MM == 6] - temp[temp.MM == 3]
#     temp_new[temp.MM == 3] = temp[temp.MM == 3]
#     temp_new=temp_new.drop(columns=['MM'])
#     temp_new=temp_new+temp_new.shift()+temp_new.shift(2)+temp_new.shift(3)
#     temp_new['date']=dataout.date
#     temp_new.set_index('date', inplace=True)
#     dataout=dataout.drop(columns=temp_columnlist)
#     dataout.set_index('date', inplace=True)
#     dataout=pd.concat([dataout,temp_new],axis=1)
#     dataout.to_csv(r'D:\AQUMON\ThemeProject\data\dataout\\' + symbol_raw + '.csv')
#
#     datain=pd.read_csv(r'D:\AQUMON\ThemeProject\data\financials_new\\'+symbol_raw+'.csv')
#     datain.loc[:, 'date'] = pd.to_datetime(datain.loc[:, 'date'], format='%Y/%m/%d', errors='ignore')
#     datain.set_index('date', inplace=True)
#     dataout=pd.concat([datain,dataout],axis=0)
#
#     dataold=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\cn_data\financials_wind\\'+symbol_raw+'.csv')
#     dataold.loc[:, 'date'] = pd.to_datetime(dataold.loc[:, 'date'], format='%Y/%m/%d', errors='ignore')
#     dataold.set_index('date', inplace=True)
#     dataold=dataold.drop(columns=['ANNOUNCEMENT_DT','NET_OPER_INCOME','CF_CAP_EXPEND_PRPTY_ADD','EBITDA','IS_INT_EXPENSE'])
#     dataout=dataout.drop(columns=['BS_TOT_ASSET','BS_TOT_LIAB2','TOT_EQUITY','SALES_REV_TURN','EBIT','NET_INCOME',\
#                           'CF_CASH_FROM_OPER','LONGTERM_BORROW'])
#     dataout=pd.merge(left=dataold, right=dataout, left_index=True, right_index=True, how='outer')
#
#     dataout.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\cn_data\financials\\'+symbol_raw+'.csv')
#


#处理美股symbol_list

# import pandas as pd
# import numpy
# import os
# import warnings
# warnings.filterwarnings('ignore')
# import datetime
#
#
# #设置路径
# read_path1 = r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\us_data\financials'
# read_path2 = r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\us_data\trading'

# os.chdir(read_path1)
# csv_name_list1 = os.listdir()
# os.chdir(read_path2)
# csv_name_list2 = os.listdir()
#
# csv_name_list=[]
# for csv_name in csv_name_list1:
#     if csv_name in csv_name_list2:
#         csv_name_list.append(csv_name)
#
# for i in range(len(csv_name_list)):
#     csv_name_list[i]=csv_name_list[i][:-4]
#
# csv_name_list=pd.DataFrame(csv_name_list,columns=['symbol'])
# csv_name_list.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\US_Quality\symbol_list.csv')


# #choice上导出基金优选白名单申赎状态
# from choice_client import c
# import pandas as pd
# import datetime
# from retry import retry
#
# @retry()
# def datasearch(symbol,field,parastr):
#     data=c.css(symbol,field,parastr)
#     return data
#
# fund_list=pd.read_csv(r'D:\AQUMON\whitelist\whitelist.csv')
# fund_list=fund_list['wind_ticker']
# start=datetime.datetime(2012,1,1)
# end = datetime.datetime(2020,9,1)
# date_list=pd.date_range(start=start,end=end,freq='M')
# PURCHSTATUS=pd.DataFrame(index=date_list,columns=list(fund_list))
# REDEMSTATUS=pd.DataFrame(index=date_list,columns=list(fund_list))
# for symbol in fund_list:
#     temp_p=pd.DataFrame(columns=list([symbol]),index=date_list)
#     temp_r = pd.DataFrame(columns=list([symbol]),index=date_list)
#     for date in date_list:
#         print(date)
#         data=datasearch(symbol,"PURCHSTATUS,REDEMSTATUS","TradeDate="+date.strftime("%Y-%m-%d"))
#         temp_p.loc[date,symbol]=data.values[0][0]
#         temp_r.loc[date,symbol]=data.values[0][1]
#     PURCHSTATUS.loc[:,symbol]=temp_p
#     REDEMSTATUS.loc[:,symbol]=temp_r
# PURCHSTATUS.to_csv(r'D:\AQUMON\whitelist\PURCHSTATUS.csv',encoding='utf-8_sig')
# REDEMSTATUS.to_csv(r'D:\AQUMON\whitelist\REDEMSTATUS.csv',encoding='utf-8_sig')
#
#



#导出美股证券代码，简称和行业分布
# from choice_client import c
# import pandas as pd
#
# from retrying import retry
#
# @retry(stop_max_attempt_number=10)
# def datasearch1(symbol):
#     data = c.css(symbol, "GICS,GICSCODE,GICSENG,LOTSIZE,NAME","ClassiFication=1,TradeDate=2020-09-27")[symbol]
#     return data




# symbol_list_choice="A.N,AA.N,AACG.O,AACQ.O,AACQU.O,AAL.O,AAMC.A,AAME.O,AAN.N,AAOI.O,AAON.O,AAP.N,AAPL.O,AAT.N,AAU.A,AAWW.O,AAXN.O,AB.N,ABB.N,ABBV.N,ABC.N,ABCB.O,ABEO.O,ABEV.N,ABG.N,ABIO.O,ABM.N,ABMD.O,ABR.N,ABR_A.N,ABR_B.N,ABR_C.N,ABT.N,ABTX.O,ABUS.O,AC.N,ACA.N,ACAD.O,ACAM.O,ACAMU.O,ACB.N,ACBI.O,ACC.N,ACCD.O,ACCO.N,ACEL.N,ACER.O,ACET.O,ACEV.O,ACEVU.O,ACGL.O,ACGLO.O,ACGLP.O,ACH.N,ACHC.O,ACHV.O,ACI.N,ACIA.O,ACIU.O,ACIW.O,ACLS.O,ACM.N,ACMR.O,ACN.N,ACNB.O,ACND.N,ACND_U.N,ACOR.O,ACRE.N,ACRS.O,ACRX.O,ACST.O,ACTCU.O,ACTG.O,ACU.A,ACY.A,ADAP.O,ADBE.O,ADC.N,ADCT.N,ADES.O,ADI.O,ADIL.O,ADM.N,ADMA.O,ADMP.O,ADMS.O,ADNT.N,ADP.O,ADPT.O,ADRO.O,ADS.N,ADSK.O,ADSW.N,ADT.N,ADTN.O,ADTX.O,ADUS.O,ADVM.O,ADXN.O,ADXS.O,AE.A,AEE.N,AEG.N,AEGN.O,AEHR.O,AEIS.O,AEL.N,AEL_A.N,AEL_B.N,AEM.N,AEMD.O,AEO.N,AEP.N,AER.N,AERI.O,AES.N,AESE.O,AEY.O,AEYE.O,AEZS.O,AFG.N,AFI.N,AFIB.O,AFIN.O,AFINP.O,AFL.N,AFMD.O,AFYA.O,AG.N,AGBA.O,AGBAU.O,AGCO.N,AGE.A,AGEN.O,AGFS.O,AGI.N,AGIO.O,AGLE.O,AGM.N,AGM/A.N,AGM_C.N,AGM_D.N,AGM_E.N,AGM_F.N,AGMH.O,AGNC.O,AGNCM.O,AGNCN.O,AGNCO.O,AGNCP.O,AGO.N,AGR.N,AGRO.N,AGRX.O,AGS.N,AGTC.O,AGX.N,AGYS.O,AHACU.O,AHC.N,AHCO.O,AHH.N,AHH_A.N,AHL_C.N,AHL_D.N,AHL_E.N,AHPI.O,AHT.N,AHT_D.N,AHT_F.N,AHT_G.N,AHT_H.N,AHT_I.N,AI.N,AI_B.N,AI_C.N,AIG.N,AIG_A.N,AIH.O,AIHS.O,AIKI.O,AIM.A,AIMC.O,AIMT.O,AIN.N,AINC.A,AINV.O,AIR.N,AIRG.O,AIRI.A,AIRT.O,AIRTP.O,AIT.N,AIV.N,AIZ.N,AIZP.N,AJG.N,AJRD.N,AJX.N,AKAM.O,AKBA.O,AKCA.O,AKER.O,AKO_A.N,AKO_B.N,AKR.N,AKRO.O,AKTS.O,AKTX.O,AKU.O,AKUS.O,AL.N,AL_A.N,ALAC.O,ALACU.O,ALB.N,ALBO.O,ALC.N,ALCO.O,ALDX.O,ALE.N,ALEC.O,ALEX.N,ALG.N,ALGN.O,ALGT.O,ALIM.O,ALIN_A.N,ALIN_B.N,ALIN_E.N,ALJJ.O,ALK.N,ALKS.O,ALL.N,ALL_G.N,ALL_H.N,ALL_I.N,ALLE.N,ALLK.O,ALLO.O,ALLT.O,ALLY.N,ALLY_A.N,ALNA.O,ALNY.O,ALOT.O,ALP_Q.N,ALPN.O,ALRM.O,ALRN.O,ALRS.O,ALSK.O,ALSN.N,ALT.O,ALTA.O,ALTG.N,ALTM.O,ALTR.O,ALUS.N,ALUS_U.N,ALV.N,ALVR.O,ALX.N,ALXN.O,ALXO.O,ALYA.O,AM.N,AMAG.O,AMAL.O,AMAT.O,AMBA.O,AMBC.N,AMBO.A,AMC.N,AMCI.O,AMCIU.O,AMCR.N,AMCX.O,AMD.O,AME.N,AMED.O,AMEH.O,AMG.N,AMGN.O,AMH.N,AMH_D.N,AMH_E.N,AMH_F.N,AMH_G.N,AMH_H.N,AMHC.O,AMHCU.O,AMK.N,AMKR.O,AMN.N,AMNB.O,AMOT.O,AMOV.N,AMP.N,AMPE.A,AMPH.O,AMPY.N,AMRB.O,AMRC.N,AMRH.O,AMRK.O,AMRN.O,AMRS.O,AMRX.N,AMS.A,AMSC.O,AMSF.O,AMST.O,AMSWA.O,AMT.N,AMTB.O,AMTBB.O,AMTD.O,AMTI.O,AMTX.O,AMWD.O,AMWL.N,AMX.N,AMYT.O,AMZN.O,AN.N,ANAB.O,ANAT.O,ANCN.O,ANDA.O,ANDAU.O,ANDE.O,ANET.N,ANF.N,ANGI.O,ANGO.O,ANH.N,ANH_A.N,ANH_B.N,ANH_C.N,ANIK.O,ANIP.O,ANIX.O,ANNX.O,ANPC.O,ANSS.O,ANTE.O,ANTM.N,ANVS.A,ANY.O,AON.N,AONE_U.N,AOS.N,AOSL.O,AOUT.O,AP.N,APA.O,APAM.N,APD.N,APDN.O,APEI.O,APEN.O,APEX.O,APG.N,APH.N,APHA.O,API.O,APLE.N,APLS.O,APLT.O,APM.O,APO.N,APO_A.N,APO_B.N,APOG.O,APOP.O,APPF.O,APPN.O,APPS.O,APRE.O,APRN.N,APT.A,APTO.O,APTS.N,APTV.N,APTV_A.N,APTX.O,APVO.O,APWC.O,APXT.O,APXTU.O,APYX.O,AQB.O,AQMS.O,AQN.N,AQST.O,AQUA.N,AR.N,ARA.N,ARAV.O,ARAY.O,ARC.N,ARCB.O,ARCC.O,ARCE.O,ARCH.N,ARCO.N,ARCT.O,ARD.N,ARDS.O,ARDX.O,ARE.N,AREC.O,ARES.N,ARES_A.N,ARGO.N,ARGO_A.N,ARGX.O,ARI.N,ARKR.O,ARL.N,ARLO.N,ARLP.O,ARMK.N,ARMP.A,ARNA.O,ARNC.N,AROC.N,AROW.O,ARPO.O,ARQT.O,ARR.N,ARR_C.N,ARTL.O,ARTNA.O,ARTW.O,ARVN.O,ARW.N,ARWR.O,ARYA.O,ARYB.O,ARYBU.O,ASB.N,ASB_C.N,ASB_D.N,ASB_E.N,ASB_F.N,ASC.N,ASFI.O,ASGN.N,ASH.N,ASIX.N,ASLN.O,ASM.A,ASMB.O,ASML.O,ASND.O,ASPL_U.N,ASPN.N,ASPS.O,ASPU.O,ASR.N,ASRT.O,ASRV.O,ASTC.O,ASTE.O,ASUR.O,ASX.N,ASYS.O,AT.N,ATAX.O,ATCO.N,ATCO_D.N,ATCO_E.N,ATCO_G.N,ATCO_H.N,ATCO_I.N,ATCX.O,ATEC.O,ATEN.N,ATEX.O,ATGE.N,ATH.N,ATH_A.N,ATH_B.N,ATH_C.N,ATHA.O,ATHE.O,ATHM.N,ATHX.O,ATI.N,ATIF.O,ATKR.N,ATLC.O,ATLO.O,ATNI.O,ATNM.A,ATNX.O,ATO.N,ATOM.O,ATOS.O,ATR.N,ATRA.O,ATRC.O,ATRI.O,ATRO.O,ATRS.O,ATSG.O,ATTO.N,ATUS.N,ATV.N,ATVI.O,ATXI.O,AU.N,AUB.O,AUBAP.O,AUBN.O,AUDC.O,AUG.A,AUMN.A,AUPH.O,AUTL.O,AUTO.O,AUVI.O,AUY.N,AVA.N,AVAL.N,AVAV.O,AVB.N,AVCO.O,AVCT.O,AVD.N,AVDL.O,AVEO.O,AVGO.O,AVGOP.O,AVGR.O,AVID.O,AVLR.N,AVNS.N,AVNT.N,AVNW.O,AVRO.O,AVT.O,AVTR.N,AVTR_A.N,AVXL.O,AVY.N,AVYA.N,AWH.O,AWI.N,AWK.N,AWR.N,AWRE.O,AWX.A,AX.N,AXAS.O,AXDX.O,AXGN.O,AXGT.O,AXL.N,AXLA.O,AXNX.O,AXP.N,AXR.N,AXS.N,AXS_E.N,AXSM.O,AXTA.N,AXTI.O,AXU.A,AY.O,AYI.N,AYLA.O,AYRO.O,AYTU.O,AYX.N,AZEK.N,AZN.O,AZO.N,AZPN.O,AZRE.N,AZRX.O,AZUL.N,AZZ.N,B.N,BA.N,BABA.N,BAC.N,BAC_A.N,BAC_B.N,BAC_C.N,BAC_E.N,BAC_K.N,BAC_L.N,BAC_M.N,BAC_N.N,BAH.N,BAK.N,BAM.N,BANC.N,BANC_D.N,BANC_E.N,BAND.O,BANF.O,BANFP.O,BANR.O,BANX.O,BAP.N,BASI.O,BATL.A,BATRA.O,BATRK.O,BAX.N,BB.N,BBAR.N,BBBY.O,BBCP.O,BBD.N,BBDC.N,BBDO.N,BBGI.O,BBI.O,BBIO.O,BBL.N,BBQ.O,BBSI.O,BBU.N,BBVA.N,BBW.N,BBX.N,BBY.N,BC.N,BCBP.O,BCC.N,BCDA.O,BCE.N,BCEI.N,BCEL.O,BCH.N,BCLI.O,BCML.O,BCO.N,BCOR.O,BCOV.O,BCOW.O,BCPC.O,BCRX.O,BCS.N,BCSF.N,BCTG.O,BCV_A.A,BCYC.O,BDC.N,BDGE.O,BDL.A,BDN.N,BDR.A,BDSI.O,BDTX.O,BDX.N,BDXB.N,BE.N,BEAM.O,BEAT.O,BECN.O,BEDU.N,BEEM.O,BEKE.N,BELFA.O,BELFB.O,BEN.N,BEP.N,BEP_A.N,BEPC.N,BERY.N,BEST.N,BF_A.N,BF_B.N,BFAM.N,BFC.O,BFIN.O,BFRA.O,BFS.N,BFS_D.N,BFS_E.N,BFST.O,BFT_U.N,BG.N,BGCP.O,BGFV.O,BGI.A,BGNE.O,BGS.N,BGSF.N,BH.N,BH_A.N,BHAT.O,BHB.A,BHC.N,BHE.N,BHF.O,BHFAO.O,BHFAP.O,BHLB.N,BHP.N,BHR.N,BHR_B.N,BHR_D.N,BHTG.O,BHVN.N,BIDU.O,BIG.N,BIGC.O,BIIB.O,BILI.O,BILL.N,BIMI.O,BIO.N,BIO_B.N,BIOC.O,BIOL.O,BIOX.A,BIP.N,BIP_A.N,BIPC.N,BITA.N,BIVI.O,BJ.N,BJRI.O,BK.N,BK_C.N,BKCC.O,BKD.N,BKE.N,BKEP.O,BKEPP.O,BKH.N,BKI.N,BKNG.O,BKR.N,BKSC.O,BKTI.A,BKU.N,BKYI.O,BL.O,BLBD.O,BLCM.O,BLCT.O,BLD.N,BLDP.O,BLDR.O,BLFS.O,BLI.O,BLIN.O,BLK.N,BLKB.O,BLL.N,BLMN.O,BLNK.O,BLPH.O,BLRX.O,BLU.O,BLUE.O,BLX.N,BMA.N,BMCH.O,BMI.N,BML_G.N,BML_H.N,BML_J.N,BML_L.N,BMO.N,BMRA.O,BMRC.O,BMRG.N,BMRG_U.N,BMRN.O,BMTC.O,BMY.N,BNED.N,BNFT.O,BNGO.O,BNL.N,BNR.O,BNS.N,BNSO.O,BNTC.O,BNTX.O,BOCH.O,BOH.N,BOKF.O,BOMN.O,BOOM.O,BOOT.N,BORR.N,BOSC.O,BOTJ.O,BOWXU.O,BOX.N,BOXL.O,BP.N,BPFH.O,BPMC.O,BPMP.N,BPOP.O,BPRN.O,BPT.N,BPTH.O,BPY.O,BPYPN.O,BPYPO.O,BPYPP.O,BPYU.O,BPYUP.O,BR.N,BRBR.N,BRBS.A,BRC.N,BREW.O,BRFS.N,BRG.A,BRG_A.A,BRG_C.A,BRG_D.A,BRID.O,BRK_A.N,BRK_B.N,BRKL.O,BRKR.O,BRKS.O,BRLI.O,BRLIU.O,BRMK.N,BRN.A,BRO.N,BROG.O,BRP.O,BRPA.O,BRPAU.O,BRQS.O,BRT.N,BRX.N,BRY.O,BSAC.N,BSBK.O,BSBR.N,BSET.O,BSGM.O,BSIG.N,BSM.N,BSMX.N,BSN_U.N,BSQR.O,BSRR.O,BSTC.O,BSVN.O,BSX.N,BSX_A.N,BSY.O,BTAI.O,BTAQU.O,BTBT.O,BTE.N,BTG.A,BTI.N,BTN.A,BTU.N,BUD.N,BURL.N,BUSE.O,BV.N,BVN.N,BVXV.O,BW.N,BWA.N,BWAY.O,BWB.O,BWEN.O,BWFG.O,BWL_A.A,BWMX.O,BWXT.N,BX.N,BXC.N,BXG.N,BXMT.N,BXP.N,BXP_B.N,BXRX.O,BXS.N,BXS_A.N,BY.N,BYD.N,BYFC.O,BYND.O,BYSI.O,BZH.N,BZUN.O,C.N,C_J.N,C_K.N,C_S.N,CAAP.N,CAAS.O,CABA.O,CABO.N,CAC.O,CACC.O,CACI.N,CADE.N,CAE.N,CAG.N,CAH.N,CAI.N,CAI_A.N,CAI_B.N,CAJ.N,CAKE.O,CAL.N,CALA.O,CALB.O,CALM.O,CALT.O,CALX.N,CAMP.O,CAMT.O,CAN.O,CANF.A,CANG.N,CAPAU.O,CAPL.N,CAPR.O,CAR.O,CARA.O,CARE.O,CARG.O,CARR.N,CARS.N,CARV.O,CASA.O,CASH.O,CASI.O,CASS.O,CASY.O,CAT.N,CATB.O,CATC.O,CATM.O,CATO.N,CATY.O,CB.N,CBAN.O,CBAT.O,CBAY.O,CBB.N,CBB_B.N,CBD.N,CBFV.O,CBIO.O,CBL.N,CBL_D.N,CBL_E.N,CBLI.O,CBMB.O,CBMG.O,CBNK.O,CBOE.A,CBPO.O,CBRE.N,CBRL.O,CBSH.O,CBT.N,CBTX.O,CBU.N,CBZ.N,CC.N,CCAC.N,CCAC_U.N,CCAP.O,CCB.O,CCBG.O,CCC.N,CCCL.O,CCEP.N,CCF.A,CCI.N,CCIV.N,CCIV_U.N,CCJ.N,CCK.N,CCL.N,CCLP.O,CCM.N,CCMP.O,CCNC.O,CCNE.O,CCNEP.O,CCO.N,CCOI.O,CCR.N,CCRC.O,CCRN.O,CCS.N,CCU.N,CCX.N,CCX_U.N,CCXI.O,CCXX.N,CCXX_U.N,CDAY.N,CDE.N,CDEV.O,CDK.O,CDLX.O,CDMO.O,CDMOP.O,CDNA.O,CDNS.O,CDOR.A,CDR.N,CDR_B.N,CDR_C.N,CDTX.O,CDW.O,CDXC.O,CDXS.O,CDZI.O,CE.N,CEA.N,CECE.O,CEF.A,CEI.A,CEIX.N,CEL.N,CELC.O,CELH.O,CELP.N,CEMI.O,CENT.O,CENTA.O,CENX.O,CEO.N,CEPU.N,CEQP.N,CEQP_.N,CERC.O,CERN.O,CERS.O,CETV.O,CETX.O,CETXP.O,CEVA.O,CF.N,CFB.O,CFBI.O,CFBK.O,CFFA.O,CFFAU.O,CFFI.O,CFFN.O,CFG.N,CFG_D.N,CFG_E.N,CFIIU.O,CFMS.O,CFR.N,CFRX.O,CFX.N,CG.O,CGA.N,CGBD.O,CGC.N,CGEN.O,CGIX.O,CGNX.O,CGRO.O,CGROU.O,CHA.N,CHAQ.A,CHAQ_U.A,CHCI.O,CHCO.O,CHCT.N,CHD.N,CHDN.O,CHE.N,CHEF.O,CHEK.O,CHFS.O,CHGG.N,CHH.N,CHKP.O,CHL.N,CHMA.O,CHMG.O,CHMI.N,CHMI_A.N,CHMI_B.N,CHNG.O,CHNR.O,CHPM.O,CHPMU.O,CHRA.N,CHRS.O,CHRW.O,CHS.N,CHSCL.O,CHSCM.O,CHSCN.O,CHSCO.O,CHSCP.O,CHT.N,CHTR.O,CHU.N,CHUY.O,CHWY.N,CHX.N,CI.N,CIA.N,CIB.N,CIDM.O,CIEN.N,CIG.N,CIG_C.N,CIGI.O,CIH.O,CIIC.O,CIICU.O,CIM.N,CIM_A.N,CIM_B.N,CIM_C.N,CIM_D.N,CINF.O,CINR.N,CIO.N,CIO_A.N,CIR.N,CIT.N,CIT_B.N,CIVB.O,CIX.A,CIZN.O,CJJD.O,CKH.N,CKPT.O,CKX.A,CL.N,CLA_U.N,CLAR.O,CLB.N,CLBK.O,CLBS.O,CLCT.O,CLDB.O,CLDR.N,CLDT.N,CLDX.O,CLEU.O,CLF.N,CLFD.O,CLGN.O,CLGX.N,CLH.N,CLI.N,CLIR.O,CLLS.O,CLMT.O,CLNC.N,CLNE.O,CLNY.N,CLNY_G.N,CLNY_H.N,CLNY_I.N,CLNY_J.N,CLPR.N,CLPS.O,CLPT.O,CLR.N,CLRB.O,CLRO.O,CLS.N,CLSD.O,CLSK.O,CLSN.O,CLVS.O,CLW.N,CLWT.O,CLX.N,CLXT.O,CM.N,CMA.N,CMBM.O,CMC.N,CMCL.A,CMCM.N,CMCO.O,CMCSA.O,CMCT.O,CMCTP.O,CMD.N,CME.O,CMG.N,CMI.N,CMLFU.O,CMLS.O,CMO.N,CMO_E.N,CMP.N,CMPI.O,CMPR.O,CMPS.O,CMRE.N,CMRE_B.N,CMRE_C.N,CMRE_D.N,CMRE_E.N,CMRX.O,CMS.N,CMS_B.N,CMT.A,CMTL.O,CNA.N,CNBKA.O,CNC.N,CNCE.O,CNDT.O,CNET.O,CNF.N,CNFR.O,CNHI.N,CNI.N,CNK.N,CNMD.N,CNNB.O,CNNE.N,CNO.N,CNOB.O,CNP.N,CNP_B.N,CNQ.N,CNR.N,CNS.N,CNSL.O,CNSP.O,CNST.O,CNTG.O,CNTY.O,CNX.N,CNXM.N,CNXN.O,CO.N,COCP.O,CODA.O,CODI.N,CODI_A.N,CODI_B.N,CODI_C.N,CODX.O,COE.N,COF.N,COF_F.N,COF_G.N,COF_H.N,COF_I.N,COF_J.N,COF_K.N,COFS.O,COG.N,COHN.A,COHR.O,COHU.O,COKE.O,COLB.O,COLD.N,COLL.O,COLM.O,COMM.O,CONE.O,CONN.O,COO.N,COOP.O,COP.N,COR.N,CORE.O,CORR.N,CORR_A.N,CORT.O,COST.O,COTY.N,COUP.O,COWN.O,CP.N,CPA.N,CPAA.O,CPAAU.O,CPAC.N,CPAH.O,CPB.N,CPE.N,CPF.N,CPG.N,CPHC.O,CPHI.A,CPIX.O,CPK.N,CPLG.N,CPLP.O,CPRI.N,CPRT.O,CPRX.O,CPS.N,CPSH.O,CPSI.O,CPSR.N,CPSR_U.N,CPSS.O,CPST.O,CPT.N,CPTA.O,CQP.A,CR.N,CRAI.O,CRBP.O,CRD_A.N,CRD_B.N,CRDF.O,CREE.O,CREG.O,CRESY.O,CREX.O,CRH.N,CRHC_U.N,CRHM.A,CRI.N,CRIS.O,CRK.N,CRL.N,CRM.N,CRMD.A,CRMT.O,CRNC.O,CRNT.O,CRNX.O,CRON.O,CROX.O,CRS.N,CRSA.O,CRSAU.O,CRSP.O,CRSR.O,CRT.N,CRTD.O,CRTO.O,CRTX.O,CRUS.O,CRVL.O,CRVS.O,CRWD.O,CRWS.O,CRY.N,CS.N,CSBR.O,CSCO.O,CSGP.O,CSGS.O,CSII.O,CSIQ.O,CSL.N,CSLT.N,CSOD.O,CSPI.O,CSPR.N,CSSE.O,CSSEP.O,CSTE.O,CSTL.O,CSTM.N,CSTR.O,CSU.N,CSV.N,CSWC.O,CSWI.O,CSX.O,CTA_A.N,CTA_B.N,CTAS.O,CTB.N,CTBI.O,CTEK.A,CTG.O,CTHR.O,CTIB.O,CTIC.O,CTK.N,CTLT.N,CTMX.O,CTO.A,CTRA.N,CTRE.O,CTRM.O,CTRN.O,CTS.N,CTSH.O,CTSO.O,CTT.N,CTVA.N,CTXR.O,CTXS.O,CUB.N,CUBE.N,CUBI.N,CUBI_C.N,CUBI_D.N,CUBI_E.N,CUBI_F.N,CUE.O,CUK.N,CULP.N,CURO.N,CUTR.O,CUZ.N,CVA.N,CVAC.O,CVBF.O,CVCO.O,CVCY.O,CVE.N,CVEO.N,CVET.O,CVGI.O,CVGW.O,CVI.N,CVLG.O,CVLT.O,CVLY.O,CVM.A,CVNA.N,CVR.A,CVS.N,CVU.A,CVV.O,CVX.N,CW.N,CWBC.O,CWBR.O,CWCO.O,CWEN.N,CWEN_A.N,CWH.N,CWK.N,CWST.O,CWT.N,CX.N,CXDC.O,CXDO.O,CXO.N,CXP.N,CXW.N,CYAD.O,CYAN.O,CYBE.O,CYBR.O,CYCC.O,CYCCP.O,CYCN.O,CYD.N,CYH.N,CYRN.O,CYRX.O,CYTK.O,CZNC.O,CZR.O,CZWI.O,CZZ.N,D.N,DAC.N,DADA.O,DAIO.O,DAKT.O,DAL.N,DAN.N,DAO.N,DAR.N,DARE.O,DAVA.N,DB.N,DBD.N,DBI.N,DBVT.O,DBX.O,DCI.N,DCO.N,DCOM.O,DCOMP.O,DCP.N,DCP_B.N,DCP_C.N,DCPH.O,DCT.O,DCTH.O,DD.N,DDD.N,DDOG.O,DDS.N,DDT.N,DE.N,DEA.N,DECK.N,DEH.N,DEH_U.N,DEI.N,DELL.N,DEN.N,DENN.O,DEO.N,DESP.N,DFFN.O,DFHT.O,DFHTU.O,DFIN.N,DFNS.N,DFNS_U.N,DFPH.O,DFPHU.O,DFS.N,DG.N,DGICA.O,DGICB.O,DGII.O,DGLY.O,DGNR_U.N,DGX.N,DHC.O,DHI.N,DHIL.O,DHR.N,DHR_A.N,DHR_B.N,DHT.N,DHX.N,DIN.N,DIOD.O,DIS.N,DISCA.O,DISCB.O,DISCK.O,DISH.O,DIT.A,DJCO.O,DK.N,DKL.N,DKNG.O,DKS.N,DL.N,DLA.A,DLB.N,DLHC.O,DLNG.N,DLNG_A.N,DLNG_B.N,DLPH.N,DLPN.O,DLR.N,DLR_C.N,DLR_G.N,DLR_J.N,DLR_K.N,DLR_L.N,DLTH.O,DLTR.O,DLX.N,DLY.N,DMAC.O,DMLP.O,DMRC.O,DMS.N,DMTK.O,DMYD_U.N,DMYT.N,DMYT_U.N,DNB.N,DNK.N,DNKN.O,DNLI.O,DNN.A,DNOW.N,DOC.N,DOCU.O,DOGZ.O,DOMO.O,DOOO.O,DOOR.N,DORM.O,DOV.N,DOW.N,DOX.O,DOYU.O,DPHC.O,DPHCU.O,DPW.A,DPZ.N,DQ.N,DRAD.O,DRADP.O,DRD.N,DRE.N,DRH.N,DRH_A.N,DRI.N,DRIO.O,DRNA.O,DRQ.N,DRRX.O,DRTT.O,DS.N,DS_B.N,DS_C.N,DS_D.N,DSGX.O,DSKE.O,DSPG.O,DSS.A,DSSI.N,DSWL.O,DSX.N,DSX_B.N,DT.N,DTE.N,DTEA.O,DTIL.O,DTLA_.N,DTSS.O,DUK.N,DUK_A.N,DUO.O,DUOT.O,DVA.N,DVAX.O,DVD.N,DVN.N,DWSN.O,DX.N,DX_B.N,DX_C.N,DXC.N,DXCM.O,DXF.A,DXLG.O,DXPE.O,DXR.A,DXYN.O,DY.N,DYAI.O,DYN.O,DYNT.O,DZSI.O,E.N,EA.O,EAF.N,EARN.N,EARS.O,EAST.O,EAT.N,EB.N,EBAY.O,EBF.N,EBIX.O,EBMT.O,EBON.O,EBR.N,EBR_B.N,EBS.N,EBSB.O,EBTC.O,EC.N,ECCB.N,ECF_A.A,ECHO.O,ECL.N,ECOL.O,ECOM.N,ECOR.O,ECPG.O,ED.N,EDAP.O,EDIT.O,EDN.N,EDNT.O,EDRY.O,EDSA.O,EDTK.O,EDU.N,EDUC.O,EEFT.O,EEX.N,EFC.N,EFC_A.N,EFOI.O,EFSC.O,EFX.N,EGAN.O,EGBN.O,EGHT.N,EGLE.O,EGO.N,EGOV.O,EGP.N,EGRX.O,EGY.N,EH.O,EHC.N,EHTH.O,EIC.N,EIDX.O,EIG.N,EIGI.O,EIGR.O,EIX.N,EKSO.O,EL.N,ELA.A,ELAN.N,ELF.N,ELLO.A,ELMD.A,ELOX.O,ELP.N,ELS.N,ELSE.O,ELTK.O,ELVT.N,ELY.N,EMAN.A,EMCF.O,EME.N,EMKR.O,EML.O,EMN.N,EMR.N,EMX.A,ENB.N,ENBL.N,ENDP.O,ENG.O,ENIA.N,ENIC.N,ENLC.N,ENLV.O,ENOB.O,ENPC_U.N,ENPH.O,ENR.N,ENR_A.N,ENS.N,ENSG.O,ENSV.A,ENTA.O,ENTG.O,ENTX.O,ENV.N,ENVA.N,ENZ.N,EOG.N,EOLS.O,EP_C.N,EPAC.N,EPAM.N,EPAY.O,EPC.N,EPD.N,EPIX.O,EPM.A,EPR.N,EPR_C.N,EPR_E.N,EPR_G.N,EPRT.N,EPSN.O,EPZM.O,EQ.O,EQBK.O,EQC.N,EQC_D.N,EQD_U.N,EQH.N,EQH_A.N,EQIX.O,EQNR.N,EQR.N,EQS.N,EQT.N,EQX.A,ERES.O,ERESU.O,ERF.N,ERIC.O,ERIE.O,ERII.O,ERJ.N,ERYP.O,ES.N,ESBA.A,ESBK.O,ESCA.O,ESE.N,ESEA.O,ESGC.N,ESGR.O,ESGRO.O,ESGRP.O,ESI.N,ESLT.O,ESNT.N,ESP.A,ESPR.O,ESQ.O,ESRT.N,ESS.N,ESSA.O,ESSC.O,ESSCU.O,ESTA.O,ESTC.N,ESTE.N,ESXB.O,ET.N,ETAC.O,ETACU.O,ETFC.O,ETH.N,ETI_.N,ETM.N,ETN.N,ETNB.O,ETON.O,ETP_C.N,ETP_D.N,ETP_E.N,ETR.N,ETRN.N,ETSY.O,ETTX.O,EURN.N,EV.N,EVA.N,EVBG.O,EVBN.A,EVC.N,EVER.O,EVFM.O,EVGN.O,EVH.N,EVI.A,EVK.O,EVLO.O,EVOK.O,EVOL.O,EVOP.O,EVR.N,EVRG.N,EVRI.N,EVTC.N,EW.N,EWBC.O,EXAS.O,EXC.O,EXEL.O,EXFO.O,EXK.N,EXLS.O,EXN.A,EXP.N,EXPC.O,EXPCU.O,EXPD.O,EXPE.O,EXPI.O,EXPO.O,EXPR.N,EXR.N,EXTN.N,EXTR.O,EYE.O,EYEG.O,EYEN.O,EYES.O,EYPT.O,EZPW.O,F.N,FAF.N,FAII_U.N,FAMI.O,FANG.O,FANH.O,FARM.O,FARO.O,FAST.O,FAT.O,FATBP.O,FATE.O,FB.O,FBC.N,FBHS.N,FBIO.O,FBIOP.O,FBIZ.O,FBK.N,FBM.N,FBMS.O,FBNC.O,FBP.N,FBRX.O,FBSS.O,FC.N,FCACU.O,FCAP.O,FCAU.N,FCBC.O,FCBP.O,FCCO.O,FCCY.O,FCEL.O,FCF.N,FCFS.O,FCN.N,FCNCA.O,FCNCP.O,FCPT.N,FCRD.O,FCX.N,FDBC.O,FDP.N,FDS.N,FDUS.O,FDX.N,FE.N,FEAC.N,FEAC_U.N,FEDU.N,FEIM.O,FELE.O,FENC.O,FENG.N,FET.N,FEYE.O,FF.N,FFBC.O,FFBW.O,FFG.N,FFHL.O,FFIC.O,FFIN.O,FFIV.O,FFNW.O,FFWM.O,FGBI.O,FGEN.O,FHB.O,FHI.N,FHN.N,FHN_A.N,FHN_B.N,FHN_C.N,FHN_D.N,FHN_E.N,FI.N,FIBK.O,FICO.N,FIII.O,FIIIU.O,FINV.N,FIS.N,FISI.O,FISK.A,FISV.O,FIT.N,FITB.O,FITBI.O,FITBO.O,FITBP.O,FIVE.O,FIVN.O,FIX.N,FIXX.O,FIZZ.O,FL.N,FLDM.O,FLEX.O,FLGT.O,FLIC.O,FLIR.O,FLL.O,FLMN.O,FLNG.N,FLNT.O,FLO.N,FLOW.N,FLR.N,FLS.N,FLT.N,FLUX.O,FLWS.O,FLXN.O,FLXS.O,FLY.N,FMAO.O,FMBH.O,FMBI.O,FMBIO.O,FMBIP.O,FMC.N,FMCI.O,FMCIU.O,FMNB.O,FMS.N,FMTX.O,FMX.N,FN.N,FNB.N,FNB_E.N,FNCB.O,FND.N,FNF.N,FNHC.O,FNKO.O,FNLC.O,FNV.N,FNWB.O,FOCS.O,FOE.N,FOLD.O,FONR.O,FOR.N,FORD.O,FORK.O,FORM.O,FORR.O,FORTY.O,FOSL.O,FOUR.N,FOX.O,FOXA.O,FOXF.O,FPAY.O,FPH.N,FPI.N,FPI_B.N,FPRX.O,FR.N,FRAF.O,FRAN.O,FRBA.O,FRBK.O,FRC.N,FRC_F.N,FRC_G.N,FRC_H.N,FRC_I.N,FRC_J.N,FRC_K.N,FRD.A,FREE.O,FREQ.O,FRG.O,FRGAP.O,FRGI.O,FRHC.O,FRLN.O,FRME.O,FRO.N,FROG.O,FRPH.O,FRPT.O,FRSX.O,FRT.N,FRT_C.N,FRTA.O,FSBW.O,FSDC.O,FSEA.O,FSFG.O,FSI.A,FSK.N,FSKR.N,FSLR.O,FSLY.N,FSM.N,FSP.A,FSRV.O,FSRVU.O,FSS.N,FST_U.N,FSTR.O,FSV.O,FTAC.O,FTACU.O,FTAI.N,FTAI_A.N,FTAI_B.N,FTCH.N,FTDR.O,FTEK.O,FTFT.O,FTHM.O,FTI.N,FTIVU.O,FTK.N,FTNT.O,FTOCU.O,FTS.N,FTSI.A,FTV.N,FTV_A.N,FTV_V.N,FUL.N,FULC.O,FULT.O,FUN.N,FUNC.O,FUSB.O,FUSE.N,FUSE_U.N,FUSN.O,FUTU.O,FUV.O,FVAC.N,FVAC_U.N,FVCB.O,FVE.O,FVRR.N,FWONA.O,FWONK.O,FWP.O,FWRD.O,FXNC.O,G.N,GAB_G.N,GAB_H.N,GAB_J.N,GAB_K.N,GABC.O,GAIA.O,GAIN.O,GAINL.O,GAINM.O,GALT.O,GAM_B.N,GAN.O,GARS.O,GASS.O,GATX.N,GAU.A,GB.N,GBCI.O,GBDC.O,GBIO.O,GBL.N,GBLI.O,GBR.A,GBT.O,GBX.N,GCBC.O,GCI.N,GCO.N,GCP.N,GD.N,GDDY.N,GDEN.O,GDL_C.N,GDOT.N,GDP.A,GDRX.O,GDS.O,GDV_G.N,GDV_H.N,GDYN.O,GE.N,GEC.O,GECC.O,GEF.N,GEF_B.N,GEL.N,GEN.N,GENC.O,GENE.O,GEO.N,GEOS.O,GERN.O,GES.N,GEVO.O,GFED.O,GFF.N,GFI.N,GFL.N,GFN.O,GFNCP.O,GGAL.O,GGB.N,GGG.N,GGN_B.A,GGT_E.N,GGT_G.N,GGZ_A.N,GH.O,GHC.N,GHG.N,GHIV.O,GHIVU.O,GHL.N,GHM.N,GHSI.O,GIB.N,GIFI.O,GIGM.O,GIII.O,GIK.N,GIK_U.N,GIL.N,GILD.O,GILT.O,GIS.N,GIX.N,GIX_U.N,GKOS.N,GL.N,GLAD.O,GLBS.O,GLBZ.O,GLDD.O,GLEO.N,GLEO_U.N,GLG.O,GLIBA.O,GLIBP.O,GLMD.O,GLNG.O,GLOB.N,GLOG.N,GLOG_A.N,GLOP.N,GLOP_A.N,GLOP_B.N,GLOP_C.N,GLP.N,GLP_A.N,GLPG.O,GLPI.O,GLRE.O,GLT.N,GLU_A.A,GLU_B.A,GLUU.O,GLW.N,GLYC.O,GM.N,GMAB.O,GMBL.O,GMDA.O,GME.N,GMED.N,GMHI.O,GMHIU.O,GMLP.O,GMLPP.O,GMO.A,GMRE.N,GMRE_A.N,GMS.N,GNCA.O,GNE.N,GNE_A.N,GNFT.O,GNK.N,GNL.N,GNL_A.N,GNL_B.N,GNLN.O,GNMK.O,GNPX.O,GNRC.N,GNRS.O,GNRSU.O,GNSS.O,GNT_A.N,GNTX.O,GNTY.O,GNUS.O,GNW.N,GO.O,GOAC.N,GOAC_U.N,GOCO.O,GOED.A,GOGL.O,GOGO.O,GOL.N,GOLD.N,GOLF.N,GOOD.O,GOODM.O,GOODN.O,GOOG.O,GOOGL.O,GOOS.N,GORO.A,GOSS.O,GOVX.O,GP.O,GPC.N,GPI.N,GPK.N,GPL.A,GPMT.N,GPN.N,GPOR.O,GPP.O,GPRE.O,GPRK.N,GPRO.O,GPS.N,GPX.N,GRA.N,GRAF.N,GRAF_U.N,GRAM.N,GRAY.O,GRBK.O,GRC.N,GRCY.O,GRCYU.O,GRFS.O,GRIF.O,GRIL.O,GRIN.O,GRMN.O,GRNQ.O,GRNV.O,GRNVU.O,GROW.O,GRP_U.N,GRPN.O,GRSVU.O,GRTS.O,GRTX.O,GRUB.N,GRVY.O,GRWG.O,GRX_B.N,GS.N,GS_A.N,GS_C.N,GS_D.N,GS_J.N,GS_K.N,GS_N.N,GSAH.N,GSAH_U.N,GSAT.A,GSBC.O,GSBD.N,GSH.N,GSHD.O,GSIT.O,GSK.N,GSKY.O,GSL.N,GSL_B.N,GSM.O,GSMG.O,GSS.A,GSUM.O,GSV.A,GSX.N,GT.O,GTE.A,GTEC.O,GTES.N,GTH.O,GTHX.O,GTIM.O,GTLS.O,GTN.N,GTN_A.N,GTS.N,GTT.N,GTY.N,GTYH.O,GURE.O,GUT_A.N,GUT_C.N,GV.A,GVA.N,GVP.O,GWB.N,GWGH.O,GWPH.O,GWRE.N,GWRS.O,GWW.N,GXGX.O,GXGXU.O,GYRO.O,H.N,HA.O,HAE.N,HAFC.O,HAIN.O,HAL.N,HALL.O,HALO.O,HAPP.O,HARP.O,HAS.O,HASI.N,HAYN.O,HBAN.O,HBANN.O,HBANO.O,HBB.N,HBCP.O,HBI.N,HBIO.O,HBM.N,HBMD.O,HBNC.O,HBP.O,HBT.O,HCA.N,HCAC.O,HCACU.O,HCAP.O,HCAT.O,HCC.N,HCCH.O,HCCHU.O,HCCI.O,HCCO.O,HCCOU.O,HCDI.O,HCFT.N,HCHC.N,HCI.N,HCKT.O,HCM.O,HCSG.O,HD.N,HDB.N,HDS.O,HDSN.O,HE.N,HEAR.O,HEBT.O,HEC.O,HECCU.O,HEES.O,HEI.N,HEI_A.N,HELE.O,HEP.N,HEPA.O,HES.N,HESM.N,HEXO.N,HFBL.O,HFC.N,HFFG.O,HFRO_A.N,HFWA.O,HGBL.O,HGEN.O,HGSH.O,HGV.N,HHC.N,HHR.O,HHT.O,HI.N,HIBB.O,HIFS.O,HIG.N,HIG_G.N,HIHO.O,HII.N,HIL.N,HIMX.O,HIW.N,HJLI.O,HKIB.N,HL.N,HL_B.N,HLF.N,HLG.O,HLI.N,HLIO.O,HLIT.O,HLM_.A,HLNE.O,HLT.N,HLX.N,HMC.N,HMG.A,HMHC.O,HMI.N,HMLP.N,HMLP_A.N,HMN.N,HMNF.O,HMST.O,HMSY.O,HMTV.O,HMY.N,HNGR.N,HNI.N,HNNA.O,HNP.N,HNRG.O,HOFT.O,HOFV.O,HOG.N,HOL.O,HOLI.O,HOLUU.O,HOLX.O,HOMB.O,HOME.N,HON.N,HONE.O,HOOK.O,HOPE.O,HOTH.O,HOV.N,HOVNP.O,HP.N,HPE.N,HPK.O,HPP.N,HPQ.N,HPR.N,HPX.N,HPX_U.N,HQI.O,HQY.O,HR.N,HRB.N,HRC.N,HRI.N,HRL.N,HRMY.O,HROW.O,HRTG.N,HRTX.O,HRZN.O,HSAQ.O,HSBC.N,HSBC_A.N,HSC.N,HSDT.O,HSIC.O,HSII.O,HSKA.O,HSON.O,HST.N,HSTM.O,HSTO.O,HSY.N,HT.N,HT_C.N,HT_D.N,HT_E.N,HTA.N,HTBI.O,HTBK.O,HTBX.O,HTGC.N,HTGM.O,HTH.N,HTHT.O,HTIA.O,HTLD.O,HTLF.O,HTLFP.O,HTZ.N,HUBB.N,HUBG.O,HUBS.N,HUD.N,HUGE.O,HUIZ.O,HUM.N,HUN.N,HURC.O,HURN.O,HUSA.A,HUSN.O,HUYA.N,HVBC.O,HVT.N,HVT_A.N,HWBK.O,HWC.O,HWCC.O,HWKN.O,HWM.N,HWM_.A,HX.O,HXL.N,HY.N,HYAC.O,HYACU.O,HYGO.O,HYMC.O,HYRE.O,HZAC_U.N,HZN.N,HZNP.O,HZO.N,IAA.N,IAC.O,IAG.N,IART.O,IBA.N,IBCP.O,IBEX.O,IBIO.A,IBKR.O,IBM.N,IBN.N,IBOC.O,IBP.N,IBTX.O,ICAD.O,ICBK.O,ICCC.O,ICCH.O,ICD.N,ICE.N,ICFI.O,ICHR.O,ICL.N,ICLK.O,ICLR.O,ICMB.O,ICON.O,ICPT.O,ICUI.O,IDA.N,IDCC.O,IDEX.O,IDN.O,IDRA.O,IDT.N,IDXG.O,IDXX.O,IDYA.O,IEA.O,IEC.O,IEP.O,IESC.O,IEX.N,IFF.N,IFMK.O,IFRX.O,IFS.N,IGC.A,IGIC.O,IGMS.O,IGT.N,IHC.N,IHG.N,IHRT.O,IHT.A,III.O,IIIN.O,IIIV.O,IIN.O,IIPR.N,IIPR_A.N,IIVI.O,IIVIP.O,IKNX.O,ILMN.O,ILPT.O,IMAB.O,IMAC.O,IMAX.N,IMBI.O,IMGN.O,IMH.A,IMKTA.O,IMMP.O,IMMR.O,IMMU.O,IMO.A,IMOS.O,IMRA.O,IMRN.O,IMTE.O,IMTX.O,IMUX.O,IMV.O,IMVT.O,IMXI.O,INAQU.O,INBK.O,INBX.O,INCY.O,INDB.O,INDO.A,INFI.O,INFN.O,INFO.N,INFU.A,INFY.N,ING.N,INGN.O,INGR.N,INMB.O,INMD.O,INN.N,INN_D.N,INN_E.N,INO.O,INOD.O,INOV.O,INPX.O,INS.A,INSE.O,INSG.O,INSM.O,INSP.N,INSU.O,INSUU.O,INSW.N,INT.N,INTC.O,INTG.O,INTT.A,INTU.O,INUV.A,INVA.O,INVE.O,INVH.N,INWK.O,INZY.O,IO.N,IONS.O,IOR.A,IOSP.O,IOVA.O,IP.N,IPAR.O,IPDN.O,IPG.N,IPGP.O,IPHA.O,IPHI.N,IPI.N,IPLDP.O,IPOB.N,IPOB_U.N,IPOC.N,IPOC_U.N,IPV.N,IPV_U.N,IPWR.O,IQ.O,IQV.N,IR.N,IRBT.O,IRCP.O,IRDM.O,IRET.N,IRET_C.N,IRIX.O,IRM.N,IRMD.O,IROQ.O,IRS.N,IRT.N,IRTC.O,IRWD.O,ISBC.O,ISDR.A,ISEE.O,ISIG.O,ISNS.O,ISR.A,ISRG.O,ISSC.O,ISTR.O,IT.N,ITACU.O,ITCB.N,ITCI.O,ITGR.N,ITI.O,ITIC.O,ITMR.O,ITOS.O,ITP.A,ITRG.A,ITRI.O,ITRM.O,ITRN.O,ITT.N,ITUB.N,ITW.N,IVA.O,IVAC.O,IVC.N,IVR.N,IVR_A.N,IVR_B.N,IVR_C.N,IVZ.N,IX.N,IZEA.O,J.N,JACK.O,JAGX.O,JAKK.O,JAMF.O,JAN.O,JAX.N,JAZZ.O,JBGS.N,JBHT.O,JBL.N,JBLU.O,JBSS.O,JBT.N,JCAP.N,JCAP_B.N,JCI.N,JCOM.O,JCS.O,JCTCF.O,JD.O,JE.N,JE_A.N,JEF.N,JELD.N,JFIN.O,JFK.O,JFKKU.O,JFU.O,JG.O,JHG.N,JHX.N,JIH.N,JIH_U.N,JILL.N,JJSF.O,JKHY.O,JKS.N,JLL.N,JMIA.N,JMP.N,JNCE.O,JNJ.N,JNPR.N,JOB.A,JOBS.O,JOE.N,JOUT.O,JP.N,JPM.N,JPM_C.N,JPM_D.N,JPM_G.N,JPM_H.N,JPM_J.N,JRJC.O,JRSH.O,JRVR.O,JT.N,JVA.O,JW_A.N,JW_B.N,JWN.N,JWS.N,JWS_U.N,JYNT.O,K.N,KAI.N,KALA.O,KALU.O,KALV.O,KAMN.N,KAR.N,KB.N,KBAL.O,KBH.N,KBLM.O,KBLMU.O,KBNT.O,KBR.N,KBSF.O,KC.O,KCAC.N,KCAC_U.N,KDMN.N,KDP.O,KE.O,KELYA.O,KELYB.O,KEN.N,KEP.N,KEQU.O,KERN.O,KEX.N,KEY.N,KEY_I.N,KEY_J.N,KEY_K.N,KEYS.N,KFFB.O,KFRC.O,KFS.N,KFY.N,KGC.N,KHC.O,KIDS.O,KIM.N,KIM_L.N,KIM_M.N,KIN.O,KINS.O,KIQ.A,KIRK.O,KKR.N,KKR_A.N,KKR_B.N,KKR_C.N,KL.N,KLAC.O,KLDO.O,KLIC.O,KLR.A,KLXE.O,KMB.N,KMDA.O,KMI.N,KMPR.N,KMT.N,KMX.N,KN.N,KNDI.O,KNL.N,KNOP.N,KNSA.O,KNSL.O,KNX.N,KO.N,KOD.O,KODK.N,KOF.N,KOP.N,KOPN.O,KOR.O,KOS.N,KOSS.O,KPTI.O,KR.N,KRA.N,KRC.N,KREF.N,KRG.N,KRKR.O,KRMD.O,KRNT.O,KRNY.O,KRO.N,KROS.O,KRP.N,KRTX.O,KRUS.O,KRYS.O,KSMTU.O,KSPN.O,KSS.N,KSU.N,KSU_.N,KT.N,KTB.N,KTCC.O,KTOS.O,KTOV.O,KTRA.O,KURA.O,KVHI.O,KW.N,KWR.N,KXIN.O,KYMR.O,KZIA.O,KZR.O,L.N,LAC.N,LACQ.O,LACQU.O,LAD.N,LADR.N,LAIX.N,LAKE.O,LAMR.O,LANC.O,LAND.O,LANDP.O,LARK.O,LASR.O,LATN.O,LATNU.O,LAUR.O,LAWS.O,LAZ.N,LAZY.O,LB.N,LBAI.O,LBC.O,LBRDA.O,LBRDK.O,LBRT.N,LBTYA.O,LBTYB.O,LBTYK.O,LC.N,LCA.O,LCAHU.O,LCAPU.O,LCI.N,LCII.N,LCNB.O,LCTX.A,LCUT.O,LDL.N,LDOS.N,LE.O,LEA.N,LEAF.N,LEAP_U.N,LECO.O,LEDS.O,LEE.N,LEG.N,LEGH.O,LEGN.O,LEJU.N,LEN.N,LEN_B.N,LEU.A,LEVI.N,LEVL.O,LEVLP.O,LFAC.O,LFACU.O,LFC.N,LFUS.O,LFVN.O,LGC.N,LGC_U.N,LGF_A.N,LGF_B.N,LGHL.O,LGIH.O,LGL.A,LGND.O,LGVW.N,LGVW_U.N,LH.N,LHCG.O,LHX.N,LI.O,LIFE.O,LII.N,LILA.O,LILAK.O,LIN.N,LINC.O,LIND.O,LINX.N,LIQT.O,LITB.N,LITE.O,LIVE.O,LIVK.O,LIVKU.O,LIVN.O,LIVX.O,LIZI.O,LJPC.O,LKCO.O,LKFN.O,LKQ.O,LL.N,LLIT.O,LLNW.O,LLY.N,LMAT.O,LMB.O,LMFA.O,LMND.N,LMNL.O,LMNR.O,LMNX.O,LMPX.O,LMRK.O,LMRKN.O,LMRKO.O,LMRKP.O,LMST.O,LMT.N,LN.N,LNC.N,LND.N,LNDC.O,LNG.A,LNN.N,LNT.O,LNTH.O,LOAC.O,LOACU.O,LOAK.N,LOAK_U.N,LOAN.O,LOB.O,LOCO.O,LODE.A,LOGC.O,LOGI.O,LOMA.N,LONE.O,LOOP.O,LOPE.O,LORL.O,LOV.A,LOVE.O,LOW.N,LPCN.O,LPG.N,LPI.N,LPL.N,LPLA.O,LPRO.O,LPSN.O,LPTH.O,LPTX.O,LPX.N,LQDA.O,LQDT.O,LRCX.O,LRMR.O,LRN.N,LSAC.O,LSACU.O,LSBK.O,LSCC.O,LSF.A,LSI.N,LSPD.N,LSTR.O,LSXMA.O,LSXMB.O,LSXMK.O,LTBR.O,LTC.N,LTHM.N,LTRN.O,LTRPA.O,LTRPB.O,LTRX.O,LUB.N,LULU.O,LUMN.N,LUMO.O,LUNA.O,LUV.N,LVGO.O,LVS.N,LW.N,LWAY.O,LX.O,LXFR.N,LXP.N,LXP_C.N,LXRX.O,LXU.N,LYB.N,LYFT.O,LYG.N,LYL.O,LYRA.O,LYTS.O,LYV.N,LZB.N,M.N,MA.N,MAA.N,MAA_I.N,MAC.N,MACK.O,MAG.A,MAGS.O,MAIN.N,MAN.N,MANH.O,MANT.O,MANU.N,MAR.O,MARA.O,MARK.O,MARPS.O,MAS.N,MASI.O,MAT.O,MATW.O,MATX.N,MAXN.O,MAXR.N,MAYS.O,MBCN.O,MBI.N,MBII.O,MBIN.O,MBINO.O,MBINP.O,MBIO.O,MBNKP.O,MBOT.O,MBRX.O,MBT.N,MBUU.O,MBWM.O,MC.N,MCAC.O,MCACU.O,MCB.N,MCBC.O,MCBS.O,MCC.N,MCD.N,MCEP.O,MCF.A,MCFT.O,MCHP.O,MCHX.O,MCK.N,MCMJ.O,MCO.N,MCRB.O,MCRI.O,MCS.N,MCY.N,MD.N,MDB.O,MDC.N,MDCA.O,MDGL.O,MDGS.O,MDIA.O,MDJH.O,MDLA.N,MDLY.N,MDLZ.O,MDNA.O,MDP.N,MDRR.O,MDRRP.O,MDRX.O,MDT.N,MDU.N,MDWD.O,MEC.N,MED.N,MEDP.O,MEDS.O,MEG.N,MEI.N,MEIP.O,MELI.O,MEOH.O,MERC.O,MESA.O,MESO.O,MET.N,MET_A.N,MET_E.N,MET_F.N,METC.O,METX.O,MFA.N,MFA_B.N,MFA_C.N,MFAC.N,MFAC_U.N,MFC.N,MFG.N,MFGP.N,MFH.O,MFIN.O,MFNC.O,MG.N,MGA.N,MGEE.O,MGEN.O,MGI.O,MGIC.O,MGLN.O,MGM.N,MGNI.O,MGNX.O,MGP.N,MGPI.O,MGRC.O,MGTA.O,MGTX.O,MGY.N,MGYR.O,MH_A.N,MH_C.N,MH_D.N,MHH.A,MHK.N,MHLD.O,MHO.N,MIC.N,MICT.O,MIDD.O,MIK.O,MIME.O,MIND.O,MINDP.O,MIRM.O,MIST.O,MITK.O,MITO.O,MITT.N,MITT_A.N,MITT_B.N,MITT_C.N,MIXT.N,MKC.N,MKC_V.N,MKD.O,MKGI.O,MKL.N,MKSI.O,MKTX.O,MLAB.O,MLAC.O,MLACU.O,MLCO.O,MLHR.O,MLI.N,MLM.N,MLND.O,MLP.N,MLR.N,MLSS.A,MLVF.O,MMAC.O,MMC.N,MMI.N,MMLP.O,MMM.N,MMP.N,MMS.N,MMSI.O,MMX.A,MMYT.O,MN.N,MNCL.O,MNCLU.O,MNDO.O,MNK.N,MNKD.O,MNOV.O,MNPR.O,MNR.N,MNR_C.N,MNRL.N,MNRO.O,MNSB.O,MNSBP.O,MNST.O,MNTA.O,MNTX.O,MO.N,MOBL.O,MOD.N,MODN.N,MOFG.O,MOG_A.N,MOG_B.N,MOGO.O,MOGU.N,MOH.N,MOHO.O,MOMO.O,MOR.O,MORF.O,MORN.O,MOS.N,MOSY.O,MOTS.O,MOV.N,MOXC.O,MPAA.O,MPB.O,MPC.N,MPLX.N,MPW.N,MPWR.O,MPX.N,MR.N,MRAM.O,MRBK.O,MRC.N,MRCC.O,MRCY.O,MREO.O,MRIN.O,MRK.N,MRKR.O,MRLN.O,MRNA.O,MRNS.O,MRO.N,MRSN.O,MRTN.O,MRTX.O,MRUS.O,MRVL.O,MS.N,MS_A.N,MS_E.N,MS_F.N,MS_I.N,MS_K.N,MS_L.N,MSA.N,MSB.N,MSBI.O,MSC.N,MSCI.N,MSEX.O,MSFT.O,MSGE.N,MSGN.N,MSGS.N,MSI.N,MSM.N,MSN.A,MSON.O,MSTR.O,MSVB.O,MT.N,MTA.A,MTB.N,MTBC.O,MTBCP.O,MTC.O,MTCH.O,MTCR.O,MTD.N,MTDR.N,MTEM.O,MTEX.O,MTG.N,MTH.N,MTL.N,MTL_.N,MTLS.O,MTN.N,MTNB.A,MTOR.N,MTP.O,MTR.N,MTRN.N,MTRX.O,MTSC.O,MTSI.O,MTSL.O,MTW.N,MTX.N,MTZ.N,MU.O,MUFG.N,MUR.N,MUSA.N,MUX.N,MVBF.O,MVC.N,MVIS.O,MVO.N,MWA.N,MWK.O,MX.N,MXC.A,MXIM.O,MXL.N,MYE.N,MYFW.O,MYGN.O,MYL.O,MYO.A,MYOK.O,MYOS.O,MYOV.N,MYRG.O,MYSZ.O,MYT.O,NAII.O,NAK.A,NAKD.O,NAOV.O,NARI.O,NAT.N,NATH.O,NATI.O,NATR.O,NAV.N,NAV_D.N,NAVB.A,NAVI.O,NBAC.O,NBACU.O,NBEV.O,NBHC.N,NBIX.O,NBL.O,NBLX.O,NBN.O,NBR.N,NBR_A.N,NBRV.O,NBSE.O,NBTB.O,NBY.A,NC.N,NCBS.O,NCLH.N,NCMI.O,NCNA.O,NCNO.O,NCR.N,NCSM.O,NCTY.O,NCV_A.N,NCZ_A.N,NDAQ.O,NDLS.O,NDRA.O,NDSN.O,NEE.N,NEM.N,NEN.A,NEO.O,NEOG.O,NEON.O,NEOS.O,NEP.N,NEPH.O,NEPT.O,NERV.O,NES.A,NESR.O,NET.N,NETE.O,NEU.N,NEW.N,NEWA.O,NEWR.N,NEWT.O,NEX.N,NEXA.N,NEXT.O,NFBK.O,NFE.O,NFG.N,NFH.N,NFIN.O,NFINU.O,NFLX.O,NG.A,NGA_U.N,NGD.A,NGG.N,NGHC.O,NGHCN.O,NGHCO.O,NGHCP.O,NGL.N,NGL_B.N,NGL_C.N,NGLS_A.N,NGM.O,NGS.N,NGVC.N,NGVT.N,NH.O,NHC.A,NHI.N,NHIC.O,NHICU.O,NHLD.O,NHTC.O,NI.N,NI_B.N,NICE.O,NICK.O,NINE.N,NIO.N,NIU.O,NJR.N,NK.O,NKE.N,NKLA.O,NKSH.O,NKTR.O,NKTX.O,NL.N,NLOK.O,NLS.N,NLSN.N,NLTX.O,NLY.N,NLY_D.N,NLY_F.N,NLY_G.N,NLY_I.N,NM.N,NM_G.N,NM_H.N,NMCI.O,NMFC.O,NMIH.O,NMK_B.N,NMK_C.N,NMM.N,NMMCU.O,NMR.N,NMRD.O,NMRK.O,NMTR.O,NNA.N,NNBR.O,NNDM.O,NNI.N,NNN.N,NNN_F.N,NNOX.O,NNVC.A,NOA.N,NOAH.N,NOC.N,NODK.O,NOG.A,NOK.N,NOMD.N,NOV.N,NOVA.N,NOVN.O,NOVS.O,NOVSU.O,NOVT.O,NOW.N,NP.N,NPA.O,NPAUU.O,NPK.N,NPO.N,NPTN.N,NR.N,NRBO.O,NRC.O,NREF.N,NREF_A.N,NRG.N,NRIM.O,NRIX.O,NRP.N,NRT.N,NRZ.N,NRZ_A.N,NRZ_B.N,NRZ_C.N,NS.N,NS_A.N,NS_B.N,NS_C.N,NSA.N,NSA_A.N,NSC.N,NSCO.N,NSEC.O,NSH_U.N,NSIT.O,NSP.N,NSPR.A,NSSC.O,NSTG.O,NSYS.O,NTAP.O,NTB.N,NTCO.N,NTCT.O,NTEC.O,NTES.O,NTGR.O,NTIC.O,NTIP.A,NTLA.O,NTN.A,NTNX.O,NTP.N,NTR.N,NTRA.O,NTRP.O,NTRS.O,NTRSO.O,NTST.N,NTUS.O,NTWK.O,NTZ.N,NUAN.O,NUE.N,NURO.O,NUS.N,NUVA.O,NUZE.O,NVAX.O,NVCN.O,NVCR.O,NVDA.O,NVEC.O,NVEE.O,NVFY.O,NVGS.N,NVIV.O,NVMI.O,NVO.N,NVR.N,NVRO.N,NVS.N,NVST.N,NVT.N,NVTA.N,NVUS.O,NWBI.O,NWE.N,NWFL.O,NWG.N,NWGI.O,NWHM.N,NWL.O,NWLI.O,NWN.N,NWPX.O,NWS.O,NWSA.O,NX.N,NXE.A,NXGN.O,NXPI.O,NXRT.N,NXST.O,NXTC.O,NXTD.O,NYC.N,NYCB.N,NYCB_A.N,NYCB_U.N,NYMT.O,NYMTM.O,NYMTN.O,NYMTO.O,NYMTP.O,NYMX.O,NYT.N,O.N,OAC.N,OAC_U.N,OACB_U.N,OAK_A.N,OAK_B.N,OAS.O,OBAS.O,OBCI.O,OBLG.A,OBLN.O,OBNK.O,OBSV.O,OC.N,OCC.O,OCCI.O,OCCIP.O,OCFC.O,OCFCP.O,OCFT.N,OCGN.O,OCN.N,OCSI.O,OCSL.O,OCUL.O,OCX.A,ODC.N,ODFL.O,ODP.O,ODT.O,OEC.N,OEG.O,OESX.O,OFC.N,OFED.O,OFG.N,OFG_A.N,OFG_B.N,OFG_D.N,OFIX.O,OFLX.O,OFS.O,OGCP.A,OGE.N,OGEN.A,OGI.O,OGS.N,OHI.N,OI.N,OIBR_C.N,OII.N,OIIM.O,OIS.N,OKE.N,OKTA.O,OLB.O,OLED.O,OLLI.O,OLN.N,OLP.N,OM.O,OMAB.O,OMC.N,OMCL.O,OMER.O,OMEX.O,OMF.N,OMI.N,OMP.O,ON.O,ONB.O,ONCS.O,ONCT.O,ONCY.O,ONDK.N,ONE.N,ONEM.O,ONEW.O,ONTO.N,ONTX.O,ONVO.O,OOMA.N,OPBK.O,OPCH.O,OPES.O,OPESU.O,OPGN.O,OPHC.O,OPI.O,OPK.O,OPNT.O,OPOF.O,OPRA.O,OPRT.O,OPRX.O,OPTN.O,OPTT.O,OPY.N,OR.N,ORA.N,ORAN.N,ORBC.O,ORC.N,ORCC.N,ORCL.N,ORGO.O,ORGS.O,ORI.N,ORIC.O,ORLY.O,ORMP.O,ORN.N,ORPH.O,ORRF.O,ORSN.O,ORSNU.O,ORTX.O,OSB.N,OSBC.O,OSG.N,OSH.N,OSIS.O,OSK.N,OSMT.O,OSN.O,OSPN.O,OSS.O,OSTK.O,OSUR.O,OSW.O,OTEL.O,OTEX.O,OTIC.O,OTIS.N,OTLK.O,OTRK.O,OTRKP.O,OTTR.O,OUT.N,OVBC.O,OVID.O,OVLY.O,OVV.N,OXBR.O,OXFD.O,OXLCM.O,OXLCO.O,OXLCP.O,OXM.N,OXSQ.O,OXY.N,OYST.O,OZK.O,PAA.N,PAAS.O,PAC.N,PACB.O,PACD.N,PACK.N,PACW.O,PAE.O,PAG.N,PAGP.N,PAGS.N,PAHC.O,PAICU.O,PAM.N,PANA.N,PANA_U.N,PAND.O,PANL.O,PANW.N,PAR.N,PARR.N,PASG.O,PATI.O,PATK.O,PAVM.O,PAYC.N,PAYS.O,PAYX.O,PB.N,PBA.N,PBCT.O,PBCTP.O,PBF.N,PBFS.O,PBFX.N,PBH.N,PBHC.O,PBI.N,PBIP.O,PBPB.O,PBR.N,PBR_A.N,PBT.N,PBTS.O,PBYI.O,PCAR.O,PCB.O,PCG.N,PCG_A.A,PCG_B.A,PCG_C.A,PCG_D.A,PCG_E.A,PCG_G.A,PCG_H.A,PCG_I.A,PCH.O,PCOM.O,PCPL.N,PCPL_U.N,PCRX.O,PCSA.O,PCSB.O,PCTI.O,PCTY.O,PCVX.O,PCYG.O,PCYO.O,PD.N,PDAC_U.N,PDCE.O,PDCO.O,PDD.O,PDEX.O,PDFS.O,PDLB.O,PDLI.O,PDLIV.O,PDM.N,PDS.N,PDSB.O,PE.N,PEAK.N,PEB.N,PEB_C.N,PEB_D.N,PEB_E.N,PEB_F.N,PEBK.O,PEBO.O,PECK.O,PED.A,PEG.N,PEGA.O,PEI.N,PEI_B.N,PEI_C.N,PEI_D.N,PEIX.O,PEN.N,PENN.O,PEP.O,PERI.O,PESI.O,PETQ.O,PETS.O,PETZ.O,PFBC.O,PFBI.O,PFC.O,PFE.N,PFG.O,PFGC.N,PFHD.O,PFIE.O,PFIN.O,PFIS.O,PFLT.O,PFMT.O,PFNX.A,PFPT.O,PFS.N,PFSI.N,PFSW.O,PG.N,PGC.O,PGEN.O,PGNY.O,PGR.N,PGRE.N,PGTI.N,PH.N,PHAS.O,PHAT.O,PHCF.O,PHG.N,PHGE.A,PHGE_U.A,PHI.N,PHIO.O,PHM.N,PHR.N,PHUN.O,PHX.N,PI.O,PIAI_U.N,PIC.N,PIC_U.N,PICO.O,PIH.O,PIHPP.O,PII.N,PINC.O,PINE.N,PING.N,PINS.N,PIPR.N,PIRS.O,PIXY.O,PJT.N,PK.N,PKBK.O,PKE.N,PKG.N,PKI.N,PKOH.O,PKX.N,PLAB.O,PLAG.A,PLAN.N,PLAY.O,PLBC.O,PLCE.O,PLD.N,PLG.A,PLIN.O,PLL.O,PLM.A,PLMR.O,PLNT.N,PLOW.N,PLPC.O,PLRX.O,PLSE.O,PLT.N,PLUG.O,PLUS.O,PLX.A,PLXP.O,PLXS.O,PLYA.O,PLYM.N,PLYM_A.A,PM.N,PMBC.O,PMD.O,PME.O,PMT.N,PMT_A.N,PMT_B.N,PMVC_U.N,PMVP.O,PNBK.O,PNC.N,PNC_P.N,PNFP.O,PNFPP.O,PNM.N,PNNT.O,PNR.N,PNRG.O,PNTG.O,PNW.N,POAI.O,PODD.O,POLA.O,POOL.O,POR.N,POST.N,POWI.O,POWL.O,PPBI.O,PPC.O,PPD.O,PPG.N,PPIH.O,PPL.N,PPSI.O,PQG.N,PRA.N,PRAA.O,PRAH.O,PRCP.O,PRDO.O,PRE_F.N,PRE_G.N,PRE_H.N,PRE_I.N,PRFT.O,PRFX.O,PRGO.N,PRGS.O,PRGX.O,PRI.N,PRIF_A.N,PRIF_B.N,PRIF_C.N,PRIF_D.N,PRIF_E.N,PRIF_F.N,PRIM.O,PRK.A,PRLB.N,PRLD.O,PRMW.N,PRNB.O,PRO.N,PROF.O,PROG.O,PROS.N,PROV.O,PRPB.N,PRPB_U.N,PRPH.O,PRPL.O,PRPO.O,PRQR.O,PRSC.O,PRSP.N,PRT.N,PRTA.O,PRTH.O,PRTK.O,PRTS.O,PRTY.N,PRU.N,PRVB.O,PRVL.O,PS.O,PSA.N,PSA_B.N,PSA_C.N,PSA_D.N,PSA_E.N,PSA_F.N,PSA_G.N,PSA_H.N,PSA_I.N,PSA_J.N,PSA_K.N,PSA_L.N,PSA_M.N,PSA_W.N,PSA_X.N,PSAC.O,PSACU.O,PSB.N,PSB_W.N,PSB_X.N,PSB_Y.N,PSB_Z.N,PSEC.O,PSHG.O,PSMT.O,PSN.N,PSNL.O,PSO.N,PSTG.N,PSTH.N,PSTI.O,PSTL.N,PSTV.O,PSTX.O,PSX.N,PSXP.N,PT.O,PTAC.O,PTACU.O,PTC.O,PTCT.O,PTE.O,PTEN.O,PTGX.O,PTI.O,PTK.A,PTK_U.A,PTMN.O,PTN.A,PTNR.O,PTON.O,PTR.N,PTRS.O,PTSI.O,PTVCA.O,PTVCB.O,PTVE.O,PUK.N,PUK_.N,PUK_A.N,PULM.O,PUMP.N,PUYI.O,PVAC.O,PVBC.O,PVG.N,PVH.N,PVL.N,PW.A,PW_A.A,PWFL.O,PWOD.O,PWR.N,PXD.N,PXLW.O,PXS.O,PYPD.O,PYPL.O,PZG.A,PZN.N,PZZA.O,QADA.O,QADB.O,QCOM.O,QCRH.O,QD.N,QDEL.O,QEP.N,QFIN.O,QGEN.N,QH.O,QIWI.O,QK.O,QLGN.O,QLYS.O,QMCO.O,QNST.O,QRHC.O,QRTEA.O,QRTEB.O,QRTEP.O,QRVO.O,QSR.N,QTNT.O,QTRX.O,QTS.N,QTS_A.N,QTS_B.N,QTT.O,QTWO.N,QUAD.N,QUIK.O,QUMU.O,QUOT.N,QURE.O,R.N,RACA.O,RACE.N,RAD.N,RADA.O,RAIL.O,RAMP.N,RAND.O,RAPT.O,RARE.O,RAVE.O,RAVN.O,RBA.N,RBAC_U.N,RBB.O,RBBN.O,RBC.N,RBCAA.O,RBCN.O,RBKB.O,RBNC.O,RC.N,RCEL.O,RCI.N,RCII.O,RCKT.O,RCKY.O,RCL.N,RCM.O,RCMT.O,RCON.O,RCUS.N,RDCM.O,RDFN.O,RDHL.O,RDI.O,RDIB.O,RDN.N,RDNT.O,RDS_A.N,RDS_B.N,RDUS.O,RDVT.O,RDWR.O,RDY.N,RE.N,REAL.O,REDU.O,REED.O,REFR.O,REG.O,REGI.O,REGN.O,REI.A,REKR.O,RELL.O,RELV.O,RELX.N,RENN.N,REPH.O,REPL.O,RES.N,RESI.N,RESN.O,RETA.O,RETO.O,REV.N,REVG.N,REX.N,REXN.O,REXR.N,REXR_A.N,REXR_B.N,REXR_C.N,REYN.O,REZI.N,RF.N,RF_A.N,RF_B.N,RF_C.N,RFIL.O,RFL.N,RFP.N,RGA.N,RGCO.O,RGEN.O,RGLD.O,RGLS.O,RGNX.O,RGP.O,RGR.N,RGS.N,RH.N,RHE.A,RHE_A.A,RHI.N,RHP.N,RIBT.O,RICK.O,RIG.N,RIGL.O,RILY.O,RILYL.O,RILYP.O,RIO.N,RIOT.O,RIVE.O,RJF.N,RKDA.O,RKT.N,RL.N,RLAY.O,RLGT.A,RLGY.N,RLH.N,RLI.N,RLJ.N,RLJ_A.N,RLMD.O,RM.N,RMAX.N,RMBI.O,RMBL.O,RMBS.O,RMCF.O,RMD.N,RMED.N,RMG.N,RMG_U.N,RMM.N,RMNI.O,RMPL_.N,RMR.O,RMTI.O,RNA.O,RNDB.O,RNET.O,RNG.N,RNGR.N,RNLX.O,RNR.N,RNR_E.N,RNR_F.N,RNST.O,RNWK.O,ROAD.O,ROCH.O,ROCHU.O,ROCK.O,ROG.N,ROIC.O,ROK.N,ROKU.O,ROL.N,ROLL.O,ROP.N,ROST.O,RP.O,RPAI.N,RPAY.O,RPD.O,RPLA.N,RPLA_U.N,RPM.N,RPRX.O,RPT.N,RPT_D.N,RPTX.O,RRBI.O,RRC.N,RRD.N,RRGB.O,RRR.O,RS.N,RSF.N,RSG.N,RSSS.O,RST.N,RTLR.O,RTP_U.N,RTRX.O,RTX.N,RUBY.O,RUHN.O,RUN.O,RUSHA.O,RUSHB.O,RUTH.O,RVI.N,RVLV.N,RVMD.O,RVNC.O,RVP.A,RVSB.O,RWLK.O,RWT.N,RXN.N,RXT.O,RY.N,RY_T.N,RYAAY.O,RYAM.N,RYB.N,RYI.N,RYN.N,RYTM.O,SA.N,SABR.O,SABRP.O,SACH.A,SAFE.N,SAFM.O,SAFT.O,SAGE.O,SAH.N,SAIA.O,SAIC.N,SAIIU.O,SAIL.N,SAL.O,SALM.O,SALT.N,SAM.N,SAMA.O,SAMAU.O,SAMG.O,SAN.N,SAN_B.N,SAND.N,SANM.O,SANW.O,SAP.N,SAQN.O,SAQNU.O,SAR.N,SASR.O,SATS.O,SAVA.O,SAVE.N,SB.N,SB_C.N,SB_D.N,SBAC.O,SBBP.O,SBCF.O,SBE.N,SBE_U.N,SBFG.O,SBG_U.N,SBGI.O,SBH.N,SBLK.O,SBNY.O,SBOW.N,SBPH.O,SBR.N,SBRA.O,SBS.N,SBSI.O,SBSW.N,SBT.O,SBUX.O,SC.N,SCCO.N,SCE_B.A,SCE_C.A,SCE_D.A,SCE_E.A,SCE_G.N,SCE_H.N,SCE_J.N,SCE_K.N,SCE_L.N,SCHL.O,SCHN.O,SCHW.N,SCHW_C.N,SCHW_D.N,SCI.N,SCKT.O,SCL.N,SCM.N,SCON.O,SCOR.O,SCPE.N,SCPE_U.N,SCPH.O,SCPL.O,SCS.N,SCSC.O,SCU.N,SCVL.O,SCVX.N,SCVX_U.N,SCWX.O,SCX.N,SCYX.O,SD.N,SDC.O,SDGR.O,SDPI.A,SE.N,SEAC.O,SEAS.N,SEB.A,SECO.O,SEDG.O,SEE.N,SEED.O,SEEL.O,SEIC.O,SELB.O,SELF.O,SEM.N,SENEA.O,SENEB.O,SENS.A,SERV.N,SESN.O,SF.N,SF_A.N,SF_B.N,SF_C.N,SFBC.O,SFBS.O,SFE.N,SFET.O,SFIX.O,SFL.N,SFM.O,SFNC.O,SFST.O,SFTW.N,SFTW_U.N,SFUN.N,SG.O,SGA.O,SGBX.O,SGC.O,SGEN.O,SGH.O,SGLB.O,SGMA.O,SGMO.O,SGMS.O,SGOC.O,SGRP.O,SGRY.O,SGU.N,SHAK.N,SHBI.O,SHEN.O,SHG.N,SHI.N,SHIP.O,SHLL.N,SHLL_U.N,SHLX.N,SHO.N,SHO_E.N,SHO_F.N,SHOO.O,SHOP.N,SHSP.O,SHW.N,SHYF.O,SI.N,SIBN.O,SIC.O,SID.N,SIEB.O,SIEN.O,SIF.A,SIFY.O,SIG.N,SIGA.O,SIGI.O,SII.N,SILC.O,SILK.O,SILV.A,SIM.A,SIMO.O,SINA.O,SINO.O,SINT.O,SIRI.O,SITC.N,SITC_A.N,SITC_K.N,SITE.N,SITM.O,SIVB.O,SIVBP.O,SIX.N,SJ.O,SJI.N,SJM.N,SJR.N,SJT.N,SJW.N,SKM.N,SKT.N,SKX.N,SKY.N,SKYS.O,SKYW.O,SLAB.O,SLB.N,SLCA.N,SLCT.O,SLDB.O,SLF.N,SLG.N,SLG_I.N,SLGG.O,SLGL.O,SLGN.O,SLM.O,SLMBP.O,SLN.O,SLNO.O,SLP.O,SLQT.N,SLRC.O,SLRX.O,SLS.O,SM.N,SMAR.N,SMBC.O,SMBK.O,SMCI.O,SMED.O,SMFG.N,SMG.N,SMHI.N,SMIT.O,SMLP.N,SMMC.O,SMMCU.O,SMMF.O,SMMT.O,SMP.N,SMPL.O,SMSI.O,SMTC.O,SMTS.A,SMTX.O,SNA.N,SNAP.N,SNBP.O,SNBR.O,SNCA.O,SNCR.O,SND.O,SNDE.O,SNDL.O,SNDR.N,SNDX.O,SNE.N,SNES.O,SNEX.O,SNFCA.O,SNGX.O,SNMP.A,SNN.N,SNOA.O,SNOW.N,SNP.N,SNPR_U.N,SNPS.O,SNR.N,SNSS.O,SNV.N,SNV_D.N,SNV_E.N,SNX.N,SNY.O,SO.N,SOAC.N,SOAC_U.N,SOGO.N,SOHO.O,SOHOB.O,SOHON.O,SOHOO.O,SOHU.O,SOI.N,SOL.N,SOLO.O,SOLY.O,SON.N,SONA.O,SONM.O,SONN.O,SONO.O,SOS.N,SP.O,SPAQ.N,SPAQ_U.N,SPB.N,SPCB.O,SPCE.N,SPE_B.N,SPFI.O,SPG.N,SPG_J.N,SPGI.N,SPH.N,SPI.O,SPKE.O,SPKEP.O,SPLK.O,SPLP.N,SPLP_A.N,SPNE.O,SPNS.O,SPOK.O,SPOT.N,SPPI.O,SPR.N,SPRO.O,SPRT.O,SPSC.O,SPT.O,SPTN.O,SPWH.O,SPWR.O,SPXC.N,SQ.N,SQBG.O,SQM.N,SQNS.N,SR.N,SR_A.N,SRAC.O,SRACU.O,SRAX.O,SRC.N,SRC_A.N,SRCE.O,SRCL.O,SRDX.O,SRE.N,SRE_A.N,SRE_B.N,SREV.O,SRG.N,SRG_A.N,SRGA.O,SRI.N,SRL.N,SRLP.N,SRNE.O,SRPT.O,SRRA.O,SRRK.O,SRT.N,SRTS.O,SSB.O,SSBI.O,SSD.N,SSKN.O,SSL.N,SSNC.O,SSNT.O,SSP.O,SSPK.O,SSPKU.O,SSRM.O,SSSS.O,SSTI.O,SSTK.N,SSY.A,SSYS.O,ST.N,STAA.O,STAF.O,STAG.N,STAG_C.N,STAR.N,STAR_D.N,STAR_G.N,STAR_I.N,STAY.O,STBA.O,STC.N,STCN.O,STE.N,STEP.O,STFC.O,STG.N,STIM.O,STKL.O,STKS.O,STL.N,STL_A.N,STLD.O,STM.N,STMP.O,STN.N,STND.O,STNE.O,STNG.N,STOK.O,STON.N,STOR.N,STPK_U.N,STRA.O,STRL.O,STRM.O,STRO.O,STRS.O,STRT.O,STSA.O,STT.N,STT_D.N,STT_G.N,STWD.N,STWOU.O,STX.O,STXB.O,STXS.A,STZ.N,STZ_B.N,SU.N,SUI.N,SUM.N,SUMO.O,SUMR.O,SUN.N,SUNS.O,SUNW.O,SUP.N,SUPN.O,SUPV.N,SURF.O,SUZ.N,SVA.O,SVACU.O,SVBI.O,SVC.O,SVM.A,SVMK.O,SVRA.O,SVT.A,SVVC.O,SWAV.O,SWBI.O,SWCH.N,SWI.N,SWIR.O,SWK.N,SWKH.O,SWKS.O,SWM.N,SWN.N,SWTX.O,SWX.N,SXC.N,SXI.N,SXT.N,SXTC.O,SY.O,SYBT.O,SYBX.O,SYF.N,SYF_A.N,SYK.N,SYKE.O,SYN.A,SYNA.O,SYNC.O,SYNH.O,SYNL.O,SYPR.O,SYRS.O,SYTA.O,SYX.N,SYY.N,T.N,T_A.N,T_C.N,TA.O,TAC.N,TACO.O,TACT.O,TAIT.O,TAK.N,TAL.N,TALO.N,TANH.O,TAOP.O,TAP.N,TAP_A.N,TARA.O,TARO.N,TAST.O,TAT.A,TATT.O,TAYD.O,TBBK.O,TBI.N,TBIO.O,TBK.O,TBKCP.O,TBLT.O,TBNK.O,TBPH.O,TC.O,TCBI.O,TCBIP.O,TCBK.O,TCCO.O,TCDA.O,TCF.O,TCFC.O,TCFCP.O,TCI.N,TCMD.O,TCO.N,TCO_J.N,TCO_K.N,TCOM.O,TCON.O,TCP.N,TCPC.O,TCRR.O,TCS.N,TCX.O,TD.N,TDAC.O,TDACU.O,TDC.N,TDG.N,TDOC.N,TDS.N,TDW.N,TDY.N,TEAM.O,TECH.O,TECK.N,TECTP.O,TEDU.O,TEF.N,TEL.N,TELA.O,TELL.O,TEN.N,TENB.O,TENX.O,TEO.N,TER.O,TESS.O,TEUM.O,TEVA.N,TEX.N,TFC.N,TFC_F.N,TFC_G.N,TFC_H.N,TFC_I.N,TFC_O.N,TFC_R.N,TFFP.O,TFII.N,TFSL.O,TFX.N,TG.N,TGA.O,TGB.A,TGC.A,TGH.N,TGI.N,TGLS.O,TGNA.N,TGP.N,TGP_A.N,TGP_B.N,TGS.N,TGT.N,TGTX.O,TH.O,THBR.O,THBRU.O,THC.N,THCA.O,THCAU.O,THCB.O,THCBU.O,THFF.O,THG.N,THM.A,THMO.O,THO.N,THR.N,THRM.O,THS.N,THTX.O,TIF.N,TIG.O,TIGO.O,TIGR.O,TILE.O,TIPT.O,TISI.N,TITN.O,TJX.N,TK.N,TKAT.A,TKC.N,TKR.N,TLC.O,TLGT.O,TLK.N,TLND.O,TLRY.O,TLSA.O,TLYS.N,TM.N,TMBR.A,TMDI.O,TMDX.O,TME.N,TMHC.N,TMO.N,TMP.A,TMQ.A,TMST.N,TMUS.O,TNAV.O,TNC.N,TNDM.O,TNET.N,TNK.N,TNP.N,TNP_C.N,TNP_D.N,TNP_E.N,TNP_F.N,TNXP.O,TOL.N,TOPS.O,TOT.N,TOTA.O,TOTAU.O,TOUR.O,TOWN.O,TPB.N,TPC.N,TPCO.O,TPH.N,TPHS.A,TPIC.O,TPL.N,TPR.N,TPRE.N,TPTX.O,TPVG.N,TPX.N,TR.N,TRC.N,TRCH.O,TREB.N,TREB_U.N,TREC.N,TREE.O,TREX.N,TRGP.N,TRHC.O,TRI.N,TRIB.O,TRIL.O,TRIP.O,TRMB.O,TRMD.O,TRMK.O,TRMT.O,TRN.N,TRNE.N,TRNE_U.N,TRNO.N,TRNS.O,TROW.O,TROX.N,TRP.N,TRQ.N,TRS.O,TRST.O,TRT.A,TRTN.N,TRTN_A.N,TRTN_B.N,TRTN_C.N,TRTN_D.N,TRTX.N,TRU.N,TRUE.O,TRUP.O,TRV.N,TRVG.O,TRVI.O,TRVN.O,TRWH.N,TRX.A,TRXC.A,TS.N,TSBK.O,TSC.O,TSCAP.O,TSCBP.O,TSCO.O,TSE.N,TSEM.O,TSHA.O,TSLA.O,TSLX.N,TSM.N,TSN.N,TSQ.N,TSRI.O,TSU.N,TT.N,TTC.N,TTD.O,TTEC.O,TTEK.O,TTGT.O,TTI.N,TTM.N,TTMI.O,TTNP.O,TTOO.O,TTWO.O,TU.N,TUFN.N,TUP.N,TURN.O,TUSK.O,TV.N,TVTY.O,TW.O,TWCTU.O,TWI.N,TWIN.O,TWLO.N,TWND_U.N,TWNK.O,TWO.N,TWO_A.N,TWO_B.N,TWO_C.N,TWO_D.N,TWO_E.N,TWOU.O,TWST.O,TWTR.N,TX.N,TXG.O,TXMD.O,TXN.O,TXRH.O,TXT.N,TY_.N,TYHT.O,TYL.N,TYME.O,TZAC.O,TZACU.O,TZOO.O,U.N,UA.N,UAA.N,UAL.O,UAMY.A,UAN.N,UAVS.A,UBA.N,UBCP.O,UBER.N,UBFO.O,UBOH.O,UBP.N,UBP_H.N,UBP_K.N,UBS.N,UBSI.O,UBX.O,UCBI.O,UCBIO.O,UCL.O,UCTT.O,UDR.N,UE.N,UEC.A,UEIC.O,UEPS.O,UFAB.A,UFCS.O,UFI.N,UFPI.O,UFPT.O,UFS.N,UG.O,UGI.N,UGP.N,UHAL.O,UHS.N,UHT.N,UI.N,UIHC.O,UIS.N,UL.N,ULBI.O,ULH.O,ULTA.O,UMBF.O,UMC.N,UMH.N,UMH_B.N,UMH_C.N,UMH_D.N,UMPQ.O,UMRX.O,UN.N,UNAM.O,UNB.O,UNF.N,UNFI.N,UNH.N,UNIT.O,UNM.N,UNP.N,UNTY.O,UNVR.N,UONE.O,UONEK.O,UPLD.O,UPS.N,UPWK.O,URBN.O,URG.A,URGN.O,URI.N,UROV.O,USAC.N,USAK.O,USAP.O,USAS.A,USAU.O,USB.N,USB_A.N,USB_H.N,USB_M.N,USB_O.N,USB_P.N,USCR.O,USDP.N,USEG.O,USFD.N,USIO.O,USLM.O,USM.N,USNA.N,USPH.N,USWS.O,USX.N,UTHR.O,UTI.N,UTL.N,UTMD.O,UTSI.O,UTZ.N,UUU.A,UUUU.A,UVE.N,UVSP.O,UVV.N,UXIN.O,V.N,VAC.N,VACQU.O,VALE.N,VALU.O,VAPO.N,VAR.N,VBFC.O,VBIV.O,VBLT.O,VBTX.O,VC.O,VCEL.O,VCNX.O,VCRA.N,VCTR.O,VCYT.O,VEC.N,VECO.O,VEDL.N,VEEV.N,VEL.N,VEON.O,VER.N,VER_F.N,VERB.O,VERI.O,VERO.O,VERT_U.N,VERU.O,VERX.O,VERY.O,VET.N,VFC.N,VFF.O,VG.O,VGR.N,VGZ.A,VHC.N,VHI.N,VIAC.O,VIACA.O,VIAO.N,VIAV.O,VICI.N,VICR.O,VIE.O,VIHAU.O,VIOT.O,VIPS.N,VIR.O,VIRC.O,VIRT.O,VISL.O,VIST.N,VITL.O,VIV.N,VIVE.O,VIVO.O,VJET.O,VKTX.O,VLGEA.O,VLO.N,VLRS.N,VLY.O,VLYPO.O,VLYPP.O,VMAC.O,VMACU.O,VMC.N,VMD.O,VMI.N,VMW.N,VNCE.N,VNDA.O,VNE.N,VNET.O,VNO.N,VNO_K.N,VNO_L.N,VNO_M.N,VNOM.O,VNRX.A,VNTR.N,VOC.N,VOD.O,VOLT.A,VOXX.O,VOYA.N,VOYA_B.N,VPG.N,VRA.O,VRAY.O,VRCA.O,VREX.O,VRM.O,VRME.O,VRNA.O,VRNS.O,VRNT.O,VRRM.O,VRS.N,VRSK.O,VRSN.O,VRT.N,VRTS.O,VRTU.O,VRTV.N,VRTX.O,VSAT.O,VSEC.O,VSH.N,VSLR.N,VST.N,VSTA.O,VSTM.O,VSTO.N,VTGN.O,VTNR.O,VTOL.N,VTR.N,VTRU.O,VTSI.O,VTVT.O,VUZI.O,VVI.N,VVNT.N,VVPR.O,VVV.N,VXRT.O,VYGR.O,VYNE.O,VZ.N,W.N,WAB.N,WABC.O,WAFD.O,WAFU.O,WAL.N,WASH.O,WAT.N,WATT.O,WB.O,WBA.O,WBAI.N,WBK.N,WBS.N,WBS_F.N,WBT.N,WCC.N,WCC_A.N,WCN.N,WD.N,WDAY.O,WDC.O,WDFC.O,WDR.N,WEC.N,WEI.N,WELL.N,WEN.O,WERN.O,WES.N,WETF.O,WEX.N,WEYS.O,WF.N,WFC.N,WFC_L.N,WFC_N.N,WFC_O.N,WFC_P.N,WFC_Q.N,WFC_R.N,WFC_T.N,WFC_V.N,WFC_W.N,WFC_X.N,WFC_Y.N,WFC_Z.N,WGO.N,WH.N,WHD.N,WHF.O,WHG.N,WHLM.O,WHLR.O,WHLRD.O,WHLRP.O,WHR.N,WIFI.O,WILC.O,WIMI.O,WINA.O,WING.O,WINS.O,WINT.O,WIRE.O,WISA.O,WIT.N,WIX.O,WK.N,WKEY.O,WKHS.O,WLDN.O,WLFC.O,WLK.N,WLKP.N,WLL.N,WLTW.O,WM.N,WMB.N,WMC.N,WMG.O,WMGI.O,WMK.N,WMS.N,WMT.N,WNC.N,WNEB.O,WNS.N,WOR.N,WORK.N,WORX.O,WOW.N,WPC.N,WPF.N,WPF_U.N,WPG.N,WPG_H.N,WPG_I.N,WPM.N,WPP.N,WPRT.O,WPX.N,WRB.N,WRE.N,WRI.N,WRK.N,WRLD.O,WRN.A,WRTC.O,WSBC.O,WSBCP.O,WSBF.O,WSC.O,WSFS.O,WSG.O,WSM.N,WSO.N,WSO_B.N,WSR.N,WST.N,WSTG.O,WSTL.O,WTBA.O,WTER.O,WTFC.O,WTFCM.O,WTFCP.O,WTI.N,WTM.N,WTRE.O,WTREP.O,WTRG.N,WTRH.O,WTS.N,WTT.A,WTTR.N,WU.N,WVE.O,WVFC.O,WVVI.O,WVVIP.O,WW.O,WWD.O,WWE.N,WWR.O,WWW.N,WY.N,WYND.N,WYNN.O,WYY.A,X.N,XAIR.O,XAN.N,XAN_C.N,XBIO.O,XBIT.O,XCUR.O,XEC.N,XEL.O,XELA.O,XELB.O,XENE.O,XENT.O,XERS.O,XFOR.O,XGN.O,XHR.N,XIN.N,XLNX.O,XLRN.O,XNCR.O,XNET.O,XOM.N,XOMA.O,XONE.O,XP.O,XPEL.O,XPER.O,XPEV.N,XPL.A,XPO.N,XRAY.O,XRX.N,XSPA.O,XTLB.O,XTNT.A,XXII.A,XYF.N,XYL.N,Y.N,YAC.N,YAC_U.N,YCBD.A,YCBD_A.A,YELP.N,YETI.N,YEXT.N,YGYI.O,YGYIP.O,YI.O,YIN.O,YJ.O,YMAB.O,YNDX.O,YORW.O,YPF.N,YRCW.O,YRD.N,YTEN.O,YTRA.O,YUM.N,YUMC.N,YVR.O,YY.O,Z.O,ZAGG.O,ZBH.N,ZBRA.O,ZCMD.O,ZDGE.A,ZEAL.O,ZEN.N,ZEUS.O,ZG.O,ZGNX.O,ZGYH.O,ZGYHU.O,ZI.O,ZION.O,ZIONN.O,ZIONO.O,ZIONP.O,ZIOP.O,ZIXI.O,ZKIN.O,ZLAB.O,ZM.O,ZNGA.O,ZNH.N,ZNTL.O,ZOM.A,ZS.O,ZSAN.O,ZTO.N,ZTS.N,ZUMZ.O,ZUO.N,ZVO.O,ZYME.N,ZYNE.O,ZYXI.O"
# symbol_list_choice=symbol_list_choice.split(',')
#
# temp=pd.DataFrame()
# for symbol in symbol_list_choice:
#     print(symbol)
#     data=datasearch1(symbol)
#     data.index=[symbol]
#     temp=pd.concat([temp,data],axis=0)
#
# temp.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\US_Quality\symbol_list_choice.csv',encoding='utf-8_sig')
#
# for i in range(len(temp)):
#     temp['symbol'][i]=temp.index[i][:-2]



# #将港股证券代码处理成00001.HK格式
# symbol_list=pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\HK_HSTECH\symbol_list.csv')
# symbol_list=symbol_list.set_index('symbol')
# symbol_list['symbol1']=symbol_list.index
# for symbol in symbol_list.index:
#     print(symbol)
#     symbol_list.loc[symbol,'symbol1'] = symbol_list.loc[symbol,'symbol1'][:-10]
#     symbol_list.loc[symbol,'symbol1']='0'*(5-len(symbol_list.loc[symbol,'symbol1']))+symbol_list.loc[symbol,'symbol1']+'.HK'
#
# symbol_list.to_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\HK_HSTECH\symbol_list.csv')


# #由文件名导出symbol_list
# import pandas as pd
# import numpy
# import os
# import warnings
# warnings.filterwarnings('ignore')
#
# #设置路径
# read_path = r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\data\hk_data\trading'
# os.chdir(read_path)
# csv_name_list = os.listdir()
# symbol_list=[]
# for csv_name in csv_name_list:
#     csv_name=csv_name[:-4]
#     symbol_list.append(csv_name)
# symbol_list=pd.DataFrame(symbol_list,columns=['symbol'])


# import pandas as pd
#
# data =pd.read_csv(r'D:\AQUMON\ThemeProject\aqumon-theme-portfolio\config\US_Quality\symbol_list.csv')
# data=data.dropna()
#
# #提取每个股票的行业信息
# import pandas as pd
# import numpy
# import os
# import warnings
# warnings.filterwarnings('ignore')

#设置路径
# read_path = r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/us_data/Quarterly Financial csv'
# os.chdir(read_path)
# csv_name_list = os.listdir()
# csv_name_list.remove('desktop.ini')
# symbol_list=pd.DataFrame(index=csv_name_list,columns=['symbol','gicsector','gicgroup','gicind','gicsubind','standard_ind'])
#
# for i in range(len(csv_name_list)):
#     csv_name=csv_name_list[i]
#     print(csv_name[:-4])
#     symbol_list['symbol'][i]=csv_name[:-4]
#     datain=pd.read_csv(read_path+'//'+csv_name)
#     gsector=datain['gsector'].dropna().drop_duplicates()
#     if len(gsector.values)>0:
#         symbol_list['gicsector'][i] =gsector.values[0]
#
#     ggroup=datain['ggroup'].dropna().drop_duplicates()
#     if len(ggroup.values) > 0:
#         symbol_list['gicgroup'][i] =ggroup.values[0]
#
#     gind=datain['gind'].dropna().drop_duplicates()
#     if len(gind.values) > 0:
#         symbol_list['gicind'][i] =gind.values[0]
#
#     gsubind=datain['gsubind'].dropna().drop_duplicates()
#     if len(gsubind.values) > 0:
#         symbol_list['gicsubind'][i] =gsubind.values[0]
#
#     sic=datain['sic'].dropna().drop_duplicates()
#     if len(sic.values) > 0:
#         symbol_list['standard_ind'][i] =sic.values[0]
#
# symbol_list.set_index('symbol',inplace=True)
# symbol_list=symbol_list.dropna()
# symbol_list.to_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/us_data/symbol_industry.csv')

# 处理港股财务数据
# import pandas as pd
# import numpy
# import os
# import warnings
# warnings.filterwarnings('ignore')
# import datetime
#
#
# # 设置路径
# read_path = r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/hk_data_raw_financial'
# save_path = r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/hk_data/financials'
# os.chdir(read_path)
# csv_name_list = os.listdir()
# # csv_name_list.remove('desktop.ini')
#
# def ANNOUNCEMENT_DT_processing(array):
#     array = str(int(array))
#     return array[0:4] + '/' + array[4:6] + '/' + array[6:8]
#
#
# na_list=[]
# for csv_name in csv_name_list:
#     # print(csv_name)
#     datain=pd.read_csv(read_path+'//'+csv_name)
#     if len(datain)>0:
#         datain.loc[:, 'date'] = pd.to_datetime(datain['date'], format='%Y/%m/%d', errors='ignore')
#         datain.index=datain.loc[:,'date']
#
#         dataout = pd.DataFrame(index=datain.index)
#         dataout.loc[:, 'ANNOUNCEMENT_DT'] = datain.loc[:, 'ANNOUNCEMENT_DT'].dropna().apply(ANNOUNCEMENT_DT_processing)
#
#         dataout.to_csv(save_path+'//'+csv_name)
#     else:
#         print(csv_name+' has no financial data')
#         na_list.append(csv_name[:-4])
# print(na_list)

#dm上导下港股交易数据
# import pandas as pd
# from datamaster import dm_client
# dm_client.start()
# import numpy as np
#
# symbol_list=pd.read_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/hk_data/symbol_list(3).csv')
# symbol_list=symbol_list['symbol'].tolist()
#
# column_names={
#     'close':'PX_LAST_RAW',
#     'low':'PX_LOW_RAW',
#     'open':'PX_OPEN_RAW',
#     'high':'PX_HIGH_RAW',
#     'volume':'PX_VOLUME_RAW',
#     'bbg_total_shares_outstanding':'EQY_SH_OUT_RAW',
#     'bbg_daily_total_return':'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS',
#     'bbg_mkt_cap':'MARKET_CAP'
# }
#
# for symbol in symbol_list:
#     print(symbol)
#     res=dm_client.historical(symbols=[symbol], start_date='2009-12-31', fields='open,high,low,close,volume,bbg_daily_total_return,bbg_total_shares_outstanding,bbg_mkt_cap')
#     res_output = pd.DataFrame(res['values'][symbol], columns=res['fields'])
#     res_output=res_output.rename(columns=column_names)
#     # print(res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'])
#     if len(res_output)>0:
#         res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS']=np.where(pd.isna(res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS']),
#                                                             res_output['PX_LAST_RAW']/res_output['PX_LAST_RAW'].shift()-1,res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'])
#         res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT']=(res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'].fillna(0)+1).cumprod()
#         res_output['adjust_factor']=res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT']/res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT'].iloc[-1]
#         res_output['PX_LAST']=res_output['PX_LAST_RAW'].iloc[-1]*res_output['adjust_factor']
#
#
#         res_output.to_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/hk_data/Trading'+'//'+symbol+'.csv',index=False)


# from choice_client import c
# import pandas as pd
#
# from retrying import retry
#
# @retry(stop_max_attempt_number=10)
# def datasearch1(symbol):
#     data = c.css(symbol, "CODE,NAME,LOTSIZE,VOLUNIT,PAR,GICS,GICSCODE,HKSE,GICSENG,SEHKENG","TradeDate=2020-11-02,ClassiFication=1")[symbol]
#     return data
#
#
#
#
# symbol_list_choice="00001.HK,00002.HK,00003.HK,00004.HK,00005.HK,00006.HK,00007.HK,00008.HK,00009.HK,00010.HK,00011.HK,00012.HK,00014.HK,00016.HK,00017.HK,00018.HK,00019.HK,00021.HK,00022.HK,00023.HK,00024.HK,00025.HK,00026.HK,00027.HK,00028.HK,00029.HK,00030.HK,00031.HK,00032.HK,00033.HK,00034.HK,00035.HK,00036.HK,00037.HK,00038.HK,00039.HK,00040.HK,00041.HK,00042.HK,00043.HK,00045.HK,00046.HK,00047.HK,00048.HK,00050.HK,00051.HK,00052.HK,00053.HK,00055.HK,00057.HK,00058.HK,00059.HK,00060.HK,00061.HK,00062.HK,00063.HK,00064.HK,00065.HK,00066.HK,00067.HK,00068.HK,00069.HK,00070.HK,00071.HK,00072.HK,00073.HK,00075.HK,00076.HK,00077.HK,00078.HK,00079.HK,00080.HK,00081.HK,00082.HK,00083.HK,00084.HK,00085.HK,00086.HK,00087.HK,00088.HK,00089.HK,00090.HK,00091.HK,00092.HK,00093.HK,00094.HK,00095.HK,00096.HK,00097.HK,00098.HK,00099.HK,00100.HK,00101.HK,00102.HK,00103.HK,00104.HK,00105.HK,00106.HK,00107.HK,00108.HK,00109.HK,00110.HK,00111.HK,00112.HK,00113.HK,00114.HK,00115.HK,00116.HK,00117.HK,00118.HK,00119.HK,00120.HK,00122.HK,00123.HK,00124.HK,00125.HK,00126.HK,00127.HK,00128.HK,00129.HK,00130.HK,00131.HK,00132.HK,00133.HK,00135.HK,00136.HK,00137.HK,00138.HK,00139.HK,00141.HK,00142.HK,00143.HK,00144.HK,00145.HK,00146.HK,00147.HK,00148.HK,00149.HK,00150.HK,00151.HK,00152.HK,00153.HK,00154.HK,00155.HK,00156.HK,00157.HK,00158.HK,00159.HK,00160.HK,00162.HK,00163.HK,00164.HK,00165.HK,00166.HK,00167.HK,00168.HK,00169.HK,00171.HK,00172.HK,00173.HK,00174.HK,00175.HK,00176.HK,00177.HK,00178.HK,00179.HK,00180.HK,00181.HK,00182.HK,00183.HK,00184.HK,00185.HK,00186.HK,00187.HK,00188.HK,00189.HK,00190.HK,00191.HK,00193.HK,00194.HK,00195.HK,00196.HK,00197.HK,00198.HK,00199.HK,00200.HK,00201.HK,00202.HK,00204.HK,00205.HK,00206.HK,00207.HK,00208.HK,00209.HK,00210.HK,00211.HK,00212.HK,00213.HK,00214.HK,00215.HK,00216.HK,00217.HK,00218.HK,00219.HK,00220.HK,00222.HK,00223.HK,00224.HK,00225.HK,00226.HK,00227.HK,00228.HK,00229.HK,00230.HK,00231.HK,00232.HK,00234.HK,00235.HK,00236.HK,00237.HK,00238.HK,00239.HK,00240.HK,00241.HK,00242.HK,00243.HK,00244.HK,00245.HK,00247.HK,00248.HK,00250.HK,00251.HK,00252.HK,00253.HK,00254.HK,00255.HK,00256.HK,00257.HK,00258.HK,00259.HK,00260.HK,00261.HK,00262.HK,00263.HK,00264.HK,00265.HK,00266.HK,00267.HK,00268.HK,00269.HK,00270.HK,00271.HK,00272.HK,00273.HK,00274.HK,00275.HK,00276.HK,00277.HK,00278.HK,00279.HK,00280.HK,00281.HK,00282.HK,00285.HK,00286.HK,00287.HK,00288.HK,00289.HK,00290.HK,00291.HK,00292.HK,00293.HK,00294.HK,00295.HK,00296.HK,00297.HK,00298.HK,00299.HK,00301.HK,00302.HK,00303.HK,00305.HK,00306.HK,00307.HK,00308.HK,00309.HK,00310.HK,00311.HK,00312.HK,00313.HK,00315.HK,00316.HK,00317.HK,00318.HK,00320.HK,00321.HK,00322.HK,00323.HK,00326.HK,00327.HK,00328.HK,00329.HK,00330.HK,00331.HK,00332.HK,00333.HK,00334.HK,00335.HK,00336.HK,00337.HK,00338.HK,00339.HK,00340.HK,00341.HK,00342.HK,00343.HK,00345.HK,00346.HK,00347.HK,00348.HK,00351.HK,00352.HK,00353.HK,00354.HK,00355.HK,00356.HK,00357.HK,00358.HK,00359.HK,00360.HK,00361.HK,00362.HK,00363.HK,00364.HK,00365.HK,00366.HK,00367.HK,00368.HK,00369.HK,00370.HK,00371.HK,00372.HK,00373.HK,00374.HK,00375.HK,00376.HK,00377.HK,00378.HK,00379.HK,00380.HK,00381.HK,00382.HK,00383.HK,00384.HK,00385.HK,00386.HK,00387.HK,00388.HK,00389.HK,00390.HK,00391.HK,00392.HK,00393.HK,00395.HK,00396.HK,00397.HK,00398.HK,00399.HK,00400.HK,00401.HK,00403.HK,00406.HK,00408.HK,00410.HK,00411.HK,00412.HK,00413.HK,00416.HK,00417.HK,00418.HK,00419.HK,00420.HK,00423.HK,00425.HK,00426.HK,00428.HK,00430.HK,00431.HK,00432.HK,00433.HK,00434.HK,00436.HK,00438.HK,00439.HK,00440.HK,00442.HK,00444.HK,00445.HK,00449.HK,00450.HK,00451.HK,00455.HK,00456.HK,00458.HK,00459.HK,00460.HK,00462.HK,00464.HK,00465.HK,00467.HK,00468.HK,00471.HK,00472.HK,00474.HK,00475.HK,00476.HK,00479.HK,00480.HK,00482.HK,00483.HK,00484.HK,00485.HK,00486.HK,00487.HK,00488.HK,00489.HK,00491.HK,00493.HK,00495.HK,00496.HK,00497.HK,00498.HK,00499.HK,00500.HK,00503.HK,00505.HK,00506.HK,00508.HK,00509.HK,00510.HK,00511.HK,00512.HK,00513.HK,00515.HK,00517.HK,00518.HK,00519.HK,00520.HK,00521.HK,00522.HK,00524.HK,00525.HK,00526.HK,00527.HK,00528.HK,00529.HK,00530.HK,00531.HK,00532.HK,00533.HK,00535.HK,00536.HK,00538.HK,00539.HK,00540.HK,00542.HK,00543.HK,00544.HK,00546.HK,00547.HK,00548.HK,00550.HK,00551.HK,00552.HK,00553.HK,00554.HK,00555.HK,00556.HK,00557.HK,00558.HK,00559.HK,00560.HK,00563.HK,00564.HK,00565.HK,00567.HK,00568.HK,00570.HK,00571.HK,00572.HK,00573.HK,00574.HK,00575.HK,00576.HK,00577.HK,00578.HK,00579.HK,00580.HK,00581.HK,00582.HK,00583.HK,00585.HK,00586.HK,00587.HK,00588.HK,00589.HK,00590.HK,00591.HK,00592.HK,00593.HK,00595.HK,00596.HK,00598.HK,00599.HK,00600.HK,00601.HK,00602.HK,00603.HK,00604.HK,00605.HK,00607.HK,00608.HK,00609.HK,00610.HK,00611.HK,00612.HK,00613.HK,00616.HK,00617.HK,00618.HK,00619.HK,00620.HK,00621.HK,00622.HK,00623.HK,00626.HK,00627.HK,00628.HK,00629.HK,00630.HK,00631.HK,00632.HK,00633.HK,00635.HK,00636.HK,00637.HK,00638.HK,00639.HK,00640.HK,00641.HK,00643.HK,00645.HK,00646.HK,00648.HK,00650.HK,00651.HK,00653.HK,00655.HK,00656.HK,00657.HK,00658.HK,00659.HK,00660.HK,00661.HK,00662.HK,00663.HK,00665.HK,00666.HK,00667.HK,00668.HK,00669.HK,00670.HK,00672.HK,00673.HK,00674.HK,00675.HK,00676.HK,00677.HK,00678.HK,00679.HK,00680.HK,00681.HK,00682.HK,00683.HK,00684.HK,00685.HK,00686.HK,00687.HK,00688.HK,00689.HK,00690.HK,00691.HK,00693.HK,00694.HK,00695.HK,00696.HK,00697.HK,00698.HK,00699.HK,00700.HK,00701.HK,00702.HK,00703.HK,00704.HK,00706.HK,00707.HK,00708.HK,00709.HK,00710.HK,00711.HK,00712.HK,00713.HK,00715.HK,00716.HK,00717.HK,00718.HK,00719.HK,00720.HK,00721.HK,00722.HK,00723.HK,00724.HK,00725.HK,00726.HK,00727.HK,00728.HK,00729.HK,00730.HK,00731.HK,00732.HK,00733.HK,00736.HK,00737.HK,00738.HK,00743.HK,00745.HK,00746.HK,00747.HK,00750.HK,00751.HK,00752.HK,00753.HK,00754.HK,00755.HK,00756.HK,00757.HK,00758.HK,00759.HK,00760.HK,00762.HK,00763.HK,00764.HK,00765.HK,00766.HK,00767.HK,00768.HK,00769.HK,00770.HK,00771.HK,00772.HK,00775.HK,00776.HK,00777.HK,00780.HK,00784.HK,00787.HK,00788.HK,00789.HK,00794.HK,00797.HK,00798.HK,00799.HK,00800.HK,00802.HK,00803.HK,00804.HK,00806.HK,00807.HK,00809.HK,00810.HK,00811.HK,00812.HK,00813.HK,00814.HK,00815.HK,00817.HK,00818.HK,00819.HK,00821.HK,00822.HK,00825.HK,00826.HK,00827.HK,00828.HK,00829.HK,00830.HK,00831.HK,00832.HK,00833.HK,00834.HK,00836.HK,00837.HK,00838.HK,00839.HK,00840.HK,00841.HK,00842.HK,00844.HK,00845.HK,00846.HK,00848.HK,00850.HK,00851.HK,00852.HK,00853.HK,00854.HK,00855.HK,00856.HK,00857.HK,00858.HK,00859.HK,00860.HK,00861.HK,00862.HK,00863.HK,00864.HK,00865.HK,00866.HK,00867.HK,00868.HK,00869.HK,00871.HK,00872.HK,00873.HK,00874.HK,00875.HK,00876.HK,00878.HK,00880.HK,00881.HK,00882.HK,00883.HK,00884.HK,00885.HK,00886.HK,00887.HK,00888.HK,00889.HK,00891.HK,00893.HK,00894.HK,00895.HK,00896.HK,00897.HK,00898.HK,00899.HK,00900.HK,00901.HK,00902.HK,00904.HK,00905.HK,00906.HK,00907.HK,00908.HK,00909.HK,00910.HK,00911.HK,00912.HK,00913.HK,00914.HK,00915.HK,00916.HK,00918.HK,00919.HK,00921.HK,00922.HK,00923.HK,00924.HK,00925.HK,00926.HK,00927.HK,00928.HK,00929.HK,00931.HK,00932.HK,00934.HK,00935.HK,00936.HK,00938.HK,00939.HK,00941.HK,00943.HK,00945.HK,00947.HK,00948.HK,00950.HK,00951.HK,00952.HK,00953.HK,00954.HK,00956.HK,00959.HK,00960.HK,00966.HK,00967.HK,00968.HK,00969.HK,00970.HK,00973.HK,00974.HK,00975.HK,00976.HK,00978.HK,00979.HK,00980.HK,00981.HK,00982.HK,00983.HK,00984.HK,00985.HK,00986.HK,00987.HK,00988.HK,00989.HK,00990.HK,00991.HK,00992.HK,00993.HK,00994.HK,00995.HK,00996.HK,00997.HK,00998.HK,00999.HK,01000.HK,01001.HK,01002.HK,01003.HK,01004.HK,01005.HK,01006.HK,01007.HK,01008.HK,01009.HK,01010.HK,01011.HK,01013.HK,01019.HK,01020.HK,01022.HK,01023.HK,01025.HK,01026.HK,01027.HK,01028.HK,01029.HK,01030.HK,01031.HK,01033.HK,01034.HK,01036.HK,01037.HK,01038.HK,01039.HK,01041.HK,01043.HK,01044.HK,01045.HK,01046.HK,01047.HK,01049.HK,01050.HK,01051.HK,01052.HK,01053.HK,01055.HK,01057.HK,01058.HK,01059.HK,01060.HK,01061.HK,01062.HK,01063.HK,01064.HK,01065.HK,01066.HK,01068.HK,01069.HK,01070.HK,01071.HK,01072.HK,01073.HK,01075.HK,01076.HK,01079.HK,01080.HK,01082.HK,01083.HK,01084.HK,01085.HK,01086.HK,01087.HK,01088.HK,01089.HK,01090.HK,01091.HK,01093.HK,01094.HK,01096.HK,01097.HK,01098.HK,01099.HK,01100.HK,01101.HK,01102.HK,01103.HK,01104.HK,01105.HK,01106.HK,01107.HK,01108.HK,01109.HK,01110.HK,01111.HK,01112.HK,01113.HK,01114.HK,01115.HK,01116.HK,01117.HK,01118.HK,01119.HK,01120.HK,01121.HK,01122.HK,01123.HK,01124.HK,01125.HK,01126.HK,01127.HK,01128.HK,01129.HK,01130.HK,01131.HK,01132.HK,01133.HK,01134.HK,01137.HK,01138.HK,01139.HK,01140.HK,01141.HK,01142.HK,01143.HK,01145.HK,01146.HK,01147.HK,01148.HK,01150.HK,01152.HK,01155.HK,01156.HK,01157.HK,01158.HK,01159.HK,01160.HK,01161.HK,01162.HK,01163.HK,01164.HK,01165.HK,01166.HK,01168.HK,01169.HK,01170.HK,01171.HK,01172.HK,01173.HK,01175.HK,01176.HK,01177.HK,01178.HK,01179.HK,01180.HK,01181.HK,01182.HK,01183.HK,01184.HK,01185.HK,01186.HK,01188.HK,01189.HK,01190.HK,01191.HK,01192.HK,01193.HK,01194.HK,01195.HK,01196.HK,01198.HK,01199.HK,01200.HK,01201.HK,01202.HK,01203.HK,01205.HK,01206.HK,01207.HK,01208.HK,01210.HK,01211.HK,01212.HK,01213.HK,01215.HK,01216.HK,01217.HK,01218.HK,01219.HK,01220.HK,01221.HK,01222.HK,01223.HK,01224.HK,01225.HK,01226.HK,01227.HK,01229.HK,01230.HK,01231.HK,01232.HK,01233.HK,01234.HK,01235.HK,01237.HK,01238.HK,01239.HK,01240.HK,01241.HK,01243.HK,01245.HK,01246.HK,01247.HK,01249.HK,01250.HK,01251.HK,01252.HK,01253.HK,01255.HK,01257.HK,01258.HK,01259.HK,01260.HK,01262.HK,01263.HK,01265.HK,01266.HK,01268.HK,01269.HK,01270.HK,01271.HK,01272.HK,01273.HK,01277.HK,01278.HK,01280.HK,01281.HK,01282.HK,01283.HK,01285.HK,01286.HK,01288.HK,01289.HK,01290.HK,01292.HK,01293.HK,01296.HK,01297.HK,01298.HK,01299.HK,01300.HK,01301.HK,01302.HK,01303.HK,01305.HK,01308.HK,01310.HK,01312.HK,01313.HK,01314.HK,01315.HK,01316.HK,01317.HK,01319.HK,01321.HK,01323.HK,01326.HK,01327.HK,01328.HK,01329.HK,01330.HK,01332.HK,01333.HK,01335.HK,01336.HK,01337.HK,01338.HK,01339.HK,01340.HK,01341.HK,01343.HK,01345.HK,01346.HK,01347.HK,01348.HK,01349.HK,01353.HK,01355.HK,01357.HK,01358.HK,01359.HK,01360.HK,01361.HK,01362.HK,01363.HK,01365.HK,01366.HK,01367.HK,01368.HK,01369.HK,01370.HK,01371.HK,01372.HK,01373.HK,01375.HK,01376.HK,01378.HK,01380.HK,01381.HK,01382.HK,01383.HK,01385.HK,01386.HK,01387.HK,01388.HK,01389.HK,01393.HK,01395.HK,01396.HK,01397.HK,01398.HK,01399.HK,01400.HK,01401.HK,01402.HK,01408.HK,01410.HK,01412.HK,01415.HK,01416.HK,01417.HK,01418.HK,01419.HK,01420.HK,01421.HK,01425.HK,01427.HK,01428.HK,01429.HK,01430.HK,01431.HK,01432.HK,01433.HK,01439.HK,01442.HK,01443.HK,01446.HK,01447.HK,01448.HK,01449.HK,01450.HK,01451.HK,01452.HK,01455.HK,01456.HK,01458.HK,01459.HK,01460.HK,01461.HK,01462.HK,01463.HK,01466.HK,01468.HK,01469.HK,01470.HK,01472.HK,01475.HK,01476.HK,01477.HK,01478.HK,01480.HK,01483.HK,01486.HK,01488.HK,01492.HK,01495.HK,01496.HK,01498.HK,01499.HK,01500.HK,01501.HK,01502.HK,01508.HK,01509.HK,01513.HK,01515.HK,01518.HK,01520.HK,01521.HK,01522.HK,01523.HK,01525.HK,01526.HK,01527.HK,01528.HK,01529.HK,01530.HK,01532.HK,01533.HK,01536.HK,01538.HK,01539.HK,01540.HK,01542.HK,01543.HK,01545.HK,01546.HK,01547.HK,01548.HK,01549.HK,01551.HK,01552.HK,01553.HK,01555.HK,01556.HK,01557.HK,01558.HK,01559.HK,01560.HK,01561.HK,01563.HK,01565.HK,01566.HK,01568.HK,01569.HK,01570.HK,01571.HK,01572.HK,01573.HK,01575.HK,01576.HK,01577.HK,01578.HK,01579.HK,01580.HK,01581.HK,01582.HK,01583.HK,01585.HK,01586.HK,01587.HK,01588.HK,01589.HK,01591.HK,01592.HK,01593.HK,01596.HK,01597.HK,01598.HK,01599.HK,01600.HK,01601.HK,01606.HK,01608.HK,01609.HK,01610.HK,01611.HK,01612.HK,01613.HK,01615.HK,01616.HK,01617.HK,01618.HK,01620.HK,01621.HK,01622.HK,01623.HK,01626.HK,01627.HK,01628.HK,01629.HK,01630.HK,01631.HK,01632.HK,01633.HK,01635.HK,01636.HK,01637.HK,01638.HK,01639.HK,01640.HK,01645.HK,01647.HK,01649.HK,01650.HK,01651.HK,01652.HK,01653.HK,01655.HK,01656.HK,01657.HK,01658.HK,01659.HK,01660.HK,01661.HK,01662.HK,01663.HK,01665.HK,01666.HK,01667.HK,01668.HK,01669.HK,01671.HK,01672.HK,01673.HK,01675.HK,01676.HK,01678.HK,01679.HK,01680.HK,01681.HK,01682.HK,01683.HK,01685.HK,01686.HK,01689.HK,01690.HK,01691.HK,01692.HK,01693.HK,01695.HK,01696.HK,01697.HK,01698.HK,01699.HK,01701.HK,01702.HK,01703.HK,01705.HK,01706.HK,01707.HK,01708.HK,01709.HK,01710.HK,01711.HK,01712.HK,01713.HK,01715.HK,01716.HK,01717.HK,01718.HK,01719.HK,01720.HK,01721.HK,01722.HK,01723.HK,01725.HK,01726.HK,01727.HK,01728.HK,01729.HK,01730.HK,01731.HK,01732.HK,01733.HK,01735.HK,01736.HK,01737.HK,01738.HK,01739.HK,01740.HK,01741.HK,01742.HK,01743.HK,01745.HK,01746.HK,01747.HK,01748.HK,01749.HK,01750.HK,01751.HK,01752.HK,01753.HK,01755.HK,01756.HK,01757.HK,01758.HK,01759.HK,01760.HK,01761.HK,01762.HK,01763.HK,01765.HK,01766.HK,01767.HK,01769.HK,01771.HK,01772.HK,01773.HK,01775.HK,01776.HK,01777.HK,01778.HK,01780.HK,01781.HK,01782.HK,01783.HK,01785.HK,01786.HK,01787.HK,01788.HK,01789.HK,01790.HK,01792.HK,01793.HK,01796.HK,01797.HK,01798.HK,01799.HK,01800.HK,01801.HK,01802.HK,01803.HK,01806.HK,01808.HK,01809.HK,01810.HK,01811.HK,01812.HK,01813.HK,01815.HK,01816.HK,01817.HK,01818.HK,01820.HK,01821.HK,01822.HK,01823.HK,01825.HK,01826.HK,01827.HK,01829.HK,01830.HK,01831.HK,01832.HK,01833.HK,01835.HK,01836.HK,01837.HK,01838.HK,01839.HK,01841.HK,01842.HK,01843.HK,01845.HK,01846.HK,01847.HK,01848.HK,01849.HK,01850.HK,01851.HK,01853.HK,01854.HK,01856.HK,01857.HK,01858.HK,01859.HK,01860.HK,01861.HK,01862.HK,01863.HK,01865.HK,01866.HK,01867.HK,01868.HK,01869.HK,01870.HK,01871.HK,01872.HK,01873.HK,01875.HK,01876.HK,01877.HK,01878.HK,01882.HK,01883.HK,01884.HK,01885.HK,01886.HK,01888.HK,01889.HK,01890.HK,01891.HK,01894.HK,01895.HK,01896.HK,01897.HK,01898.HK,01899.HK,01900.HK,01901.HK,01902.HK,01903.HK,01905.HK,01906.HK,01907.HK,01908.HK,01909.HK,01910.HK,01911.HK,01912.HK,01913.HK,01915.HK,01916.HK,01917.HK,01918.HK,01919.HK,01920.HK,01921.HK,01922.HK,01925.HK,01928.HK,01929.HK,01930.HK,01931.HK,01932.HK,01933.HK,01935.HK,01936.HK,01937.HK,01938.HK,01939.HK,01941.HK,01942.HK,01943.HK,01949.HK,01950.HK,01951.HK,01952.HK,01953.HK,01955.HK,01957.HK,01958.HK,01959.HK,01960.HK,01961.HK,01962.HK,01963.HK,01966.HK,01967.HK,01968.HK,01969.HK,01970.HK,01971.HK,01972.HK,01975.HK,01977.HK,01978.HK,01979.HK,01980.HK,01981.HK,01982.HK,01983.HK,01985.HK,01986.HK,01987.HK,01988.HK,01989.HK,01990.HK,01991.HK,01992.HK,01993.HK,01995.HK,01996.HK,01997.HK,01998.HK,01999.HK,02000.HK,02001.HK,02002.HK,02003.HK,02005.HK,02006.HK,02007.HK,02008.HK,02009.HK,02010.HK,02011.HK,02012.HK,02013.HK,02014.HK,02016.HK,02017.HK,02018.HK,02019.HK,02020.HK,02022.HK,02023.HK,02025.HK,02028.HK,02030.HK,02031.HK,02033.HK,02038.HK,02039.HK,02048.HK,02051.HK,02057.HK,02060.HK,02066.HK,02068.HK,02078.HK,02080.HK,02083.HK,02086.HK,02088.HK,02096.HK,02098.HK,02099.HK,02100.HK,02101.HK,02102.HK,02103.HK,02107.HK,02108.HK,02111.HK,02112.HK,02113.HK,02115.HK,02116.HK,02118.HK,02119.HK,02120.HK,02122.HK,02123.HK,02128.HK,02130.HK,02132.HK,02133.HK,02136.HK,02138.HK,02139.HK,02163.HK,02166.HK,02168.HK,02169.HK,02178.HK,02180.HK,02181.HK,02182.HK,02183.HK,02186.HK,02188.HK,02189.HK,02193.HK,02196.HK,02198.HK,02199.HK,02202.HK,02203.HK,02208.HK,02211.HK,02212.HK,02213.HK,02218.HK,02221.HK,02222.HK,02223.HK,02225.HK,02226.HK,02227.HK,02228.HK,02230.HK,02231.HK,02232.HK,02233.HK,02236.HK,02238.HK,02239.HK,02255.HK,02258.HK,02262.HK,02263.HK,02266.HK,02268.HK,02269.HK,02277.HK,02278.HK,02280.HK,02281.HK,02282.HK,02283.HK,02286.HK,02288.HK,02289.HK,02292.HK,02293.HK,02296.HK,02298.HK,02299.HK,02300.HK,02302.HK,02303.HK,02307.HK,02308.HK,02309.HK,02310.HK,02312.HK,02313.HK,02314.HK,02317.HK,02318.HK,02319.HK,02320.HK,02322.HK,02323.HK,02324.HK,02326.HK,02327.HK,02328.HK,02329.HK,02330.HK,02331.HK,02333.HK,02336.HK,02337.HK,02338.HK,02339.HK,02340.HK,02341.HK,02342.HK,02343.HK,02345.HK,02346.HK,02348.HK,02349.HK,02355.HK,02356.HK,02357.HK,02358.HK,02359.HK,02360.HK,02362.HK,02363.HK,02366.HK,02368.HK,02369.HK,02371.HK,02377.HK,02378.HK,02379.HK,02380.HK,02381.HK,02382.HK,02383.HK,02386.HK,02388.HK,02389.HK,02393.HK,02398.HK,02399.HK,02400.HK,02448.HK,02488.HK,02500.HK,02528.HK,02552.HK,02558.HK,02588.HK,02600.HK,02601.HK,02606.HK,02607.HK,02608.HK,02611.HK,02616.HK,02623.HK,02628.HK,02633.HK,02638.HK,02660.HK,02662.HK,02663.HK,02666.HK,02668.HK,02669.HK,02678.HK,02680.HK,02682.HK,02683.HK,02686.HK,02688.HK,02689.HK,02696.HK,02698.HK,02699.HK,02700.HK,02708.HK,02718.HK,02722.HK,02727.HK,02728.HK,02738.HK,02768.HK,02772.HK,02777.HK,02779.HK,02788.HK,02789.HK,02798.HK,02799.HK,02858.HK,02863.HK,02866.HK,02868.HK,02869.HK,02877.HK,02878.HK,02880.HK,02882.HK,02883.HK,02885.HK,02886.HK,02888.HK,02892.HK,02898.HK,02899.HK,02950.HK,02951.HK,02952.HK,02954.HK,02955.HK,02956.HK,02957.HK,03300.HK,03301.HK,03302.HK,03303.HK,03306.HK,03308.HK,03309.HK,03311.HK,03313.HK,03315.HK,03316.HK,03318.HK,03319.HK,03320.HK,03321.HK,03322.HK,03323.HK,03326.HK,03328.HK,03329.HK,03330.HK,03331.HK,03332.HK,03333.HK,03335.HK,03336.HK,03337.HK,03339.HK,03344.HK,03347.HK,03348.HK,03358.HK,03360.HK,03363.HK,03366.HK,03368.HK,03369.HK,03377.HK,03378.HK,03380.HK,03382.HK,03383.HK,03389.HK,03390.HK,03393.HK,03395.HK,03396.HK,03398.HK,03399.HK,03600.HK,03601.HK,03603.HK,03606.HK,03608.HK,03613.HK,03616.HK,03618.HK,03623.HK,03626.HK,03628.HK,03633.HK,03636.HK,03638.HK,03639.HK,03662.HK,03663.HK,03666.HK,03668.HK,03669.HK,03678.HK,03680.HK,03681.HK,03683.HK,03686.HK,03688.HK,03689.HK,03690.HK,03692.HK,03698.HK,03699.HK,03700.HK,03708.HK,03709.HK,03718.HK,03728.HK,03737.HK,03738.HK,03759.HK,03768.HK,03773.HK,03778.HK,03788.HK,03789.HK,03798.HK,03799.HK,03800.HK,03808.HK,03813.HK,03816.HK,03818.HK,03822.HK,03828.HK,03830.HK,03833.HK,03836.HK,03838.HK,03839.HK,03848.HK,03860.HK,03866.HK,03868.HK,03869.HK,03877.HK,03878.HK,03882.HK,03883.HK,03886.HK,03888.HK,03889.HK,03893.HK,03898.HK,03899.HK,03900.HK,03903.HK,03908.HK,03913.HK,03918.HK,03919.HK,03928.HK,03933.HK,03938.HK,03939.HK,03948.HK,03958.HK,03963.HK,03968.HK,03969.HK,03978.HK,03983.HK,03988.HK,03989.HK,03990.HK,03991.HK,03992.HK,03993.HK,03996.HK,03997.HK,03998.HK,03999.HK,04332.HK,04333.HK,04335.HK,04336.HK,04337.HK,04338.HK,06030.HK,06033.HK,06036.HK,06038.HK,06049.HK,06055.HK,06058.HK,06060.HK,06063.HK,06066.HK,06068.HK,06069.HK,06078.HK,06080.HK,06083.HK,06088.HK,06090.HK,06093.HK,06098.HK,06099.HK,06100.HK,06108.HK,06110.HK,06111.HK,06113.HK,06116.HK,06117.HK,06118.HK,06119.HK,06122.HK,06123.HK,06128.HK,06133.HK,06136.HK,06138.HK,06158.HK,06160.HK,06161.HK,06162.HK,06163.HK,06166.HK,06168.HK,06169.HK,06178.HK,06182.HK,06183.HK,06185.HK,06186.HK,06188.HK,06189.HK,06190.HK,06193.HK,06196.HK,06198.HK,06199.HK,06288.HK,06805.HK,06806.HK,06808.HK,06811.HK,06812.HK,06816.HK,06818.HK,06819.HK,06820.HK,06822.HK,06823.HK,06826.HK,06828.HK,06829.HK,06830.HK,06833.HK,06836.HK,06837.HK,06838.HK,06839.HK,06855.HK,06858.HK,06860.HK,06862.HK,06865.HK,06866.HK,06868.HK,06869.HK,06877.HK,06878.HK,06880.HK,06881.HK,06882.HK,06885.HK,06886.HK,06888.HK,06889.HK,06890.HK,06893.HK,06896.HK,06898.HK,06899.HK,06908.HK,06918.HK,06919.HK,06928.HK,06933.HK,06958.HK,06966.HK,06968.HK,06969.HK,06978.HK,06988.HK,06989.HK,06998.HK,08001.HK,08003.HK,08005.HK,08006.HK,08007.HK,08009.HK,08011.HK,08013.HK,08017.HK,08018.HK,08019.HK,08020.HK,08021.HK,08022.HK,08023.HK,08025.HK,08026.HK,08027.HK,08028.HK,08029.HK,08030.HK,08031.HK,08032.HK,08033.HK,08035.HK,08036.HK,08037.HK,08039.HK,08040.HK,08041.HK,08042.HK,08043.HK,08045.HK,08047.HK,08048.HK,08049.HK,08050.HK,08051.HK,08052.HK,08053.HK,08055.HK,08056.HK,08057.HK,08059.HK,08060.HK,08062.HK,08063.HK,08065.HK,08066.HK,08067.HK,08069.HK,08070.HK,08071.HK,08072.HK,08073.HK,08075.HK,08076.HK,08078.HK,08079.HK,08080.HK,08081.HK,08082.HK,08083.HK,08086.HK,08087.HK,08088.HK,08089.HK,08090.HK,08091.HK,08092.HK,08093.HK,08095.HK,08096.HK,08098.HK,08100.HK,08101.HK,08103.HK,08106.HK,08107.HK,08108.HK,08109.HK,08111.HK,08112.HK,08113.HK,08115.HK,08116.HK,08117.HK,08118.HK,08119.HK,08120.HK,08121.HK,08123.HK,08125.HK,08126.HK,08128.HK,08130.HK,08131.HK,08132.HK,08133.HK,08135.HK,08136.HK,08137.HK,08139.HK,08140.HK,08143.HK,08146.HK,08147.HK,08148.HK,08149.HK,08150.HK,08151.HK,08152.HK,08153.HK,08155.HK,08156.HK,08158.HK,08159.HK,08160.HK,08161.HK,08162.HK,08163.HK,08165.HK,08166.HK,08167.HK,08168.HK,08169.HK,08170.HK,08171.HK,08172.HK,08173.HK,08175.HK,08176.HK,08178.HK,08179.HK,08181.HK,08186.HK,08187.HK,08188.HK,08189.HK,08190.HK,08191.HK,08192.HK,08193.HK,08195.HK,08196.HK,08198.HK,08200.HK,08201.HK,08202.HK,08203.HK,08205.HK,08206.HK,08207.HK,08208.HK,08210.HK,08211.HK,08213.HK,08215.HK,08216.HK,08217.HK,08218.HK,08219.HK,08220.HK,08221.HK,08222.HK,08223.HK,08225.HK,08226.HK,08227.HK,08228.HK,08229.HK,08231.HK,08232.HK,08235.HK,08236.HK,08237.HK,08238.HK,08239.HK,08241.HK,08242.HK,08245.HK,08246.HK,08247.HK,08249.HK,08250.HK,08255.HK,08256.HK,08257.HK,08258.HK,08259.HK,08260.HK,08262.HK,08265.HK,08266.HK,08267.HK,08268.HK,08269.HK,08270.HK,08271.HK,08272.HK,08275.HK,08277.HK,08279.HK,08280.HK,08281.HK,08282.HK,08283.HK,08285.HK,08286.HK,08287.HK,08290.HK,08291.HK,08292.HK,08293.HK,08295.HK,08296.HK,08297.HK,08299.HK,08300.HK,08301.HK,08305.HK,08307.HK,08308.HK,08309.HK,08310.HK,08311.HK,08313.HK,08315.HK,08316.HK,08317.HK,08319.HK,08320.HK,08321.HK,08325.HK,08326.HK,08328.HK,08329.HK,08331.HK,08333.HK,08337.HK,08340.HK,08341.HK,08346.HK,08347.HK,08348.HK,08349.HK,08350.HK,08351.HK,08353.HK,08356.HK,08357.HK,08360.HK,08362.HK,08363.HK,08365.HK,08366.HK,08367.HK,08368.HK,08370.HK,08371.HK,08372.HK,08373.HK,08375.HK,08377.HK,08379.HK,08383.HK,08385.HK,08391.HK,08392.HK,08395.HK,08400.HK,08401.HK,08402.HK,08403.HK,08405.HK,08406.HK,08411.HK,08412.HK,08413.HK,08416.HK,08417.HK,08418.HK,08419.HK,08420.HK,08422.HK,08423.HK,08425.HK,08426.HK,08427.HK,08428.HK,08429.HK,08430.HK,08431.HK,08432.HK,08436.HK,08437.HK,08439.HK,08441.HK,08445.HK,08446.HK,08447.HK,08448.HK,08450.HK,08451.HK,08452.HK,08455.HK,08456.HK,08460.HK,08462.HK,08465.HK,08471.HK,08472.HK,08473.HK,08475.HK,08476.HK,08479.HK,08480.HK,08481.HK,08482.HK,08483.HK,08485.HK,08487.HK,08490.HK,08491.HK,08493.HK,08495.HK,08496.HK,08500.HK,08501.HK,08502.HK,08506.HK,08507.HK,08509.HK,08510.HK,08511.HK,08512.HK,08513.HK,08516.HK,08519.HK,08521.HK,08523.HK,08525.HK,08526.HK,08527.HK,08532.HK,08535.HK,08536.HK,08537.HK,08540.HK,08545.HK,08547.HK,08572.HK,08601.HK,08603.HK,08606.HK,08607.HK,08609.HK,08611.HK,08612.HK,08613.HK,08616.HK,08617.HK,08619.HK,08620.HK,08621.HK,08622.HK,08623.HK,08627.HK,08631.HK,08635.HK,08645.HK,08646.HK,08657.HK,08659.HK,08668.HK,09616.HK,09618.HK,09633.HK,09668.HK,09677.HK,09688.HK,09698.HK,09900.HK,09906.HK,09908.HK,09909.HK,09911.HK,09913.HK,09916.HK,09918.HK,09919.HK,09922.HK,09923.HK,09926.HK,09928.HK,09929.HK,09933.HK,09936.HK,09938.HK,09939.HK,09958.HK,09966.HK,09968.HK,09969.HK,09977.HK,09978.HK,09979.HK,09983.HK,09986.HK,09987.HK,09988.HK,09989.HK,09990.HK,09991.HK,09993.HK,09996.HK,09997.HK,09998.HK,09999.HK,80737.HK"
# symbol_list_choice=symbol_list_choice.split(',')
# temp=pd.DataFrame()
# for symbol in symbol_list_choice:
#     print(symbol)
#     data=datasearch1(symbol)
#     data.index=[symbol]
#     temp=pd.concat([temp, data],axis=0)
#     temp.to_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/hk_data/HK Stock List_choice_industry.csv',encoding='utf-8_sig')

#港股上导下财务数据
# import pandas as pd
# from datamaster import dm_client
# dm_client.start()
# import numpy as np
#
# symbol_list=pd.read_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/hk_data/symbol_list.csv')
# symbol_list=symbol_list['symbol'].tolist()
#
# column_names={
#     'close':'PX_LAST_RAW',
#     'low':'PX_LOW_RAW',
#     'open':'PX_OPEN_RAW',
#     'high':'PX_HIGH_RAW',
#     'volume':'PX_VOLUME_RAW',
#     'bbg_total_shares_outstanding':'EQY_SH_OUT_RAW',
#     'bbg_daily_total_return':'DAY_TO_DAY_TOT_RETURN_GROSS_DVDS',
#     'bbg_mkt_cap':'MARKET_CAP'
# }
#
# for symbol in symbol_list:
#     print(symbol)
#     res=dm_client.historical(symbols=[symbol], start_date='2009-12-31', fields='open,high,low,close,volume,bbg_daily_total_return,bbg_total_shares_outstanding,bbg_mkt_cap')
#     res_output = pd.DataFrame(res['values'][symbol], columns=res['fields'])
#     res_output=res_output.rename(columns=column_names)
#     # print(res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'])
#     if len(res_output)>0:
#         res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS']=np.where(pd.isna(res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS']),
#                                                             res_output['PX_LAST_RAW']/res_output['PX_LAST_RAW'].shift()-1,res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'])
#         res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT']=(res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS'].fillna(0)+1).cumprod()
#         res_output['adjust_factor']=res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT']/res_output['DAY_TO_DAY_TOT_RETURN_GROSS_DVDS_PRODUCT'].iloc[-1]
#         res_output['PX_LAST']=res_output['PX_LAST_RAW'].iloc[-1]*res_output['adjust_factor']


        # res_output.to_csv(r'/Users/liyang/Desktop/AQUMON/aqumon-theme-portfolio/data/hk_data/Trading'+'//'+symbol+'.csv',index=False)


import pandas as pd
import os
import json
from datamaster import dm_client

read_path1 = r'/Users/lizhirui/PycharmProjects/aqumon-theme-portfolio-quantcycle/data/cn_data/financial_monthly_data'
save_path = r'/Users/lizhirui/PycharmProjects/aqumon-theme-portfolio-quantcycle/data/12_31_data/cn_data/financial_monthly_data'
os.chdir(read_path1)
csv_name_list1 = os.listdir()
dm_client.start()
start_date = '1999-12-31'
for csv_name in csv_name_list1:
    data_old = pd.read_csv(os.path.join(read_path1, csv_name), index_col=0, parse_dates=[0])
    try:
        symbol=csv_name[:-4]
        print(symbol)
        tmp = pd.DataFrame(
            columns=['date','bps_adjust','lt_borrow','qfa_eps',
                     'qfa_net_cash_flows_oper_act','qfa_net_profit_is','qfa_opprofit',
                     'qfa_tot_oper_rev','tot_assets','tot_equity','tot_liab'])

        data = dm_client.historical(symbols=symbol, start_date=start_date,
                                    fields=['choice_bpsadjust','choice_ltborrow','choice_qfa_eps',
                     'choice_qfa_net_cash_flows_oper_act','choice_qfa_net_profit_is','choice_qfa_opprofit',
                     'choice_qfa_tot_oper_rev','choice_tot_assets','choice_tot_equity','choice_tot_liab'])
        data = pd.DataFrame(data['values'][symbol], columns=data['fields'])
        data['date'] = pd.to_datetime(data['date'])

        tmp['date'] = data['date']
        tmp['bps_adjust'] = data['choice_bpsadjust']
        tmp['lt_borrow'] = data['choice_ltborrow']
        tmp['qfa_eps'] = data['choice_qfa_eps']
        tmp['qfa_net_cash_flows_oper_act'] = data['choice_qfa_net_cash_flows_oper_act']
        tmp['qfa_net_profit_is'] = data['choice_qfa_net_profit_is']
        tmp['qfa_opprofit'] = data['choice_qfa_opprofit']
        tmp['qfa_tot_oper_rev'] = data['choice_qfa_tot_oper_rev']
        tmp['tot_assets'] = data['choice_tot_assets']
        tmp['tot_equity'] = data['choice_tot_equity']
        tmp['tot_liab'] = data['choice_tot_liab']

        # data_new=pd.concat([data_old,tmp],axis=0)
        tmp.to_csv(os.path.join(save_path, symbol + '.csv'),index=False)
        # print(tmp)
    except:
        print(csv_name,"has no data in dm")
        data_old.to_csv(os.path.join(save_path,csv_name),index=False)




