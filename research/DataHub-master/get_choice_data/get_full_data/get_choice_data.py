from choice_proxy_client import choice_client
from datetime import timedelta, datetime
from retrying import retry
import pandas as pd


class get_monthly_data(object):

    def __init__(self):

        self.start_date = '2008-06-01'
        self.end_date = '2019-07-31'
        self.all_date_set_1M = pd.date_range(self.start_date, self.end_date, freq = '1M')
        self.all_date_set_1M = [datetime.strftime(date, '%Y-%m-%d') for date in self.all_date_set_1M]

        self.all_date_set_6M = pd.date_range(self.start_date, self.end_date, freq='6M')
        self.all_date_set_6M = [datetime.strftime(date, '%Y-%m-%d') for date in self.all_date_set_6M]

        # self.all_stock_set = pd.read_csv("all_stock_set.csv")['ticker'].tolist()
        self.all_stock_set = pd.read_csv("merge_stock.csv")['ticker'].tolist()
        self.all_stock_set = ['0' + ticker for ticker in self.all_stock_set]

        self.monthly_factors_direct = pd.read_csv('monthly_factor_direct.csv')['factor'].tolist()
        self.monthly_factors_statement = pd.read_csv('monthly_factor_statement.csv')

        self.direct_factor_names = ['ASSETTURNRATIO','BPSADJUST','CATURNRATIO','CFOPS','EBITDATOGR',
                                    'EPSTTM','FATURNRATIO','GPMARGIN','INVTURNRATIO','NPMARGIN', 'OPTOEBT',
                                    'PB','PE','PS','ROA']
        self.monthly_factors_statement_new = pd.read_csv('monthly_factor_new.csv')

    @retry
    def get_css_data(self, ticker, factor_list, params):
        monthly_data = choice_client.css(ticker, factor_list, params)
        return monthly_data


    def get_direct_monthly_data(self):

        for ticker in self.all_stock_set:
            print("Get direct monthly data for stock: " + ticker)

            monthly_df = pd.DataFrame()
            for date in self.all_date_set_1M:
                param = "TradeDate = {0}, ReportDate = {1}, curtype=4".format(date, date)
                monthly_data = self.get_css_data(ticker, self.monthly_factors_direct, param)
                monthly_data.columns = self.direct_factor_names
                monthly_data.index = [date]
                monthly_df = pd.concat([monthly_df, monthly_data], axis=0)

            # print(monthly_df.iloc[-5:, :])
            monthly_df = monthly_df.fillna(method='ffill')
            monthly_df.to_csv('output_data/monthly_data/' + ticker + '.csv')



    def get_statement_monthly_data(self):
        for update_date in self.all_date_set_6M:

            data = pd.DataFrame()
            for i in range(len(self.monthly_factors_statement)):
                factor_name = self.monthly_factors_statement.iloc[i, 0]
                print('get monthly statement data for date: {}, for factor: {}'.format(update_date, factor_name))
                statement = self.monthly_factors_statement.iloc[i, 1]
                item_code = self.monthly_factors_statement.iloc[i, 2]

                data_new = pd.DataFrame()
                params = "ReportDate={0},type=1,CurType=4,ItemsCode={1}".format(update_date, item_code)

                for i in range(37):
                    # print (i)
                    data_sub = self.get_css_data(self.all_stock_set[i * 50: (i + 1) * 50], statement,params)
                    data_new = pd.concat([data_new, data_sub], axis=1)

                data_new.columns = self.all_stock_set
                data = pd.concat([data, data_new], axis=0)

            data.index = self.monthly_factors_statement['factor'].tolist()
            data.columns = self.all_stock_set

            for ticker in self.all_stock_set:
                print('get monthly statement data for ticker: ' + ticker)
                try:
                    former_df = pd.read_csv('output_data/monthly_factor_statement/' + ticker + '.csv',index_col=0)
                except:
                    former_df = pd.DataFrame()
                update_df = pd.DataFrame(data[ticker]).T
                update_df.columns = self.monthly_factors_statement['factor'].tolist()
                update_df.index = [update_date]
                later_df = pd.concat([former_df, update_df], axis=0)
                later_df.to_csv('output_data/monthly_factor_statement/' + ticker + '.csv')


    def get_statement_monthly_data_new(self):
        for update_date in self.all_date_set_6M:

            data = pd.DataFrame()
            for i in range(len(self.monthly_factors_statement_new)):
                factor_name = self.monthly_factors_statement_new.iloc[i, 0]
                print('get new monthly statement data for date: {}, for factor: {}'.format(update_date, factor_name))
                statement = self.monthly_factors_statement_new.iloc[i, 1]
                item_code = self.monthly_factors_statement_new.iloc[i, 2]

                data_new = pd.DataFrame()
                params = "ReportDate={0},type=1,CurType=4,ItemsCode={1}".format(update_date, item_code)

                for i in range(37):
                    data_sub = self.get_css_data(self.all_stock_set[i * 50: (i + 1) * 50], statement,params)
                    data_new = pd.concat([data_new, data_sub], axis=1)

                data_new.columns = self.all_stock_set
                data = pd.concat([data, data_new], axis=0)

            data.index = self.monthly_factors_statement_new['factor'].tolist()
            data.columns = self.all_stock_set

            for ticker in self.all_stock_set:
                print('get new monthly statement data for ticker: ' + ticker)
                try:
                    former_df = pd.read_csv('output_data/monthly_factor_statement_new/' + ticker + '.csv',index_col=0)
                except:
                    former_df = pd.DataFrame()
                update_df = pd.DataFrame(data[ticker]).T
                update_df.columns = self.monthly_factors_statement_new['factor'].tolist()
                update_df.index = [update_date]
                later_df = pd.concat([former_df, update_df], axis=0)
                later_df.to_csv('output_data/monthly_factor_statement_new/' + ticker + '.csv')


if __name__ == '__main__':
    choice_data = get_monthly_data()
    choice_data.get_direct_monthly_data()
    # choice_data.get_statement_monthly_data()
    # choice_data.get_statement_monthly_data_new()