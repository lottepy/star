from qtoolkit.data import choice_client
import pandas as pd
import os

START_DATE = pd.to_datetime('2010-01-01')
END_DATE = pd.to_datetime('2020-11-30')


def download_industry_indices():
    industry_name= pd.read_csv('data/indices/industry/industry_name.csv', index_col=0)
    codes = ','.join(industry_name['symbol'].tolist())
    df = choice_client.csd(codes,
                           "CLOSE",
                           START_DATE.strftime("%Y-%m-%d"),
                           END_DATE.strftime("%Y-%m-%d"),
                           "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    df.columns = industry_name['name']
    df.to_csv('data/indices/industry/industry.csv')
    return


def download_industry_index_name():
    df = choice_client.sector("905009002011", f"{END_DATE.strftime('%Y-%m-%d')}")
    df2 = choice_client.sector("905018003", f"{END_DATE.strftime('%Y-%m-%d')}")
    all_df = pd.concat([df, df2], axis=0)
    all_df.columns = ['symbol', 'name']
    all_df.to_csv('data/indices/industry/industry_name.csv')
    return


if __name__ == '__main__':
    download_industry_index_name()
    download_industry_indices()