import datetime

from pandas.tseries.offsets import *
from sklearn.ensemble import RandomForestRegressor

from Algo.DFM import *
from Algo.Load_Data import *


def getGDP(train_date):
    GDP_QOQ = pd.read_csv(RAWDATAPATH + "\\US\\GDP QoQ Index.csv")
    GDP_QOQ = GDP_QOQ[['Date', 'Period', 'Actual']]
    GDP_QOQ.columns = ['Release Date', 'Period', 'GDP_QOQ']
    GDP_QOQ['Release Date'] = pd.to_datetime(GDP_QOQ['Release Date'])
    GDP_QOQ['Date'] = GDP_QOQ['Release Date']
    GDP_QOQ['Date'] = GDP_QOQ['Date'].apply(lambda x: x - QuarterEnd())
    GDP_QOQ['Period'] = GDP_QOQ['Period'].apply(lambda x: x[-1])
    GDP_QOQ = GDP_QOQ[GDP_QOQ['Period'] == "A"]
    GDP_QOQ['GDP_QOQ'] = GDP_QOQ['GDP_QOQ'].apply(lambda x: eval(x[:-1]) if x[-1] == "%" else np.nan)
    GDP_QOQ.dropna(axis=0, inplace=True)
    GDP_QOQ.drop(['Period'], axis=1, inplace=True)
    GDP_QOQ.set_index(['Date'], inplace=True)
    GDP_QOQ = GDP_QOQ[GDP_QOQ['Release Date'] < datetime.datetime.strptime(train_date, '%Y-%m-%d')]
    GDP_QOQ.drop(['Release Date'], axis=1, inplace=True)

    begindate = GDP_QOQ.index[0]
    enddate = GDP_QOQ.index[-1]
    tmp_df = pd.DataFrame(index=pd.date_range(start=begindate, end=enddate, freq='M'))
    GDP_QOQ = pd.concat([GDP_QOQ, tmp_df], axis=1)
    GDP_QOQ['GDP_QOQ'] = GDP_QOQ['GDP_QOQ'].interpolate(limit_direction='backward')
    return GDP_QOQ


def mergegdp_factor(factors_mth, factors_qtr):
    ml_data_tmp = pd.merge(factors_mth, factors_qtr, left_index=True, right_index=True, how='outer')
    ml_data_tmp = ml_data_tmp[~ml_data_tmp.index.duplicated('last')]
    ml_data_tmp['f1_x_1'] = ml_data_tmp['f1_x'].shift(1)
    ml_data_tmp['f1_x_2'] = ml_data_tmp['f1_x'].shift(2)
    ml_data_tmp['f1_x_3'] = ml_data_tmp['f1_x'].shift(3)
    ml_data_tmp['f1_x_4'] = ml_data_tmp['f1_x'].shift(4)
    ml_data_tmp['f1_x_5'] = ml_data_tmp['f1_x'].shift(5)
    ml_data_tmp['f2_x_1'] = ml_data_tmp['f2_x'].shift(1)
    ml_data_tmp['f2_x_2'] = ml_data_tmp['f2_x'].shift(2)
    ml_data_tmp['f2_x_3'] = ml_data_tmp['f2_x'].shift(3)
    ml_data_tmp['f2_x_4'] = ml_data_tmp['f2_x'].shift(4)
    ml_data_tmp['f2_x_5'] = ml_data_tmp['f2_x'].shift(5)
    ml_data_tmp['f3_x_1'] = ml_data_tmp['f3_x'].shift(1)
    ml_data_tmp['f3_x_2'] = ml_data_tmp['f3_x'].shift(2)
    ml_data_tmp['f3_x_3'] = ml_data_tmp['f3_x'].shift(3)
    ml_data_tmp['f3_x_4'] = ml_data_tmp['f3_x'].shift(4)
    ml_data_tmp['f3_x_5'] = ml_data_tmp['f3_x'].shift(5)
    ml_data_tmp['f1_y_2'] = ml_data_tmp['f1_y'].shift(2)
    ml_data_tmp['f2_y_2'] = ml_data_tmp['f2_y'].shift(2)
    ml_data_tmp['f3_y_2'] = ml_data_tmp['f3_y'].shift(2)

    return ml_data_tmp


def matchquarter(release_date):
    if release_date.month <= 3 and release_date.month >= 1:
        date = datetime.datetime(release_date.year - 1, 11, 30)
    elif release_date.month <= 6 and release_date.month >= 4:
        date = datetime.datetime(release_date.year, 2, calendar.monthrange(release_date.year, 2)[1])
    elif release_date.month <= 9 and release_date.month >= 7:
        date = datetime.datetime(release_date.year, 5, 31)
    elif release_date.month <= 12 and release_date.month >= 10:
        date = datetime.datetime(release_date.year, 8, 31)
    return date


if __name__ == "__main__":
    qtr_align_dt, mtr_align_dt, wkl_align_dt = load_data(QTR_PATH, MTR_PATH, WKL_PATH, 'US')

    train_dates = ['2016-01-01','2016-02-01','2016-03-01','2016-04-01','2016-05-01','2016-06-01','2016-07-01','2016-08-01','2016-09-01','2016-10-01','2016-11-01','2016-12-01','2017-01-01','2017-02-01','2017-03-01','2017-04-01','2017-05-01','2017-06-01','2017-07-01','2017-08-01','2017-09-01','2017-10-01','2017-11-01','2017-12-01', '2018-01-01','2018-02-01','2018-03-01','2018-04-01','2018-05-01','2018-06-01','2018-07-01','2018-08-01','2018-09-01','2018-10-01','2018-11-01','2018-12-01']
    #train_dates = ['2018-01-01']

    result = None
    result_list = []

    GDP_QOQ = pd.read_csv(RAWDATAPATH + "\\US\\GDP QoQ Index.csv", parse_dates=['Date'])
    GDP_QOQ = GDP_QOQ[GDP_QOQ['Period'].str.contains('A')]
    GDP_QOQ['date'] = GDP_QOQ['Date'].map(matchquarter)
    GDP_QOQ.sort_values(by='date', inplace=True)
    GDP_QOQ = GDP_QOQ.loc[:, ['Date', 'date', 'Actual']]
    GDP_QOQ.rename(columns={'Actual': 'GDP QoQ', 'Date': 'release date'}, inplace=True)
    GDP_QOQ = GDP_QOQ.set_index(['date'])
    while (np.isnan(GDP_QOQ['GDP QoQ'].iloc[0])):
        GDP_QOQ.drop(GDP_QOQ.index[0], inplace=True)

    for i in range(len(train_dates)):
        # if i == len(train_dates)-1:
        #    break

        train_date = train_dates[i]
        if i == len(train_dates) - 1:
            forecast_dates = [j.strftime('%Y-%m-%d') for j in pd.date_range(pd.to_datetime(train_dates[i]), pd.to_datetime(train_dates[i]) + datetime.timedelta(days=30), freq='W')]
        else:
            forecast_dates = [j.strftime('%Y-%m-%d') for j in
                              pd.date_range(pd.to_datetime(train_dates[i]), pd.to_datetime(train_dates[i + 1]),
                                            freq='W')]

            insampl_qtr_align_dt, insampl_mtr_align_dt, insampl_wkl_align_dt, qtr_variables, mtr_variables, wkl_variables = insample_querry(
                qtr_align_dt, mtr_align_dt, wkl_align_dt, train_date, window=15, min_period=14)

            dfm_m = DFM()
            dfm_m.insample = insampl_mtr_align_dt
            dfm_m.fit()
            factors_m = dfm_m.factors

            dfm_q = DFM()
            dfm_q.insample = insampl_qtr_align_dt
            dfm_q.fit()
            factors_q = dfm_q.factors

            for forecast_date in forecast_dates:
                outsampl_qtr_align_dt, outsampl_mtr_align_dt, outsampl_wkl_align_dt = outsample_querry(qtr_align_dt,
                                                                                                       mtr_align_dt,
                                                                                                       wkl_align_dt,
                                                                                                       forecast_date,
                                                                                                       qtr_variables,
                                                                                                       mtr_variables,
                                                                                                       wkl_variables,
                                                                                                       rolling_qtr=20,
                                                                                                       rolling_mtr=150,
                                                                                                       rolling_wkl=50)
                dfm_m.outsample = outsampl_mtr_align_dt
                dfm_m.filter()
                new_factors_m = dfm_m.new_factors

                dfm_q.outsample = outsampl_qtr_align_dt
                dfm_q.filter()
                new_factors_q = dfm_q.new_factors

                # Get the train data set available train_date
                GDP_QOQ_train = GDP_QOQ[GDP_QOQ.index < train_date]
                GDP_QOQ_train['GDP QoQ'] = GDP_QOQ_train['GDP QoQ'].interpolate(limit_direction='backward')

                ml_data_tmp = mergegdp_factor(factors_m, factors_q)
                ml_data_tmp = ml_data_tmp.drop(columns=['f1_y', 'f2_y', 'f3_y'])
                ml_data = pd.merge(GDP_QOQ_train, ml_data_tmp, left_index=True, right_index=True)
                ml_data = ml_data.dropna()

                y = np.array(ml_data['GDP QoQ'])
                x = ml_data.iloc[:, 2:]
                x_list = list(x.columns)
                x = np.array(x)

                rf = RandomForestRegressor(n_estimators=1000, random_state=20)
                # Train the model on training data
                rf.fit(x, y)

                factors_m_full = pd.concat([factors_m, new_factors_m.iloc[-3:, :]])
                factors_q_full = pd.concat([factors_q, new_factors_q.iloc[-2:, :]])
                test_factors_raw = mergegdp_factor(factors_m_full, factors_q_full)
                test_factors_raw['f1_y_2'] = test_factors_raw['f1_y']
                test_factors_raw['f2_y_2'] = test_factors_raw['f2_y']
                test_factors_raw['f3_y_2'] = test_factors_raw['f3_y']
                test_factors_raw = test_factors_raw.ffill()
                test_factors_raw = test_factors_raw.drop(columns=['f1_y', 'f2_y', 'f3_y'])
                test_factors = test_factors_raw.iloc[-1, :]

                test_factors = np.array(test_factors)
                test_factors = test_factors.reshape(1,-1)
                # Use the forest's predict method on the test data
                predictions = rf.predict(test_factors)

                new_y = pd.DataFrame({'Predict': predictions}, index=[forecast_date])

                result_list.append(new_y)

    result = pd.concat(result_list)

    result.to_csv(RESULTSPATH + '\\test1' + '.csv')
