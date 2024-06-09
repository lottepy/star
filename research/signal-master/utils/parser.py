import pickle
import os
from quantcycle.app.result_exporter.result_reader import ResultReader
import pandas as pd
from collections import defaultdict


def parse(dir_name):
    file_name = 'status.pkl'
    result_file_name = 'result.hdf5'
    file = os.path.join(dir_name, file_name)
    result_file = os.path.join(dir_name, result_file_name)
    signal_dir = os.path.join(dir_name, 'signal')
    if not os.path.exists(signal_dir):
        os.mkdir(signal_dir)

    result_reader = ResultReader(result_file)
    attrs = result_reader.get_attr(
        result_reader.get_all_sids(), ['params', 'symbols'])
    symbols = attrs['symbols'].values[0]
    params_dict = dict(zip(attrs.index, attrs['params']))
    df_dict = defaultdict(list)

    status = pickle.load(open(file, 'rb'))
    signal_name = [i for i in status[0].keys() if i != 'time']
    time = [pd.to_datetime(int(i), unit='s') for i in status[0]['time']]

    for i in status.keys():
        current_record = status[i]
        for sgnl in signal_name:
            value = current_record[sgnl]
            for j in range(len(symbols)):
                df = pd.DataFrame(
                    value[:, j],
                    index=time,
                    columns=[f'{sgnl}%{pack_params(params_dict[i])}'])
                df_dict[symbols[j]].append(df)

    for i in df_dict.keys():
        save_file = os.path.join(signal_dir, f'{i}.csv')
        pd.concat(df_dict[i], axis=1).to_csv(save_file)
    return


def pack_params(param_array):
    if len(param_array) > 0:
        s = "&".join([str(i) for i in param_array])
        return 'params&'+s
    else:
        return 'params&NULL'


if __name__ == '__main__':
    parse('result/BollingerBand')
    parse('result/CCI')
    parse('result/DonchianChannel')
    parse('result/EMA')
    parse('result/KD')
    parse('result/MA')
    parse('result/MACD')
    parse('result/Momentum')
    parse('result/Reversion')
    parse('result/RSI')
