import pandas as pd

def timestamp_resample(timestamp_list, freq):
    '''
        get last timestamp at certain freq
        return the timestamp and the index of selected timestamp in the original list
    '''
    assert freq in ['T','H','D','S']
    date_list = pd.to_datetime(timestamp_list,unit='s')
    df = pd.DataFrame(range(len(date_list)),index=date_list)
    tmp = df.resample(freq).last()
    tmp.dropna(inplace=True)
    output_timestamp_list = [int(i.timestamp()) for i in tmp.index]
    output_timestamp_ix_list = [int(i) for i in tmp.iloc[:,0]]
    return output_timestamp_list,output_timestamp_ix_list