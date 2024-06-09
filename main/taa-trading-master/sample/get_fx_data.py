from utils.fx_client_lib.fx_client import get_oss_data

def main(symbols, start, end):
    data,time = get_oss_data(symbols, start, end)
    print(data)
    print(time)

if __name__ =='__main__':
    symbols = ['AUDCAD','AUDJPY','USDIDR']
    start = '2019-11-01'
    end = '2019-11-25'
    main(symbols, start, end)