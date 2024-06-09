from qtoolkit.data import choice_client
import os


def download_tradedates(start='2000-01-01', end='2020-12-31', reset=False):
    if os.path.exists('data/TRADEDATE.csv') and not reset:
        print("TRADEDATE.csv exists!")
        return
    data = choice_client.tradedates(
        start, end, "period=1,order=1,market=CNSESH")
    data.to_csv('data/TRADEDATE.csv', index=False)
    return


if __name__ == '__main__':
    download_tradedates()