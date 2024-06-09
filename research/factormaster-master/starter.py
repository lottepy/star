from factormaster.utils.tradedates import download_tradedates


if __name__ == '__main__':
    # UPDATE TRADEDATE
    download_tradedates(end='2021-12-31', reset=True)