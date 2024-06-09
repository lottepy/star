from lib.commonalgo.data.influxdbData import InfluxDB
import time

if __name__=="__main__":

    """
    Fetch market/orderbook data
    :param iuid: list of str, example ["CN_10_600000", "CN_10_600016"]
    :param fields: list of str, example ["price", "open", "volume"]/["a1", "av1", "b1", "bv1"]
    :param startTimestamp: str, example "YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"
    :param endTimestamp: str, example "YYYY-MM-DDTHH:MM:SS.nnnnnnnnnZ"
    :param frequency: str, example "1ms", default None
    :param toDataFrame: bool, example True
    :param convertTime: str, example 'Asia/Hong_Kong'/"UTC", default None
    :return: dict of Generator, or dict of DataFrame
    """

    influxdb = InfluxDB()
    iuids = ["CN_10_600000", "CN_10_600016"]
    fields = ["open", "high","low", "close", "volume"]
    startTimestamp = "2018-12-25T09:24:00.001000000Z"
    endTimestamp = "2018-12-28T15:00:01.001000000Z"
    frequency = "1ms"

    tic = time.time()
    res1 = influxdb.marketData(iuids, fields, startTimestamp, endTimestamp, frequency="1m",
                               toDataFrame=True, convertTime='Asia/Hong_Kong')
    toc = time.time()
    print("Elapsed time: {:.2f}".format(toc - tic))

    fields = ['b1', 'b2', 'b3', 'b4', 'b5',
              'a1', 'a2', 'a3', 'a4', 'a5',
              'bv1', 'bv2', 'bv3', 'bv4', 'bv5',
              'av1', 'av2', 'av3', 'av4', 'av5']
    startTimestamp = "2016-02-01T09:24:00.001000000Z"
    endTimestamp = "2016-04-01T15:00:01.001000000Z"

    tic = time.time()
    res2 = influxdb.orderbookData(iuids, fields, startTimestamp, endTimestamp, frequency="1ms",
                                  toDataFrame=True, convertTime='Asia/Hong_Kong')
    toc = time.time()
    print("Elapsed time: {:.2f}".format(toc - tic))

    print("Done.")

    debbie.poon@ust.hk