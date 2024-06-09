from choice_client import c
from algorithm import addpath
import pandas as pd
import os

sector_mapping = {
    "801010.SWI" : "农林牧渔",
    "801020.SWI" : "采掘",
    "801030.SWI" : "化工",
    "801040.SWI" : "钢铁",
    "801050.SWI" : "有色金属",
    "801080.SWI" : "电子",
    "801110.SWI" : "家用电器",
    "801120.SWI" : "食品饮料",
    "801130.SWI" : "纺织服装",
    "801140.SWI" : "轻工制造",
    "801150.SWI" : "医药生物",
    "801160.SWI" : "公用事业",
    "801170.SWI" : "交通运输",
    "801180.SWI" : "房地产",
    "801200.SWI" : "商业贸易",
    "801210.SWI" : "休闲服务",
    "801230.SWI" : "综合",
    "801710.SWI" : "建筑材料",
    "801720.SWI" : "建筑装饰",
    "801730.SWI" : "电气设备",
    "801740.SWI" : "国防军工",
    "801750.SWI" : "计算机",
    "801760.SWI" : "传媒",
    "801770.SWI" : "通信",
    "801780.SWI" : "银行",
    "801790.SWI" : "非银金融",
    "801880.SWI" : "汽车",
    "801890.SWI" : "机械设备",
    "000300.SH" : "沪深300",
    "000905.SH" : "中证500",
    "000852.SH" : "中证1000",
    "000918.SH" : "300成长",
    "000919.SH" : "300价值",
}

if __name__ == '__main__':
    symbols = "801010.SWI,801020.SWI,801030.SWI,801040.SWI,801050.SWI,801080.SWI,801110.SWI," \
              "801120.SWI,801130.SWI,801140.SWI,801150.SWI,801160.SWI,801170.SWI,801180.SWI," \
              "801200.SWI,801210.SWI,801230.SWI,801710.SWI,801720.SWI,801730.SWI,801740.SWI," \
              "801750.SWI,801760.SWI,801770.SWI,801780.SWI,801790.SWI,801880.SWI,801890.SWI," \
              "000300.SH,000905.SH,000852.SH,000918.SH,000919.SH"

    start_date = "2013-12-31"
    end_date = "2020-12-31"
    data = c.csd(symbols, "MV", start_date, end_date, "period=1,adjustflag=1,curtype=1,order=1,market=CNSESH")
    # print(data)

    index_mktcap = pd.DataFrame()
    index_mktcap["消费"] = data["801110.SWI", "MV"] + data["801120.SWI", "MV"] + data["801130.SWI", "MV"] + data["801880.SWI", "MV"] + data["801210.SWI", "MV"]
    index_mktcap["科技"] = data["801750.SWI", "MV"] + data["801760.SWI", "MV"] + data["801770.SWI", "MV"] + data["801080.SWI", "MV"]
    index_mktcap["工业"] = (data["801030.SWI", "MV"] + data["801040.SWI", "MV"] + data["801050.SWI", "MV"] + data["801140.SWI", "MV"] + data["801740.SWI", "MV"] +\
                          data["801730.SWI", "MV"] + data["801890.SWI", "MV"]) / 2
    index_mktcap["医疗"] = data["801150.SWI", "MV"]
    index_mktcap["金融"] = (data["801780.SWI", "MV"] + data["801790.SWI", "MV"]) / 5

    index_mktcap["大市值股票"] = data["000300.SH", "MV"]
    index_mktcap["中小市值股票"] = data["000905.SH", "MV"]
    index_mktcap["成长"] = data["000918.SH", "MV"]
    index_mktcap["价值"] = data["000919.SH", "MV"]

    print(index_mktcap)
    index_mktcap = index_mktcap / 100000000.0
    index_mktcap.to_csv(os.path.join(addpath.data_path, "foropt", "index_mktcap.csv"), encoding='utf-8-sig')
