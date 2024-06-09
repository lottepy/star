# -*- coding: utf-8 -*-
# @Time    : 7/22/2019 11:15 AM
# @Author  : Joseph Chen
# @Email   : joseph.chen@magnumwm.com
# @FileName: base.py
# @Software: PyCharm

import re

import pandas as pd


class Symbols(object):
    """
    A class to convert between different symbol types:
    iuid, symbol, intriniosymbol, factsetsymbol, choicesymbol, rqclientsymbol, iuidsymbol

    外部调用symbol_converter实例的各from_xsymbol_to_y方法
    """
    FUTURES_EX = {
        'CFE': ['IC', 'IF', 'IH', 'T', 'TF', 'TS'],
        'SHF': ['AG', 'AL', 'AU', 'BU', 'CU', 'FU', 'HC', 'NI', 'PB', 'RB', 'RU', 'SN', 'SP', 'WR', 'ZN'],
        'DCE': ['A', 'B', 'BB', 'C', 'CS', 'EG', 'FB', 'I', 'J', 'JD', 'JM', 'L', 'M', 'P', 'PP', 'V', 'Y'],
        'CZC': ['AP', 'CF', 'CY', 'FG', 'JR', 'LR', 'MA', 'OI', 'PM', 'RI', 'RM', 'RS', 'SF', 'SM', 'SR', 'TA', 'WH', 'ZC']
    }

    # FUTURES_EX = {}
    # ctp_exchange_code = pd.read_csv("live_trading/ctp/data/ctp_exchange_code.csv")
    # for ex in ctp_exchange_code.exchange.unique():
    # 	trading_commodities = list(ctp_exchange_code[ctp_exchange_code.exchange==ex].code.values)
    # 	FUTURES_EX[ex] = trading_commodities

    future_pattern = re.compile('([a-zA-Z]+)(\d+)')  # 作为类属性 不重复生成

    def __getattr__(self, from_x_to_y: str):  # 调用不存在的实例属性时执行到这里
        """
            若不存在from_x_to_y方法
            则新定义一个方法：先把x转为iuid 再从iuid转为y
        """
        if from_x_to_y not in self.__dict__:
            _, x, _, y = from_x_to_y.split('_')
            def func(ori_symbol: str):
                if x == y:
                    return ori_symbol

                _iuid = getattr(self, f'from_{x}_to_iuid')(ori_symbol)
                result = getattr(self, f'from_iuid_to_{y}')(_iuid)
                return result

            self.__setattr__(from_x_to_y, func)
            return func

    ###################################################
    @staticmethod
    def is_hk_symbol(symbol: str) -> bool:
        if symbol.isdigit() and (len(symbol) <= 5):
            return True
        return False

    @staticmethod
    def is_future_iuid(iuid) -> bool:
        _, _type, _ = iuid.split('_')  # _symbol='FG909'
        return _type == '60'

    ###################################################
    @staticmethod
    def from_symbol_to_iuid(symbol: str) -> str:
        """ symbol examples:
            600000.SH, 000001.SZ, 0700, AAPL
        """
        if ".SH" in symbol:
            iuid = "CN_10_{}".format(symbol.replace(".SH",""))
        elif ".SZ" in symbol:
            iuid = "CN_10_{}".format(symbol.replace(".SZ",""))
        elif Symbols.is_hk_symbol(symbol):
            iuid = "HK_10_{}".format(symbol)
        else:
            iuid = "US_10_{}".format(symbol)
        return iuid

    @staticmethod
    def from_iuid_to_symbol(iuid:str)->str:
        symbol = iuid.split("_")[-1]
        return symbol

    ###################################################
    @staticmethod
    def from_intriniosymbol_to_iuid(intriniosymbol: str) -> str:
        """
            intriniosymbol examples:
            AAPL.NB, FB.NB
        """
        code = intriniosymbol.split('.')[0]
        assert code.isupper(), f'invalid intriniosymbol: {intriniosymbol}'
        return f'US_10_{code}'  # TODO 增加对期货的判断?

    @staticmethod
    def from_iuid_to_intriniosymbol(iuid: str) -> str:
        region, code, symbol = iuid.split("_")
        if region == 'US':
            return symbol + '.NB'

    ###################################################
    @staticmethod
    def from_factsetsymbol_to_iuid(factsetsymbol: str) -> str:
        """
            factsetsymbol examples:
            700-HK, 3333-HK
        """
        code, region = factsetsymbol.split('-')
        return f'{region}_10_{code}'  # TODO 增加对期货的判断?

    @staticmethod
    def from_iuid_to_factsetsymbol(iuid: str) -> str:
        region, code, symbol = iuid.split("_")
        if region=="HK":
            return "{}-{}".format(symbol.lstrip("0"), region)
    
    ###################################################
    @staticmethod
    def from_choicesymbol_to_iuid(choicesymbol: str) -> str:
        """ 
            choicesymbol examples:
            J1909.DCE -> CN_60_j1909  (注意大小写变化)
            IF1908.CFE -> CN_60_IF1908
            600000.SH -> CN_10_600000 / HC_10_600000
            00700.HK -> HK_10_700
            CN19U.SG -> SG_60_CNU19
        """
        code, exchange = choicesymbol.split(".")

        if exchange in ('SH', 'SZ'):
            from ..strategy.base_strategy import configs
            if configs and configs.region == 'HC':
                iuid = "HC_10_" + code
            else:  # CN or None or others
                iuid = "CN_10_" + code
        elif exchange == 'HK':
            iuid = f'HK_10_{code.lstrip("0")}'  # iuid需要把前面的"0"删除
        elif exchange in ('CZC', 'CFE'):
            iuid = "CN_60_" + (choicesymbol[:-8]).upper() + choicesymbol[-8:-4]
        elif exchange in ('SHF', 'DCE'):
            iuid = "CN_60_" + (choicesymbol[:-8]).lower() + choicesymbol[-8:-4]
        elif exchange == 'SG':
            if code.startswith('CN'):
                iuid = f'SG_60_CN{code[-1]}{code[-3:-1]}'
            else:
                raise ValueError(choicesymbol + ': other SG symbols are not supported right now..')
        else:
            raise ValueError('what choicesymbol is it? ' + choicesymbol)

        return iuid

    @staticmethod
    def from_iuid_to_choicesymbol(iuid:str)->str:
        region, _type, symbol = iuid.split("_")
        if _type == '10':  # stock
            if region == 'HK':
                symbol = symbol.rjust(5, '0')  # 在左边补"0"使长度为5
                sufix = '.HK'
            elif region in ("CN", "HC"):
                if symbol[0] in ('6', '5', '9'):
                    sufix = '.SH'
                else:
                    sufix = '.SZ'
            return symbol + sufix
        elif _type == '60':  # futures
            if region=='CN':
                pattern = re.compile('[a-zA-Z]+')
                m = pattern.match(symbol)
                commodity = m.group()
                assert isinstance(commodity, str), "Program does not understand iuid {}".format(iuid)
                commodity = commodity.upper()
                exchange = None
                for k, v in Symbols.FUTURES_EX.items():
                    if commodity in v:
                        exchange = k
                        break
                assert exchange is not None, "Program couldn't find appropriate exchange for commodity {}".format(commodity)

                pattern = re.compile('[\d]+')
                m = pattern.search(symbol)
                date = m.group()
                assert isinstance(date, str), "Program does not understand iuid {}".format(iuid)

                return f'{commodity}{date}.{exchange}'
            elif region == 'SG':
                if symbol.startswith('CN'):
                    return f'CN{symbol[-2:]}{symbol[-3]}.SG'

    ##################################################
    @staticmethod
    def from_brokersymbol_to_iuid(brokersymbol: str) -> str:
        """ 从broker直接查询持仓信息 对于CZC交易所的品种 会返回如CN_60_FG909这样的形式
            但是发给后端下单的时候 则必须使用CN_60_FG1909这样的IUID

            该函数判断broker返回的symbol是否属于CZC
            如果是 则根据数字的第一位是否为'9' 判断是加'1'还是加'0'

            对于非CZC 直接返回原值

            还有一些其他的broker symbol需要处理

            e.g. CN_60_FG909 -> CN_60_FG1909
        """
        _region, _type, _symbol = brokersymbol.split('_')  # _symbol='FG909'
        # if _type == '60' and _region == 'CN':
        #     m = Symbols.future_pattern.match(_symbol)
        #     assert m, 'symbol not understand: {}'.format(brokersymbol)
        #     exchange, code = m.group(1), m.group(2)  # 'FG' '909'

        #     if exchange in Symbols.FUTURES_EX['CZC']:
        #         if code[0] == '9':
        #             return f'{_region}_{_type}_{exchange}1{code}'  # 'FG'和'909'之间插入一个'1'
        #         else:  # code[0] == '0' or '1' or other..
        #             return f'{_region}_{_type}_{exchange}2{code}'  # 'CY'和'001'之间插入一个'2'
        #     return brokersymbol
        if _type == '60' and _region == 'SG' and _symbol == 'XINA50':
            raise ValueError('XINA50 symbol is not allowed')
        return brokersymbol

    ###################################################
    @staticmethod
    def from_rqclientsymbol_to_iuid(rqclientsymbol: str) -> str:
        """
            CN.10.600000 -> CN_10_600000
        """
        return rqclientsymbol.replace('.', '_')

    @staticmethod
    def from_iuid_to_rqclientsymbol(iuid: str) -> str:
        return iuid.replace('_', '.')

    ###################################################
    # @staticmethod
    # def from_iuid_to_marketiuid(iuid: str) -> str:
    #     """
    #         从交易的IUID改为获取行情数据的IUID。
    #         例如，HC开头的IUID需要改为CN开头的IUID，才能被RQ Client支持，
    #         可能还有其他用到的场合

    #         HC_10_600000 -> CN_10_600000_full
    #     """
    #     if iuid.startswith('HC'):
    #         return 'CN' + iuid[2:] + '_full'
    #     else:
    #         return iuid
    
    # @staticmethod
    # def from_marketiuid_to_iuid(iuid: str) -> str:
    #     """
    #         CN_10_600000_full -> HC_10_600000
    #     """
    #     if iuid.endswith('_full') and iuid.startswith('CN'):
    #         from ..strategy.base_strategy import configs
    #         if configs and configs.region == 'HC':
    #             return 'HC' + iuid[2:-5]
    #         else:  # CN or None or others
    #             return 'CN' + iuid[2:-5]
    #     else:
    #         return iuid

    ###################################################
    @staticmethod
    def from_iuidsymbol_to_iuid(iuidsymbol: str) -> str:
        """
            没有实际转换，为了保持代码结构的一致性而添加
        """
        return iuidsymbol

    @staticmethod
    def from_iuid_to_iuidsymbol(iuid: str) -> str:
        """
            没有实际转换，为了保持代码结构的一致性而添加
        """
        return iuid

    ###################################################
    @staticmethod
    def is_stock_iuid(iuid) -> bool:
        _, _type, _ = iuid.split('_')  # _symbol='FG909'
        return _type == '10'

    @classmethod
    def get_symbol(cls, iuid, symbol_prefix):
        return getattr(cls, f"from_iuid_to_{symbol_prefix}symbol")(iuid)


symbol_converter = Symbols()  # 实例化 外部调用此实例
