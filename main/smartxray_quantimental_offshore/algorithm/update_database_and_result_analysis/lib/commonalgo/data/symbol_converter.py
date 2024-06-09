import re


class SymbolConvert():
    """
    A class to convert between different symbol formats:
    IUID, CHOICE, REUTERS, INTRINIO, BLOOMBERG
    """
    EXCHANGES = {
        'HK': ['HK'],
        'CN': ['CN', 'CH', 'SHFE', 'SH', 'SZ', 'SSE', 'CFFEX', 'CFE', 'CF', 'SGE', 'DCE', 'SHF', 'CZCE', 'ZCE',
               'CZC', 'BCE', 'INE']
    }

    STOCK_KEYS = ['CN', 'CH', 'SH', 'SZ', 'SSE']

    FUTURES_KEYS = ['CF', 'CFFEX', 'CFE', 'SGE', 'DCE', 'SHF', 'SHFE', 'ZCE', 'CZCE', 'CZC', 'BCE', 'INE']

    FUTURES = {
        'CFE': ['IC', 'IF', 'IH', 'T', 'TF', 'TS'],
        'SHF': ['AG', 'AL', 'AU', 'BU', 'CU', 'FU', 'HC', 'NI', 'PB', 'RB', 'RU', 'SN', 'SP', 'WR', 'ZN'],
        'DCE': ['A', 'B', 'BB', 'C', 'CS', 'EG', 'FB', 'I', 'J', 'JD', 'JM', 'L', 'M', 'P', 'PP', 'V', 'Y'],
        'CZC': ['AP', 'CF', 'CY', 'FG', 'JR', 'LR', 'MA', 'OI', 'PM', 'RI', 'RM', 'RS', 'SF', 'SM', 'SR', 'TA', 'WH',
                'ZC']
    }

    @staticmethod
    def find_exchange(stock_code):
        split_symbol = ['-', '_', ' ', '.', '']

        stock_code = stock_code.upper()

        if not re.search('\d', stock_code):
            stock_code = re.sub('(\sUS\sEQUITY)|([^A-Za-z0-9]US)|(US[^A-Za-z0-9])', '', stock_code)
            exchange = ['US', 'US', stock_code]
            return exchange

        for country in SymbolConvert.EXCHANGES:
            for keyword in SymbolConvert.EXCHANGES[country]:
                for split in split_symbol:
                    if keyword + split in stock_code:
                        key = keyword + split
                        exchange = [country, keyword, re.sub(key, '', stock_code)]
                        return exchange
                    elif split + keyword in stock_code:
                        key = split + keyword
                        exchange = [country, keyword, re.sub(key, '', stock_code)]
                        return exchange

    @staticmethod
    def convert_hk(exchange, trading):
        if trading:
            return ["HK_10_" + re.search("0*([1-9][0-9]*|0)", exchange[2]).group(1)]
        return "HK_10_" + re.search("0*([1-9][0-9]*|0)", exchange[2]).group(1)

    @staticmethod
    def convert_cn(exchange, trading):
        if exchange[1] in SymbolConvert.STOCK_KEYS:
            digits = re.search("\d+", exchange[2]).group()
            for i in range(0, 6 - len(digits)):
                digits = '0' + digits
            if trading:
                return ['CN_10_' + digits, 'HC_10_' + digits]
            iuid = 'CN_10_' + digits
        elif exchange[1] in SymbolConvert.FUTURES_KEYS:
            digits = re.search('\d+', exchange[2]).group()
            if len(digits) == 3:
                letters = re.search('[^0-9]', exchange[2]).group()
                if digits[0] is '9':
                    digits = '1' + digits
                elif digits[0] is '0':
                    digits = '2' + digits
                iuid = "CN_60_" + letters + digits
            else:
                iuid = 'CN_60_' + exchange[2]

        if trading:
            return [iuid]
        return iuid

    @staticmethod
    def convert_us(exchange, trading):
        if trading:
            return "US_10_" + exchange[2]
        return "US_10_" + exchange[2]

    @staticmethod
    def convert_iuid(stock_code, trading):
        exchange = SymbolConvert.find_exchange(stock_code)

        switch = {
            'HK': SymbolConvert.convert_hk,
            'CN': SymbolConvert.convert_cn,
            'US': SymbolConvert.convert_us
        }

        function = switch[exchange[0]]

        if callable(function):
            return function(exchange, trading)
        else:
            print('Incorrect exchange')

    @staticmethod
    def convert_choice(stock_code):
        region, type, symbol = stock_code.split('_')
        if type == '10':
            if region == 'HK':
                symbol = symbol.rjust(5, '0')
                suffix = '.HK'
            elif region == 'CN' or region == 'HC':
                symbol = symbol.rjust(6, '0')
                if symbol[0] in ('6', '5', '9'):
                    suffix = '.SH'
                else:
                    suffix = '.SZ'
            return symbol + suffix
        elif type == '60':
            commodity = re.search('[A-Za-z]+', symbol).group().upper()
            assert isinstance(commodity, str), 'Program does not understand symbol {}'.format(stock_code)
            for k, v in SymbolConvert.FUTURES.items():
                if commodity in v:
                    exchange = k
                    break
            assert isinstance(exchange, str), 'Unable to find correct exchange for {}'.format(stock_code)
            return symbol + '.' + exchange

    @staticmethod
    def convert_intrinio(stock_code):
        symbol = re.search('_[A-Za-z]+', stock_code).group().upper()[1:]
        return symbol + '.NB'

    @staticmethod
    def convert_reuters(stock_code):
        region, kind, symbol = stock_code.split('_')
        if region == 'HK':
            if len(symbol) != 5:
                symbol = symbol.rjust(4, '0')
            return symbol + '.HK'
        elif region == 'CN' or region == 'HC':
            return SymbolConvert.convert_choice(stock_code)

    @staticmethod
    def convert_bloomberg(stock_code):
        region, kind, symbol = stock_code.split('_')
        if region == 'HK':
            if kind == '10':
                return symbol + ' HK EQUITY'
        elif region == 'CN':
            if kind == '10':
                return symbol + ' CN EQUITY'
        elif region == 'US':
            return symbol + ' US EQUITY'

    ###################################################################
    @staticmethod
    def converter(symbols, format_in, format_out='IUID', trading=False):
        conversions = {
            'IUID': ['CHOICE', 'REUTERS', 'INTRINIO', 'BLOOMBERG'],
            'CHOICE': ['IUID', 'REUTERS', 'BLOOMBERG'],
            'INTRINIO': ['IUID', 'BLOOMBERG'],
            'REUTERS': ['IUID', 'CHOICE', 'BLOOMBERG'],
            'BLOOMBERG': ['IUID', 'REUTERS', 'CHOICE', 'INTRINIO']
        }

        format_in = format_in.upper()
        format_out = format_out.upper()

        assert format_in in conversions.keys(), 'Invalid input format'
        assert format_out in conversions.keys(), 'Invalid output format'

        if format_in == format_out:
            return symbols

        assert format_out in conversions[format_in], 'Program does not understand symbol {}'.format(symbols)

        switch = {
            'CHOICE': SymbolConvert.convert_choice,
            'INTRINIO': SymbolConvert.convert_intrinio,
            'REUTERS': SymbolConvert.convert_reuters,
            'BLOOMBERG': SymbolConvert.convert_bloomberg
        }

        if format_out != 'IUID':
            function = switch[format_out]

        if isinstance(symbols, str):
            if format_in != 'IUID':
                converted = SymbolConvert.convert_iuid(symbols, trading)
            if format_out != 'IUID':
                converted = function(symbols)
        else:
            converted = []
            if format_in != 'IUID':
                for i in range(len(symbols)):
                    converted.append(SymbolConvert.convert_iuid(symbols[i], trading))
            if format_out != 'IUID':
                for j in range(len(symbols)):
                    converted.append(function(symbols[j]))
        return converted


if __name__ == "__main__":
    symbols = ['00002HK', '000001sh', '402839sz']
    print(SymbolConvert.converter(symbols, 'choice', 'iuid'))

    symbols = ['000001.SH', '40598.HK']
    print(SymbolConvert.converter(symbols, 'choice', 'iuid'))

    symbols = ['US_10_IBM', 'US_10_VTK']
    print(SymbolConvert.converter(symbols, 'iuid', 'intrinio'))
