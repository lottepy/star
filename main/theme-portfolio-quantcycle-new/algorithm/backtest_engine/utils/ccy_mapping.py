
#..................................................................Ccy Type.....................................................................


SPOT = {  'SEK','NOK','JPY','CHF','CAD','NZD','GBP','AUD','ZAR','SGD','THB', 'EUR' }

NDF = {  'INR','IDR','TWD','KRW' }

#..................................................................FX Convert.....................................................................
CCY_INDEX = {
                'HKD':"MSFXHKD Index",
                'CNY':"MSFXCNY Index",
                'CNH':"MSFXCNY Index"
            }
CCY_CURRENCY = {
                'SEK':'USDSEK BGNL Curncy',
                'NOK':'USDNOK BGNL Curncy',
                'JPY':'USDJPY BGNL Curncy',
                'CHF':'USDCHF BGNL Curncy',
                'CAD':'USDCAD BGNL Curncy',
                'NZD':'NZDUSD BGNL Curncy',
                'GBP':'GBPUSD BGNL Curncy',
                'AUD':'AUDUSD BGNL Curncy',
                'ZAR':'USDZAR BGNL Curncy',
                'SGD':'USDSGD BGNL Curncy',
                'THB':'USDTHB BGNL Curncy',
                'EUR':'EURUSD BGNL Curncy',

                'INR':'IRN+1M BGNL Curncy',
                'IDR':'IHN+1M BGNL Curncy',
                'TWD':'NTN+1M BGNL Curncy',
                'KRW':'KWN+1M BGNL Curncy',
            }


#..................................................................FX Interest.....................................................................


CCY_TICKER = {  
                'SEK':'USDSEK BGNL Curncy',
                'NOK':'USDNOK BGNL Curncy',
                'JPY':'USDJPY BGNL Curncy',
                'CHF':'USDCHF BGNL Curncy',
                'CAD':'USDCAD BGNL Curncy',
                'NZD':'NZDUSD BGNL Curncy',
                'GBP':'GBPUSD BGNL Curncy',
                'AUD':'AUDUSD BGNL Curncy',
                'ZAR':'USDZAR BGNL Curncy',
                'SGD':'USDSGD BGNL Curncy',
                'THB':'USDTHB BGNL Curncy',
                'EUR':'EURUSD BGNL Curncy',

                'INR':'IRN+1M BGNL Curncy',
                'IDR':'IHN+1M BGNL Curncy',
                'TWD':'NTN+1M BGNL Curncy',
                'KRW':'KWN+1M BGNL Curncy',
            }



CCY_INTEREST_TICKER =  {  
                'SEK':'SEKTN BGNL Curncy',
                'NOK':'NOKTN BGNL Curncy',
                'JPY':'JPYTN BGNL Curncy',
                'CHF':'CHFTN BGNL Curncy',
                'CAD':'CADTN BGNL Curncy',
                'NZD':'NZDTN BGNL Curncy',
                'GBP':'GBPTN BGNL Curncy',
                'AUD':'AUDTN BGNL Curncy',
                'ZAR':'ZARTN BGNL Curncy',
                'SGD':'SGDTN BGNL Curncy',
                'THB':'THBTN BGNL Curncy',
                'EUR':'EURTN BGNL Curncy',

                'INR':'IRN1M BGNL Curncy',
                'IDR':'IHN1M BGNL Curncy',
                'TWD':'NTN1M BGNL Curncy',
                'KRW':'KWN1M BGNL Curncy',
            }
USD_RATE_TICKER = 'USDR1T CMP Curncy'
ndf_FP_SCALE_map = {'IDR': 0,'INR': 2,'KRW': 0,'TWD': 0}


#..................................................................Instrument type.....................................................................

instrument_type_map = {"外汇现货":"FX","外汇衍生品":"FX","港股主板":"HK_STOCK","A股主板":"CN_STOCK","中小板":"CN_STOCK","创业板":"CN_STOCK"}

fx_ticker_dm_map = {
    'USDTWD': 'NTN+1M',
    'USDKRW': 'KWN+1M',
    'USDINR': 'IRN+1M',
    'USDIDR': 'IHN+1M',
}

fx_bbg_dividends_ccy_map = {
    'NTN+1M': ['USD','TWD'],
    'KWN+1M': ['USD','KRW'],
    'IRN+1M': ['USD','INR'],
    'IHN+1M': ['USD','IDR'],
}

exchange_id_exchange_map = {
    2:"SSE",
    3:"SZSE",
    4:"HKEX",
    95:"SZSE"
}
