import pandas as pd

from quantcycle.app.data_manager.data_loader.data_center.mapping import \
    CCY2USDCCP
from quantcycle.app.data_manager.utils.csv_import import backup_mnemonic_from_info_df

from ..method_base import MethodBase


class MethodCCYFX(MethodBase):
    def __init__(self):
        super().__init__()
        self.is_final_step = True
        self.output_data_type = 'df'
        self.user_symbols = []

    def on_bundle(self, data_bundle, data_collection):
        '''
            init method
        '''
        self.acc_ccy = data_bundle.get('SymbolArgs',{}).get('AccountCCY','USD')
        self.start_ts, self.end_ts = data_bundle['StartTS'], data_bundle['EndTS']
        self.symbols = data_bundle["Symbol"]
        self.user_symbols = data_bundle.get('SymbolArgs',{}).get('UserSymbolList',[])
        
        # for STOCK using LOCAL account ccy only
        if self.acc_ccy == 'LOCAL':
            pass
        else:
            # To get ccys first
            self.create_ccys_data_mapping(data_bundle)
            self.map_data(data_collection)
            self._set_ccys()
            self.data_mapping = {}
            # create data mapping
            self.create_data_mapping(data_bundle)
            # collet mapped data
            self.map_data(data_collection)

    def _set_ccys(self):
        self.ccys = [
            self.data_dict[sym+' info'].get('trading_currency', 'USD') if sym != 'USDUSD' else 'USD' for sym in self.symbols]

    def create_ccys_data_mapping(self, data_bundle):
        info_df = data_bundle.get('SymbolArgs',{}).get("info_df", pd.DataFrame())
        data_source = data_bundle.get('SymbolArgs',{}).get("DataSource", "")
        symbols_with_source = [i+" "+data_source if data_source else i for i in self.symbols]
        symbols_with_source = backup_mnemonic_from_info_df(self.symbols, symbols_with_source, info_df)
        self.data_mapping.update(
            {sym+' info': symbols_with_source[i]+"/info" for i, sym in enumerate(self.symbols) if sym != 'USDUSD'})

    def create_data_mapping(self, data_bundle):
        data_source = data_bundle.get('SymbolArgs',{}).get("DataSource", "")
        #TODO
        self.data_mapping.update(
            {ccy+' fxrate': CCY2USDCCP[ccy]+" "+data_source+"/fxrate" if data_source else CCY2USDCCP[ccy]+"/fxrate" for ccy in self.ccys if ccy != 'USD'})

    def run(self):
        if len(self.user_symbols) != 0:
            output_symbols = self.user_symbols
        else:
            output_symbols = self.symbols
        
        if self.acc_ccy == 'LOCAL':
            return pd.DataFrame(index=[self.start_ts], columns=self.symbols).fillna(1), output_symbols, ['fx_rate']
        else:
            # Adjust the symbols name for HKDUSD, HKDCNY                     
            self.symbols = [''.join([sym[3:],sym[:3]]) if 'USDCNY' == sym or 'USDHKD' == sym else sym for sym in self.symbols]
            # FX rate
            fx_df = _construct_ccyfx_df(self.data_dict, self.ccys, self.symbols, self.user_symbols, self.start_ts, self.acc_ccy)

            return fx_df, output_symbols, ['fx_rate']


def _is_ccy_right_leg(ccy: str) -> bool:
    return CCY2USDCCP[ccy][-3:] == ccy


def _construct_ccyfx_df(data, ccys: str, symbols: list, user_symbols:list, start_ts: int, acc_ccy: str = 'USD') -> pd.DataFrame:
    list_df = []
    df_usd = pd.DataFrame(columns=['USD'], index=[start_ts]) # TODO index timestamp
    for ccy in ccys:
        if ccy == 'USD':
            list_df.append(df_usd)
        elif _is_ccy_right_leg(ccy):
            list_df.append(1 / data[ccy+' fxrate'])
        else:
            list_df.append(data[ccy+' fxrate'])
    fx_df = pd.concat(
        list_df, axis=1).fillna(method='ffill').fillna(1.0)
    if len(user_symbols) != 0:
        if len(user_symbols) != len(fx_df.columns):
            fx_df = fx_df.iloc[:,:len(user_symbols)]
        fx_df.columns = user_symbols
    else:
        fx_df.columns = symbols

    # calculate ccp against base_currency
    if acc_ccy != 'USD':
        acc_ccy_rate = data[acc_ccy+' fxrate'].iloc[:,0]
        if _is_ccy_right_leg(acc_ccy):
            acc_ccy_rate = 1 / acc_ccy_rate
        fx_df = fx_df.mul(1/acc_ccy_rate, axis=0)
    return fx_df