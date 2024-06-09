# PMS_Manager 

## Introduction

This module aims at calculating and recording of some indexes such as pnl and positon.
There are two sub_modules in this section: 
- `PorfolioManager`: Calculating and recording of some indexes such as pnl and positon of one strategy.
- `SuperPorfolioManager`: Managing the `PorfolioManager` for all strategies.





### Important Attribute of `PorfolioManager`

- symbol_cash
- dividends
- historial_pnl
- historial_position
- historial_commission_fee
- holding_value
- open_price

Sample codes:

*  Initialization
```
    def __init__(self, CASH, ccy_matrix, instrument_type_array):
        self.ccy_matrix = np.copy(ccy_matrix)
        self.n_security = self.ccy_matrix.shape[1]
        self.n_ccy = self.ccy_matrix.shape[0]
        self.current_holding = np.zeros(self.n_security)
        self.instrument_type_array = np.copy(instrument_type_array)
        self.commission_fee = np.zeros(self.n_security)
        self.acc_commission_fee = np.zeros(self.n_security)

        self.is_cn_equity_array = (self.instrument_type_array == InstrumentType.CN_STOCK.value)
        self.is_hk_equity_array = (self.instrument_type_array == InstrumentType.HK_STOCK.value)
        self.is_us_equity_array = (self.instrument_type_array == InstrumentType.US_STOCK.value)
        self.is_equity_array = self.is_cn_equity_array | self.is_hk_equity_array | self.is_us_equity_array
        self.non_equity_array = ~self.is_equity_array
        self.allow_sell_amount = self.current_holding * (self.is_cn_equity_array | self.is_hk_equity_array) + \
                                 MAX_SELL_AMOUNT * ~(self.is_cn_equity_array | self.is_hk_equity_array)

        self.init_cash = np.float(CASH)
        self.symbol_cash = np.zeros(self.n_security)
        self.dividends = np.zeros(self.n_security)
        self.current_data = np.zeros(self.n_security)
        self.current_fx_data = np.zeros(self.n_security)
        self.empty_array = np.zeros(self.n_security)
        self.is_tradable = np.array([True for symbol in range(self.n_security)])

        self.historial_pnl = List.empty_list(float64_1d_array_type)
        self.historial_position = List.empty_list(float64_1d_array_type)
        self.historial_commission_fee = List.empty_list(float64_1d_array_type)
        self.historial_time = List.empty_list(nb.float64)
```



*  Check order legality with cash and instrument_type and holding
    
```    
    def check_order(self,order):

        transaction = np.copy(order)
        transaction_amount = (self.current_fx_data) * self.current_data * transaction * self.is_equity_array
        if self.cash < np.sum(transaction_amount):
            transaction = np.zeros(len(transaction))
            transaction_status = (np.ones(len(transaction))==np.ones(len(transaction))) * PmsStatus.ILLEGAL.value
        else:
            legal_trade_identifier =  (-1 * transaction <= self.allow_sell_amount) & self.is_tradable
            transaction[~legal_trade_identifier] = 0
            transaction_status = legal_trade_identifier * PmsStatus.LEGAL.value + (~legal_trade_identifier) * PmsStatus.ILLEGAL.value
        return transaction , transaction_status
```


*  Reset field like base position
```    
    def reset_field_rollover_day(self,is_tradable):
        self.allow_sell_amount = self.current_holding * (self.is_cn_equity_array | self.is_hk_equity_array) + \
                                 MAX_SELL_AMOUNT * ~(self.is_cn_equity_array | self.is_hk_equity_array)
        if len(is_tradable) == len(self.is_tradable):
            self.is_tradable = np.copy(is_tradable)
```

*  Update field with order feedback
```    
    def receive_order_feedback(self, msg):
        transaction = msg[OrderFeedback.transaction.value]
        current_data = msg[OrderFeedback.current_data.value]
        current_fx_data = msg[OrderFeedback.current_fx_data.value]
        self.commission_fee = msg[OrderFeedback.commission_fee.value]
        self.acc_commission_fee += msg[OrderFeedback.commission_fee.value]

        self.allow_sell_amount += np.minimum(self.empty_array, transaction) * self.is_cn_equity_array + transaction * self.is_hk_equity_array
        self.calculate_spot(current_data, current_fx_data)
        transaction_amount = current_fx_data * current_data * transaction * self.is_equity_array
        self.symbol_cash -= transaction_amount
        self.current_holding += transaction
        self.current_data = np.copy(current_data)
        self.current_fx_data = np.copy(current_fx_data)
```

*  Calculate pms for future and FX
    
```    
    def calculate_spot(self, current_data, current_fx_data):
        pnl_base_ccy = calc_pnl_base_ccy_numba(current_data, self.current_data, self.current_holding,current_fx_data) * self.non_equity_array
        self.symbol_cash += pnl_base_ccy
        self.current_fx_data = current_fx_data
        self.current_data = current_data
```        
        


*  Calculate pnl from dividends
```    
    def calculate_rate(self, current_rate, current_data, current_fx_data):
        self.calculate_spot(current_data, current_fx_data)
        pnl_base_ccy = calc_rate_base_ccy(self.current_holding,current_rate, current_data, current_fx_data)
        self.dividends += pnl_base_ccy
```

*  Capture the current results in history
```    
    def capture(self, current_time):
        self.historial_time.append(current_time)
        self.historial_pnl.append(np.copy(self.pnl))
        self.historial_position.append(np.copy(self.current_holding))
        self.historial_commission_fee.append(np.copy(self.commission_fee))
        self.commission_fee = np.zeros(self.n_security)
```

*  Some properties
```
    @property
    def holding_value(self):
        return (self.current_fx_data) * self.current_data * self.current_holding * self.is_equity_array

    @property
    def symbol_cash_with_fee(self):
        return self.symbol_cash - self.acc_commission_fee + self.dividends

    @property
    def cash(self):
        return self.init_cash + np.sum(self.symbol_cash_with_fee)

    @property
    def pnl(self):
        return self.symbol_cash_with_fee + self.holding_value

    @property
    def pv(self):
        return self.init_cash + np.sum(self.pnl)

    @property
    def open_price(self):
        non_zero_holding_identifier = (self.current_holding != 0)
        current_holding = self.current_holding * non_zero_holding_identifier

        open_price_equity = -1 * (1/self.current_fx_data) * self.symbol_cash /current_holding
        open_price_non_equity = self.current_data
        open_price = open_price_equity * self.is_equity_array + open_price_non_equity *(self.non_equity_array)
        return open_price




```