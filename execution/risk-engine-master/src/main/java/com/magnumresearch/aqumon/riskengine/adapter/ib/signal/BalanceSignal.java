package com.magnumresearch.aqumon.riskengine.adapter.ib.signal;

import com.magnumresearch.aqumon.trading.constants.CurrencyType;
import com.magnumresearch.aqumon.trading.pojo.CashInfo;

public class BalanceSignal {

    private CurrencyType currency;
    private CashInfo cashInfo;

    public BalanceSignal(CurrencyType currency) {
        this.currency = currency;
    }

    public BalanceSignal(CurrencyType currency, CashInfo cashInfo) {
        this.currency = currency;
        this.cashInfo = cashInfo;
    }

    public CurrencyType getCurrency() {
        return currency;
    }

    public void setCurrency(CurrencyType currency) {
        this.currency = currency;
    }

    public CashInfo getCashInfo() {
        return cashInfo;
    }

    public void setCashInfo(CashInfo cashInfo) {
        this.cashInfo = cashInfo;
    }
}
