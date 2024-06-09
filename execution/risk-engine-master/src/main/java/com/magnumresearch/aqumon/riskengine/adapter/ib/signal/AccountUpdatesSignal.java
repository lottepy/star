package com.magnumresearch.aqumon.riskengine.adapter.ib.signal;

import com.magnumresearch.aqumon.trading.constants.CurrencyType;
import com.magnumresearch.aqumon.trading.pojo.CashInfo;

import java.util.Map;

public class AccountUpdatesSignal {

    Map<CurrencyType, CashInfo> cashInfoMap;

    public AccountUpdatesSignal(Map<CurrencyType, CashInfo> cashInfoMap) {
        this.cashInfoMap = cashInfoMap;
    }

    public Map<CurrencyType, CashInfo> getCashInfoMap() {
        return cashInfoMap;
    }

    public void setCashInfoMap(Map<CurrencyType, CashInfo> cashInfoMap) {
        this.cashInfoMap = cashInfoMap;
    }
}
