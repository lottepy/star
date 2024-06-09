package com.magnumresearch.aqumon.riskengine.adapter.ib.signal;

import com.magnumresearch.aqumon.trading.model.SubAccountSummary;

public class BrokerAccountSummaryUpdatesSignal {

    private SubAccountSummary subAccountSummary;

    public BrokerAccountSummaryUpdatesSignal(SubAccountSummary subAccountSummary) {
        this.subAccountSummary = subAccountSummary;
    }

    public SubAccountSummary getAccountSummaryMap() {
        return subAccountSummary;
    }

    public void setAccountSummaryMap(SubAccountSummary subAccountSummary) {
        this.subAccountSummary = subAccountSummary;
    }
}
