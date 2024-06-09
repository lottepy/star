package com.magnumresearch.aqumon.riskengine.constants;

public enum RiskGroupType {
    //Quote Validation Service
    QUOTE(0),
    //Holding Validation Service
    HOLDING(1),
    //SubAccountSummary Validation Service
    SUBACCOUNTSUMMARY(2),
    //CashInfo Map validation service
    CASHINFOMAP(3),
    //Instrument field validation service
    INSTRUMENTFIELD(4),
    //Customized numeric field validation service
    SPREAD(5);

    private final int type;

    RiskGroupType(int type) {
        this.type = type;
    }
}