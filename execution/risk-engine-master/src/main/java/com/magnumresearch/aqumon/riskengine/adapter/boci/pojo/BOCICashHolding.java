package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class BOCICashHolding {
    private String currencyCode = "";
    private BigDecimal tradeDateBalance = BigDecimal.ZERO;
    private BigDecimal settlementDateBalance = BigDecimal.ZERO;
    private BigDecimal revaluationValue = BigDecimal.ZERO;
    private BigDecimal marketValue = BigDecimal.ZERO;
    private BigDecimal portfolioValue = BigDecimal.ZERO;
    private BigDecimal unavailableCash = BigDecimal.ZERO;
    private BigDecimal tradeDateInterestReceivable = BigDecimal.ZERO;
    private BigDecimal tradeDateInterestPayable = BigDecimal.ZERO;
    private BigDecimal settleDateInterestReceivable = BigDecimal.ZERO;
    private BigDecimal settleDateInterestPayable = BigDecimal.ZERO;
}
