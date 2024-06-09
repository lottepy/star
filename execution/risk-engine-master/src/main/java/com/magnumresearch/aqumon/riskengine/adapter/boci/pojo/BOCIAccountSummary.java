package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class BOCIAccountSummary {
    private String currencyCode = "";
    private BigDecimal tradeDateBalance = BigDecimal.ZERO;
    private BigDecimal settlementDateBalance = BigDecimal.ZERO;
    private BigDecimal revaluationValue = BigDecimal.ZERO;
    private BigDecimal marketValue = BigDecimal.ZERO;
    private BigDecimal portfolioValue = BigDecimal.ZERO;
    private BigDecimal eligibleBuyingPower = BigDecimal.ZERO;
    private BigDecimal nonEligibleBuyingPower = BigDecimal.ZERO;
    private BigDecimal availableTPlus2Limit = BigDecimal.ZERO;
    private BigDecimal tPlus2Limit = BigDecimal.ZERO;
    private BigDecimal loanLimit = BigDecimal.ZERO;
    private BigDecimal unavailableCash = BigDecimal.ZERO;
    private BigDecimal settleDateInterestReceivable = BigDecimal.ZERO;
    private BigDecimal settleDateInterestPayable = BigDecimal.ZERO;
    private BigDecimal totalMarginValue = BigDecimal.ZERO;
    private BigDecimal t0UnfilledSellAmount = BigDecimal.ZERO;
    private BigDecimal t0UnfilledBuyAmount = BigDecimal.ZERO;
}
