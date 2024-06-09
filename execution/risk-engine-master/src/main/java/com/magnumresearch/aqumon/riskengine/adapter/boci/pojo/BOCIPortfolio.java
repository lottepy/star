package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

import lombok.Data;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Data
public class BOCIPortfolio {
    private BOCIAccountSummary accountSummary = null;
    private BOCIConsoildatedInvestmentPower consoildatedInvestmentPower = null;
    private BOCIConsolidatedCashHolding consolidatedCashHolding = null;
    private List<BOCIInvestmentPower> investmentPowers = new ArrayList<>();
    private List<BOCICashHolding> cashHoldings = new ArrayList<>();
    private List<BOCIHoldingDetail> holdingDetails = new ArrayList<>();
    private List<BOCIStructuredProductHoldingDetail> structuredProductHoldingDetails = new ArrayList<>();
}

@Data
class BOCIConsoildatedInvestmentPower {
    private String currencyCode = "";
    private BigDecimal availableCash = BigDecimal.ZERO;
    private BigDecimal tPlusUnsettledSellAmount = BigDecimal.ZERO;
    private BigDecimal t0FilledSellAmount = BigDecimal.ZERO;
    private BigDecimal totalMarginValue = BigDecimal.ZERO;
    private BigDecimal cashOnHold = BigDecimal.ZERO;
    private BigDecimal t0FilledBuyAmount = BigDecimal.ZERO;
    private BigDecimal t0TransactionFee = BigDecimal.ZERO;
    private BigDecimal tPlusUnsettledBuyAmount = BigDecimal.ZERO;
    private BigDecimal extraHoldFund = BigDecimal.ZERO;
    private BigDecimal usedTPlus2Limit = BigDecimal.ZERO;
    private BigDecimal totalUsedCash = BigDecimal.ZERO;
    private BigDecimal total = BigDecimal.ZERO;
    private BigDecimal t0UnfilledSellAmount = BigDecimal.ZERO;
    private BigDecimal t0UnfilledBuyAmount = BigDecimal.ZERO;
}

@Data
class BOCIConsolidatedCashHolding {
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

@Data
class BOCIInvestmentPower {
    private String currencyCode = "";
    private BigDecimal availableCash = BigDecimal.ZERO;
    private BigDecimal tPlusUnsettledSellAmount = BigDecimal.ZERO;
    private BigDecimal t0FilledSellAmount = BigDecimal.ZERO;
    private BigDecimal totalMarginValue = BigDecimal.ZERO;
    private BigDecimal cashOnHold = BigDecimal.ZERO;
    private BigDecimal t0FilledBuyAmount = BigDecimal.ZERO;
    private BigDecimal t0TransactionFee = BigDecimal.ZERO;
    private BigDecimal tPlusUnsettledBuyAmount = BigDecimal.ZERO;
    private BigDecimal extraHoldFund = BigDecimal.ZERO;
    private BigDecimal usedTPlus2Limit = BigDecimal.ZERO;
    private BigDecimal totalUsedCash = BigDecimal.ZERO;
    private BigDecimal total = BigDecimal.ZERO;
    private BigDecimal t0UnfilledSellAmount = BigDecimal.ZERO;
    private BigDecimal t0UnfilledBuyAmount = BigDecimal.ZERO;
}

@Data
class BOCIStructuredProductHoldingDetail {
    private BigDecimal cashOnHold = BigDecimal.ZERO;
    private String currencyCode = "";
    private BigDecimal effectiveDate = BigDecimal.ZERO;
    private String exchangeCode = "";
    private String marketCode = "";
    private String frequency = "";
    private String guarantee = "";
    private String instrumentCode = "";
    private String instrumentName = "";
    private String instrumentSubType = "";
    private BigDecimal koPrice = BigDecimal.ZERO;
    private BigDecimal leverageFactor = BigDecimal.ZERO;
    private BigDecimal maxOutstandingQty = BigDecimal.ZERO;
    private String optionSide = "";
    private String optionType = "";
    private BigDecimal outstandingQty = BigDecimal.ZERO;
    private BigDecimal premium = BigDecimal.ZERO;
    private BigDecimal requiredCollateralInAmt = BigDecimal.ZERO;
    private BigDecimal requiredCollateralInShare = BigDecimal.ZERO;
    private BigDecimal revaluationValue = BigDecimal.ZERO;
    private BigDecimal shareOnHold = BigDecimal.ZERO;
    private BigDecimal sharePerDay = BigDecimal.ZERO;
    private BigDecimal strikePrice = BigDecimal.ZERO;
    private Date terminationDate = null;
    private String underlyingStockCode = "";
}