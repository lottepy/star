package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

import lombok.Data;

import java.math.BigDecimal;
import java.util.Date;

@Data
public class BOCIHoldingDetail {
    private String exchangeId;
    private String marketCode;
    private String symbol;
    private String underlyingSymbol = null;
    private BigDecimal availableQuantity = BigDecimal.ZERO;
    private BigDecimal stockOnHold = BigDecimal.ZERO;
    private BigDecimal sharesOnHold = BigDecimal.ZERO;
    private BigDecimal t0UndueQuantity = BigDecimal.ZERO;
    private String currencyCode = "";
    private BigDecimal marketPrice = BigDecimal.ZERO;
    private BigDecimal unavailableQuantity = BigDecimal.ZERO;
    private BigDecimal marketValue = BigDecimal.ZERO;
    private BigDecimal marketValueInHkd = BigDecimal.ZERO;
    private BigDecimal marginRatio = BigDecimal.ZERO;
    private BigDecimal marginValue = BigDecimal.ZERO;
    private BigDecimal marginValueInHkd = BigDecimal.ZERO;
    private BigDecimal t0UpdateValue = BigDecimal.ZERO;
    private BigDecimal tPlusUndueQuantity = BigDecimal.ZERO;
    private BigDecimal tPlusUndueValue = BigDecimal.ZERO;
    private BigDecimal dueAndOverdueQuantity = BigDecimal.ZERO;
    private BigDecimal dueAndOverdueValue = BigDecimal.ZERO;
    private BigDecimal strikePrice = BigDecimal.ZERO;
    private Date expiryDate;
    private BigDecimal averageCost = BigDecimal.ZERO;
    private BigDecimal floatingProfitAndLoss = BigDecimal.ZERO;
    private BigDecimal averagePrice = BigDecimal.ZERO;
    private BigDecimal quantity = BigDecimal.ZERO;
    private BigDecimal nonSellableQuantity = BigDecimal.ZERO;
    private BigDecimal unsettledPlacementQuantity = BigDecimal.ZERO;
}