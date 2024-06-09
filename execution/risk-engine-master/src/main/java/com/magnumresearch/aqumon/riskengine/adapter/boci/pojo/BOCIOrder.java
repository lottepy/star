package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

import com.magnumresearch.aqumon.trading.constants.OrderPriceType;
import lombok.Data;

import java.math.BigDecimal;

@Data
public class BOCIOrder {
    // 发单需要 {
    String accountId;
    String channel;
    String subChannel;
    String symbol;
    BigDecimal quantity;
    BigDecimal price;
    String side;
    String currencyCode;
    OrderPriceType orderType;
    String exchange;
    String tradeSolicitation;
    // } 发单需要

    String orderId;
    String exchangeId;
    String underlyingSymbol;
    String timeInForce;
    BigDecimal filledQuantity;
    BigDecimal filledPrice;
    BigDecimal outstandingQuantity;
    BigDecimal reducedQuantity;
    BigDecimal cancelledQuantity;
    String qualifier;
    BOCIOrderStatusType status;
    String createdTime;
    String touchPrice;
    BOCIFilledDetail filledDetail;
}
