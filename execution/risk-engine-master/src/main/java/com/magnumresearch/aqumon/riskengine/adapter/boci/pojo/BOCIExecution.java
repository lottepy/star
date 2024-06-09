package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

import lombok.Data;

import java.math.BigDecimal;

@Data
public class BOCIExecution {
    String executionId;
    BigDecimal filledQuantity;
    BigDecimal filledPrice;
    String filledTime;
}
