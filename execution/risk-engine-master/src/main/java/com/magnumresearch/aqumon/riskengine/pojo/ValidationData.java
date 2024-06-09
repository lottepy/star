package com.magnumresearch.aqumon.riskengine.pojo;

import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import lombok.Data;

import java.util.Map;

@Data
public class ValidationData {
    private ValidationKey validationKey;
    private Map<String, ComparisonResult> fieldData;

    public ValidationData(ValidationKey validationKey, Map<String, ComparisonResult> fieldData) {
        this.validationKey = validationKey;
        this.fieldData = fieldData;
    }
}
