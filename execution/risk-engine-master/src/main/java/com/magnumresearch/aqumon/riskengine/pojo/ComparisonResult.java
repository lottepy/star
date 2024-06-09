package com.magnumresearch.aqumon.riskengine.pojo;

import com.magnumresearch.aqumon.riskengine.constants.CalculationType;
import com.magnumresearch.aqumon.riskengine.constants.CompareType;
import lombok.Data;

@Data
public class ComparisonResult {
    private Object source;
    private Object target;
    private Double threshold;
    private CompareType compareType;
    private CalculationType calculationType;

    public ComparisonResult(Object source, Object target, Double threshold, CompareType compareType, CalculationType calculationType) {
        this.source = source;
        this.target = target;
        this.threshold = threshold;
        this.compareType = compareType;
        this.calculationType = calculationType;
    }

    public ComparisonResult(Object source, Object target) {
        this.source = source;
        this.target = target;
    }
}
