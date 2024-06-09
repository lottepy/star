package com.magnumresearch.aqumon.riskengine.constants;

public enum CalculationType {
    //difference
    DIFFERENCE(0),
    //less than
    LESSTHAN(1),
    //less or equal
    LESSOREUQAL(2),
    //larger than
    GREATERTHAN(3),
    //larger or euqal
    GREATEROREQUAL(4),
    //equal
    EQUAL(5),
    //not equal
    NOTEQUAL(6);

    private final int type;

    CalculationType(int type) {
        this.type = type;
    }
}

