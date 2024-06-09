package com.magnumresearch.aqumon.riskengine.constants;

public enum CompareType {
    //Compare number based on percentage of difference
    PERCENTAGE(0),
    //Compare number based on absolute value of difference
    ABSOLUTE(1),
    //Check Strings are equal, case insensitive
    IGNORECASE(2),
    //Check Strings are equal, case sensitive
    WITHCASE(3),
    //Check if an object is not null
    NONNULL(4),
    //Check if an object is null
    ISNULL(5);

    private final int type;

    CompareType(int type) {
        this.type = type;
    }
}

