package com.magnumresearch.aqumon.riskengine.constants;

public enum DataType {
    BIGDECIMAL(1),
    INTEGER(2),
    LONG(3),
    STRING(4);

    private final int type;

    DataType(int type) {
        this.type = type;
    }

    public int getDataTypeValue() {
        return type;
    }

    public static DataType getDataType(String type) throws NoSuchFieldException {
        if (type.equalsIgnoreCase("BigDecimal"))
            return DataType.BIGDECIMAL;
        else if (type.equalsIgnoreCase("Integer") || type.equalsIgnoreCase("int"))
            return DataType.INTEGER;
        else if (type.equalsIgnoreCase("Long"))
            return DataType.LONG;
        else if (type.equalsIgnoreCase("String"))
            return DataType.STRING;
        else
            throw new NoSuchFieldException("[RiskEngine][DataType] input type is not support in DataType Enum: Input=" + type);
    }
}
