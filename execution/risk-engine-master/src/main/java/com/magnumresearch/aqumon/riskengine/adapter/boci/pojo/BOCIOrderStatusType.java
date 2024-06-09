package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

import com.magnumresearch.aqumon.trading.constants.OrderStatusType;

public enum BOCIOrderStatusType {
    //待申报
    PENDING(1),
    //成交中
    UNFILLED(2),
    //已成交
    PARTIAL_FILLED(3),
    //已成交
    PARTIAL_FILLED_CANCELLED(4),
    //已成交
    PARTIAL_FILLED_REJECTED(5),
    //已成交
    FILLED(6),
    //被拒绝
    REJECTED(7),
    //已成交
    PRICE_WARNING_PENDING(8),
    //已成交
    PRICE_WARNING_UNFILLED(9),
    //已成交
    PRICE_WARNING_PARTIAL_UNFILLED(10),
    //被拒绝
    EXCEPTION(11),
    //已撤销
    CANCELLED(12);

    private int status;

    BOCIOrderStatusType(int status) {
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    public static OrderStatusType getOrderStatusFromBOCIOrderStatus(BOCIOrderStatusType bociOrderStatusType) {
        switch (bociOrderStatusType) {
            case PENDING:
            case PRICE_WARNING_PENDING:
                return OrderStatusType.PENDING;
            case UNFILLED:
            case PARTIAL_FILLED:
            case PRICE_WARNING_UNFILLED:
            case PRICE_WARNING_PARTIAL_UNFILLED:
                return OrderStatusType.FILLING;
            case FILLED:
                return OrderStatusType.FILLED;
            case CANCELLED:
            case PARTIAL_FILLED_CANCELLED:
                return OrderStatusType.CANCELLED;
            case REJECTED:
            case PARTIAL_FILLED_REJECTED:
                return OrderStatusType.REJECTED;
            default:
                return OrderStatusType.UNKNOWN;
        }
    }
}
