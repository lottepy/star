package com.magnumresearch.aqumon.riskengine.adapter.boci.pojo;

public enum BOCIOrderType {
    MKT(1), // Market Order
    LMT(2), // Limit Order
    AMO(3), // Auction Order
    ALO(4), // Auction Limit Order
    ELO(5), // Enhanced Limit Order
    SLO(5), // Special Limit Order
    ;

    private int type;

    BOCIOrderType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}
