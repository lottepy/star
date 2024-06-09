package com.magnumresearch.aqumon.riskengine.adapter.ib.signal;

import com.magnumresearch.aqumon.trading.model.Order;

import java.util.List;

public class OpenOrdersSignal {

    List<Order> orders;

    public OpenOrdersSignal(List<Order> orders) {
        this.orders = orders;
    }

    public List<Order> getOrders() {
        return orders;
    }

    public void setOrders(List<Order> orders) {
        this.orders = orders;
    }
}
