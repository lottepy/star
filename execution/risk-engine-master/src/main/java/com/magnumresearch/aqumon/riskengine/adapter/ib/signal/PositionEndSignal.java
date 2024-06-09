package com.magnumresearch.aqumon.riskengine.adapter.ib.signal;

import com.magnumresearch.aqumon.trading.model.Holding;

import java.util.LinkedList;
import java.util.List;

public class PositionEndSignal {
    private List<Holding> holdings;

    public PositionEndSignal(){
        this.holdings = new LinkedList<>();
    }

    public PositionEndSignal(List<Holding> holdings){
        this.holdings = new LinkedList<>(holdings);
    }

    public List<Holding> getHoldings(){
        return holdings;
    }
}
