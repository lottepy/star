package com.magnumresearch.aqumon.riskengine.adapter.ib.signal;

public class NextValidIdSignal {

    int nextValidId;

    public NextValidIdSignal(int nextValidId) {
        this.nextValidId = nextValidId;
    }

    public int getNextValidId() {
        return nextValidId;
    }
}
