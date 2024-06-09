package com.magnumresearch.aqumon.riskengine.adapter.ib.signal;

public class ManagedAcctSignal {

    private String managedAcctStr;

    public ManagedAcctSignal(String managedAcctStr) {
        this.managedAcctStr = managedAcctStr;
    }

    public String getManagedAcctStr() {
        return managedAcctStr;
    }

    public void setManagedAcctStr(String managedAcctStr) {
        this.managedAcctStr = managedAcctStr;
    }
}
