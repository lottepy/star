package com.magnumresearch.aqumon.riskengine.adapter.ib.signal;

import com.magnumresearch.aqumon.trading.model.Execution;

import java.util.List;

public class ExecutionsSignal {

    List<Execution> executions;

    public ExecutionsSignal(List<Execution> executions) {
        this.executions = executions;
    }

    public List<Execution> getExecutions() {
        return executions;
    }

    public void setExecutions(List<Execution> executions) {
        this.executions = executions;
    }
}
