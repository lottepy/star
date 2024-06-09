package com.magnumresearch.aqumon.riskengine.adapter;

import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.trading.constants.AdapterStatusType;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.Holding;
import com.magnumresearch.aqumon.trading.model.SubAccount;
import com.magnumresearch.aqumon.trading.pojo.AdapterStatus;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

import java.util.List;
import java.util.Map;


public abstract class BaseBrokerAdapter {
    @Autowired
    protected ApplicationContext applicationContext;

    private AdapterStatus adapterStatus;

    protected boolean inMaintenance = false;

    public abstract BrokerType getBrokerType();

    public BaseBrokerAdapter() {
        adapterStatus = new AdapterStatus();
        adapterStatus.setBrokerType(getBrokerType());
        adapterStatus.setHasError(false);
        adapterStatus.setStatus(AdapterStatusType.OFFLINE);
        adapterStatus.setErrorMsg("");
    }

    public synchronized void raiseAdapterStatus(AdapterStatusType status, boolean hasError, String errorMsg) {
        adapterStatus.setStatus(status);
        adapterStatus.setHasError(hasError);
        adapterStatus.setErrorMsg(errorMsg);
        adapterStatus.setBrokerType(getBrokerType());
    }

    public AdapterStatus getAdapterStatus() {
        return adapterStatus;
    }

    public boolean getBrokerAccountStatus(String brokerAccount) {
        return false;
    }

    public void connect() {
    }

    public void disconnect(String brokerAccount) throws TradingException {
    }

    public void keepAlive() throws TradingException {
    }

    public List<Holding> queryHolding(String brokerAccount) throws TradingException {
        return null;
    }

    public SubAccount querySubAccountSummary(SubAccount subAccount) throws TradingException {
        return null;
    }

    public void setInMaintenance(boolean inMaintenance){
        this.inMaintenance = inMaintenance;
    }

    public Map<String, Map<String, String>> getHealthStatus() throws TradingException{
        throw new TradingException(ResultStatusConstants.UNSUPPORTED);
    }

    public boolean isAccountValid(String brokerAccount) throws TradingException{
        return false;
    };
}
