/*
 *  Copyright (c) 2018, Magnum Research Ltd. All rights reserved.
 *
 * This program and the accompanying materials (“Program”)
 * whether on any media or in any form,
 *
 * are exclusive property of Magnum Research Limited (“Magnum”).
 * Without prior written authorization from Magnum,
 * any person shall not reproduce, modify, summarize,
 * reverse-engineer, decompile or disassemble the Program,
 * or disclose any part of this Program to any other person.
 *
 * Magnum reserves all rights not expressly stated herein.
 *
 *
 */

package com.magnumresearch.aqumon.riskengine.adapter.ib;

import com.magnumresearch.aqumon.riskengine.adapter.BaseBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.ib.connector.IbConnector;
import com.magnumresearch.aqumon.riskengine.adapter.ib.connector.IbConnectorPool;
import com.magnumresearch.aqumon.riskengine.dao.SubAccountSummaryDao;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Slf4j
@ConditionalOnProperty(prefix = "adapter.ib", value = {"enabled"})
public class IbBrokerAdapter extends BaseBrokerAdapter {

    @Autowired
    IbConnectorPool ibConnectorPool;
    @Autowired
    SubAccountSummaryDao subAccountSummaryDao;

    @Override
    public BrokerType getBrokerType() {
        return BrokerType.IB;
    }

    @Override
    public boolean getBrokerAccountStatus(String brokerAccount) {
        if (inMaintenance) {
            return false;
        }
        boolean status;
        try {
            status = ibConnectorPool.isBrokerAccountConnected(brokerAccount);
        } catch (TradingException e) {
            status = false;
            e.printStackTrace();
        }
        return status;
    }

    public void refreshIbBrokerAccountConfig() {
        ibConnectorPool.init();
    }

    @Override
    public void disconnect(String brokerAccount) throws TradingException {
        ibConnectorPool.disconnect(brokerAccount);
    }

    @Override
    //Get account info and account summary info
    public SubAccount querySubAccountSummary(SubAccount subAccount) throws TradingException {
        String brokerAccount = subAccount.getAccount().getBrokerAccount();
        IbConnector ibConnector = ibConnectorPool.acquireConnectedConnector(brokerAccount);
        try {
            SubAccountSummary resultSubAccountSummary = ibConnector.getModelAccountUpdates(brokerAccount, null);
            subAccount.getSubAccountSummary().copyFromAccountSummary(resultSubAccountSummary);
            return subAccount;
        } finally {
            ibConnectorPool.releaseAndDisconnectConnector(ibConnector);
        }
    }

    @Override
    public List<Holding> queryHolding(String brokerAccount) throws TradingException {
        IbConnector ibConnector = ibConnectorPool.acquireConnectedConnector(brokerAccount);
        try {
            // modelCode为null效果和getPositions()一样, 但是可以获取子账户更多的账户的持仓
            return ibConnector.getModelPositions(brokerAccount, null);
//            return ibConnector.getPositions().stream().filter(h -> h.getBrokerAccount().equals(brokerAccount)).collect(Collectors.toList());
        } finally {
            ibConnectorPool.releaseAndDisconnectConnector(ibConnector);
        }
    }

    public List<Order> queryOpenOrders(String brokerAccount) throws TradingException {
        IbConnector ibConnector = ibConnectorPool.acquireConnectedConnector(brokerAccount);
        try {
            return ibConnector.getOpenOrders();
        } finally {
            ibConnectorPool.releaseAndDisconnectConnector(ibConnector);
        }
    }

    public List<Order> queryCompletedOrders(String brokerAccount) throws TradingException {
        IbConnector ibConnector = ibConnectorPool.acquireConnectedConnector(brokerAccount);
        try {
            return ibConnector.getCompletedOrders();
        } finally {
            ibConnectorPool.releaseAndDisconnectConnector(ibConnector);
        }
    }

    public List<Execution> queryExecutions(String brokerAccount) throws TradingException {
        IbConnector ibConnector = ibConnectorPool.acquireConnectedConnector(brokerAccount);
        try {
            return ibConnector.getExecutions();
        } finally {
            ibConnectorPool.releaseAndDisconnectConnector(ibConnector);
        }
    }

    @Override
    public Map<String, Map<String, String>> getHealthStatus() {
        return ibConnectorPool.getHealthStatus();
    }


    @Override
    public boolean isAccountValid(String brokerAccount) throws TradingException {
        IbConnector ibConnector = null;
        try {
            ibConnector = ibConnectorPool.acquireConnectedConnector(brokerAccount);
            ibConnector.getModelAccountUpdates(brokerAccount, null);
        } finally {
            ibConnectorPool.releaseAndDisconnectConnector(ibConnector);
        }
        return true;
    }
}
