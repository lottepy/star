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

package com.magnumresearch.aqumon.riskengine.adapter.ib.connector;

import com.magnumresearch.aqumon.riskengine.dao.BrokerAccountConfigDao;
import com.magnumresearch.aqumon.trading.model.BrokerAccountConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.IntStream;

//@Configuration
//@ConditionalOnProperty(prefix = "adapter.ib", value = {"enabled"})
public class IbConnectorPoolConfig {

    @Autowired
    ApplicationContext applicationContext;

    @Autowired
    BrokerAccountConfigDao brokerAccountConfigDao;

    @Bean
    @ConditionalOnProperty(prefix = "adapter.ib", value = {"enabled"})
    public IbConnectorPool ibConnectorPool() {
        IbConnectorPool pool = new IbConnectorPool();
        Set<String> brokerAccountSet = getIbConnectorBrokerAccounts();
        for (String brokerAccount : brokerAccountSet) {
            pool.insertQueue(brokerAccount, new LinkedBlockingQueue<>());
            IntStream.range(0, 1).forEach(e -> {
                IbConnector ibConnector = (IbConnector) applicationContext.getBean("ibConnector");
                ibConnector.setBrokerAccount(brokerAccount);
                List<Map<String, Object>> hostPortList = getIbConnectorHostPortList(brokerAccount);
                ibConnector.setHostPortList(hostPortList);
                pool.getPool(brokerAccount).add(ibConnector);
            });
        }
        return pool;
    }

    public Set<String> getIbConnectorBrokerAccounts() {
        Set<String> brokerAccountSet = new HashSet<>();
        List<BrokerAccountConfig> accountConfigList = brokerAccountConfigDao.findAll();
        for (BrokerAccountConfig accountConfig : accountConfigList) {
            brokerAccountSet.add(accountConfig.getBrokerAccount());
        }
        return brokerAccountSet;
    }

    public List<Map<String, Object>> getIbConnectorHostPortList(String brokerAccount) {
        List<Map<String, Object>> hostPortList = new ArrayList<>();
        List<BrokerAccountConfig> accountConfigList = brokerAccountConfigDao.findAllByBrokerAccount(brokerAccount);
        Collections.sort(accountConfigList);
        for (BrokerAccountConfig accountConfig : accountConfigList) {
            Map<String, Object> hostPortMap = new HashMap<>();
            hostPortMap.put("HOST", accountConfig.getIp());
            hostPortMap.put("PORT", accountConfig.getPort());
            hostPortList.add(hostPortMap);
        }
        return hostPortList;
    }
}
