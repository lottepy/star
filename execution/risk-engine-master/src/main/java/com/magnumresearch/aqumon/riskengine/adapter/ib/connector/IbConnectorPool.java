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

import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.common.feign.LarkRobotClient;
import com.magnumresearch.aqumon.riskengine.dao.BrokerAccountConfigDao;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.BrokerAccountConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@Component
@Slf4j
@ConditionalOnProperty(prefix = "adapter.ib", value = {"enabled"})
public class IbConnectorPool {

    @Autowired
    ApplicationContext applicationContext;
    @Autowired
    BrokerAccountConfigDao brokerAccountConfigDao;
    @Autowired
    LarkRobotClient larkRobotClient;

    @Value("${adapter.ib.connectedSize}")
    private int connectedSize;

    private final Map<String, BlockingQueue<IbConnector>> connectorsMap = new HashMap<>();
    private final Map<String, List<String>> connectedListMap = new HashMap<>();
    private final List<IbConnector> healthCheckerList = new ArrayList<>();
    private final Map<String, Map<String, String>> healthStatus = new HashMap<>();

    @PostConstruct
    public void init() {
        Set<String> brokerAccountSet = getIbConnectorBrokerAccounts();
        for (String brokerAccount : brokerAccountSet) {
            insertQueue(brokerAccount, new LinkedBlockingQueue<>());
            IbConnector ibConnector = (IbConnector) applicationContext.getBean("ibConnector");
            ibConnector.setBrokerAccount(brokerAccount);
            List<Map<String, Object>> hostPortList = getIbConnectorHostPortList(brokerAccount);
            ibConnector.setHostPortList(hostPortList);
            getPool(brokerAccount).add(ibConnector);
        }

        List<Map<String, Object>> hostPortList = getHostPortList();
        log.info(hostPortList.toString());
        for (Map<String, Object> hostPort : hostPortList) {
            IbConnector ibConnector = (IbConnector) applicationContext.getBean("ibConnector");
            String brokerAccount = brokerAccountConfigDao.findFirstByIpAndPortOrderByBrokerAccountDesc(
                    (String) hostPort.get("HOST"), (Integer) hostPort.get("PORT")).getBrokerAccount();

            ibConnector.setBrokerAccount(brokerAccount);
            ibConnector.setHostPortList(Collections.singletonList(hostPort));
            healthCheckerList.add(ibConnector);
        }
    }

    public boolean isBrokerAccountConnected(String brokerAccount) throws TradingException {
        List<String> hostPortList;
        List<String> healthyHostPortList;
        IbConnector ibConnector = acquireConnector(brokerAccount);
        try {
            hostPortList = ibConnector.getHostPortList().stream()
                    .map(hostPortMap -> hostPortMap.get("HOST").toString() + ":" + hostPortMap.get("PORT").toString())
                    .collect(Collectors.toList());

            healthyHostPortList = getHealthStatus().entrySet().stream()
                    .filter(hostPortMap -> hostPortMap.getValue().get("status").equals("connected"))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());
        } finally {
            releaseConnector(ibConnector);
        }
        return hostPortList.stream().anyMatch(healthyHostPortList::contains);

    }

    @Scheduled(fixedDelay = 120 * 1000)
    public void healthCheck() {
        log.info(healthCheckerList.toString());
        for (IbConnector healthChecker : healthCheckerList) {
            boolean healthFlag = false;
            Map<String, String> healthInfo = new HashMap<>();
            String identifier = healthChecker.getHostPortList().get(0).get("HOST").toString()
                    + ":" + healthChecker.getHostPortList().get(0).get("PORT").toString();
            String brokerAccount = healthChecker.getBrokerAccount();
            log.info("[healthCheck] host({}) brokerAccount({})", identifier, brokerAccount);
            healthChecker.setBrokerAccount("999999"); // connect前设置为999999为了得到不重复的连接号, 但是查询结束需要设置回来
            if (healthChecker.connect()) {
                try {
                    healthChecker.getModelPositions(brokerAccount, null);
                    healthFlag = true;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            healthChecker.setBrokerAccount(brokerAccount);
            healthInfo.put("last_checked", new Date().toString());
            if (healthFlag) {
                healthInfo.put("status", "connected");
            } else {
                healthInfo.put("status", "disconnected");
            }
            healthStatus.put(identifier, healthInfo);
            healthChecker.disconnect();
            log.info(String.valueOf(healthStatus));

        }
    }

    public Map<String, Map<String, String>> getHealthStatus() {
        return healthStatus;
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

    public List<Map<String, Object>> getHostPortList() {
        List<String> hostPortList = brokerAccountConfigDao.findAllDistinctHostPort();

        return hostPortList
                .stream()
                .map(hostPort -> {
                    Map<String, Object> hostPortMap = new HashMap<>();
                    hostPortMap.put("HOST", hostPort.split(":")[0]);
                    hostPortMap.put("PORT", Integer.parseInt(hostPort.split(":")[1]));
                    return hostPortMap;
                })
                .collect(Collectors.toList());

    }

    public BlockingQueue<IbConnector> getPool(String port) {
        return connectorsMap.get(port);
    }

    public void insertQueue(String brokerAccount, BlockingQueue<IbConnector> queue) {
        connectorsMap.put(brokerAccount, queue);
    }

    public IbConnector acquireConnectedConnector(String brokerAccount) throws TradingException {
        IbConnector ibConnector = acquireConnector(brokerAccount);
        try {
            boolean connectFlag = ibConnector.connect();
            if (!connectFlag) {
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_CONNECTOR_ERROR,
                        "[acquireConnectedConnector] IbConnector cannot connect with tws, brokerAccount: " +
                                brokerAccount);
            }
            log.info("[IB Adapter][acquireConnectedConnector] successful connect, brokerAccount: {}", brokerAccount);
            markConnected(ibConnector);
        } catch (Exception e) {
            releaseConnector(ibConnector);
            disconnect(ibConnector.getBrokerAccount());
            larkRobotClient.sendMessage("[acquireConnectedConnector] " +
                    "BrokerAccount(" + brokerAccount + ") cannot connect to tws!");
            throw e;
        }
        return ibConnector;
    }

    private void markConnected(IbConnector ibConnector) throws TradingException {
        List<String> connectedBrokerAccountList;
        if (!connectedListMap.containsKey(ibConnector.getActiveKey())) {
            connectedBrokerAccountList = new ArrayList<>();
            connectedListMap.put(ibConnector.getActiveKey(), connectedBrokerAccountList);
        } else {
            connectedBrokerAccountList = connectedListMap.get(ibConnector.getActiveKey());
        }
        if (!connectedBrokerAccountList.contains(ibConnector.getBrokerAccount())) {
            if (connectedBrokerAccountList.size() >= connectedSize) {
                String closeBrokerAccount = connectedBrokerAccountList.get(0);
                disconnect(closeBrokerAccount);
            }
        } else {
            connectedBrokerAccountList.remove(ibConnector.getBrokerAccount()); // 如果已经是连接状态的brokerAccount也会被移除队列, 然后排在队尾
        }
        connectedBrokerAccountList.add(ibConnector.getBrokerAccount());
    }

    public void disconnect(String brokerAccount) throws TradingException {
        IbConnector ibConnector = acquireConnector(brokerAccount);
        try {
            if (connectedListMap.containsKey(ibConnector.getActiveKey())) {
                connectedListMap.get(ibConnector.getActiveKey()).remove(ibConnector.getBrokerAccount());
            }
            ibConnector.disconnect();
            log.info("[IB Adapter][disconnect] successful disconnect brokerAccount: {}", brokerAccount);
        } finally {
            releaseConnector(ibConnector);
        }
    }

    public IbConnector acquireConnector(String brokerAccount) throws TradingException {
        if (!connectorsMap.containsKey(brokerAccount)) {
            log.error("[IB Adapter][acquireConnector] Failed to acquire ib connector, brokerAccount: {}", brokerAccount);
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_CONNECTOR_ERROR,
                    "[IB Adapter][acquireConnector] There is not connector in connectorsMap, brokerAccount:" +
                            brokerAccount);
        }
        IbConnector ibConnector;
        try {
            ibConnector = connectorsMap.get(brokerAccount).poll(40L, TimeUnit.SECONDS);
            Assert.notNull(ibConnector, "[IB Adapter][acquireConnector] ibConnector is null!");
        } catch (Exception error) {
            log.error("[IB Adapter][acquireConnector] Failed to acquire ib connector. ", error);
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_CONNECTOR_ERROR,
                    "[IB Adapter][acquireConnector] Failed to acquire ib connector for this brokerAccount:" +
                            brokerAccount);
        }
        log.info("[IB Adapter][acquireConnector] successful acquire connector, brokerAccount: {}",
                ibConnector.getBrokerAccount());
        return ibConnector;
    }

    public void releaseConnector(IbConnector connector) throws TradingException {
        try {
            if (connector != null) {
                BlockingQueue blockingQueue = connectorsMap.get(connector.getBrokerAccount());
                blockingQueue.put(connector);
                log.info("[IB Adapter][releaseConnector] successful release connector, brokerAccount: {}",
                        connector.getBrokerAccount());
            }
        } catch (InterruptedException error) {
            log.error("[IB Adapter][releaseConnector] Failed to return ib connector. ");
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_CONNECTOR_ERROR,
                    "Can not get the connector for this brokerAccount");
        }
    }

    public void releaseAndDisconnectConnector(IbConnector ibConnector) throws TradingException {
        try {
            if (Objects.nonNull(ibConnector))
                ibConnector.disconnect();
        } finally {
            releaseConnector(ibConnector);
        }
    }
}
