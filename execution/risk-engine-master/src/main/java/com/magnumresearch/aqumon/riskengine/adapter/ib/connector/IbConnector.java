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

import com.ib.client.*;
import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.common.feign.LarkRobotClient;
import com.magnumresearch.aqumon.common.utils.RedisUtil;
import com.magnumresearch.aqumon.riskengine.adapter.ib.IbBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.ib.fa.pojos.FAInfoBase;
import com.magnumresearch.aqumon.riskengine.adapter.ib.pojo.Commission;
import com.magnumresearch.aqumon.riskengine.adapter.ib.signal.*;
import com.magnumresearch.aqumon.riskengine.adapter.ib.util.ContractUtils;
import com.magnumresearch.aqumon.riskengine.adapter.ib.util.OrderUtils;
import com.magnumresearch.aqumon.riskengine.dao.BrokerAccountConfigDao;
import com.magnumresearch.aqumon.riskengine.dao.OrderDao;
import com.magnumresearch.aqumon.trading.constants.*;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.Holding;
import com.magnumresearch.aqumon.trading.model.SubAccountSummary;
import com.magnumresearch.aqumon.trading.pojo.CashInfo;
import com.magnumresearch.aqumon.trading.pojo.InstrumentDataMaster;
import com.magnumresearch.aqumon.trading.utils.InstrumentUtil;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;

@Slf4j
@Component
@Scope("prototype")
@ConditionalOnProperty(prefix = "adapter.ib", value = {"enabled"})
public class IbConnector implements EWrapper {

    @Value("${account.ib.islive}")
    protected boolean ibIsLive;
    @Autowired
    IbBrokerAdapter ibBrokerAdapter;
    @Autowired
    OrderDao orderDao;
    @Autowired
    InstrumentUtil instrumentUtil;
    @Autowired
    BrokerAccountConfigDao brokerAccountConfigDao;
    @Autowired
    ContractUtils contractUtils;
    @Autowired
    LarkRobotClient larkRobotClient;
    @Autowired
    ThreadPoolTaskExecutor threadPoolTaskExecutor;

    public enum Keys {
        Positions, PositionsMulti, Balance, AccountSummaryUpdates, ManagedAccts, Orders, Executions, Connection, CancelStatus
    }

    public enum CancelStatusType {success, failed}

    private final EReaderSignal readerSignal;
    private final EClientSocket clientSocket;
    private Integer clientId = 1; // IB的clientId是我们的EnvId
    private String brokerAccount;
    private List<Map<String, Object>> hostPortList;
    private String activeHost = null;
    private Integer activePort = null;
    private AtomicInteger extOrderId = new AtomicInteger(0); // 下单使用的extorderid，第一次从ib取回来，之后维护不断递增
    private final AtomicInteger reqId = new AtomicInteger(0); // 请求使用的msgid
    private final Map<Keys, Object> dataCache = new ConcurrentHashMap<>();
    private final Map<Keys, BlockingQueue> blockingQueueMap = new ConcurrentHashMap<>();

    public IbConnector() {
        readerSignal = new EJavaSignal();
        clientSocket = new EClientSocket(this, readerSignal);
    }

    public List<Map<String, Object>> getHostPortList() {
        return this.hostPortList;
    }

    void setHostPortList(List<Map<String, Object>> hostPortList) {
        this.hostPortList = hostPortList;
    }

    public void setBrokerAccount(String brokerAccount) {
        this.brokerAccount = brokerAccount;
    }

    public String getBrokerAccount() {
        return brokerAccount;
    }

    public String getActiveKey() {
        return activeHost + activePort;
    }

    public boolean connect() {
        if (isConnected()) {
            log.warn("[IB Request][connect] The connector of " + brokerAccount + " is already connected!");
            return true;
        } else {
            this.blockingQueueMap.put(Keys.Connection, new LinkedBlockingQueue()); // 准备一个阻塞队列用于异步确认IB连接的完成
            for (Map<String, Object> hostPortMap : getHostPortList()) {  // 尝试连接host和port的组合，不行的就会尝试备用方案组
                try {
                    String currentHost = (String) hostPortMap.get("HOST");
                    Integer currentPort = (Integer) hostPortMap.get("PORT");
                    connectByHostAndPort(currentHost, currentPort);
                    if (isConnected()) {
                        log.info("[IB Request][connect] BrokerAccount({}) connect is success! host: {}, port: {}",
                                brokerAccount, currentHost, currentPort.toString());
                        NextValidIdSignal nextValidIdSignal = pollResponseFromQueue(Keys.Connection);
                        if (nextValidIdSignal.getNextValidId() == 0) {
                            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_CONNECTOR_ERROR);
                        } // 连接完成会收到确认信号
                        activeHost = currentHost;
                        activePort = currentPort;
                        return true;
                    } else {
                        log.warn("[IB Request][connect] BrokerAccount({}) Failed to connect! host: {}, port: {}",
                                brokerAccount, hostPortMap.get("HOST"), ((Integer) hostPortMap.get("PORT")).toString());
                    }
                } catch (Exception e) {
                    log.warn("[IB Request][connect] BrokerAccount({}) Failed to connect! host: {}, port: {}",
                            brokerAccount, hostPortMap.get("HOST"), ((Integer) hostPortMap.get("PORT")).toString(), e);
                    disconnect();
                }
            }
            return false;
        }
    }

    private void connectByHostAndPort(String host, Integer port) {
        //[环境号 envID]如果使用同一个clientid, 通道被占用就不可以连接上；
        // 而非正常关闭OMS会导致通道延迟断开，所以采用每次启动clientID不同的方式；
        // 一个tws客户端最多32个环境号连接
        clientId = OrderUtils.getEnvId(ibIsLive, brokerAccount);
        log.info("[IB Request][connectByHostAndPort] start connect... brokerAccount(" + brokerAccount + ") -> " +
                "host: " + host + " port: " + port + " clientID: " + clientId);
        clientSocket.eConnect(host, port, clientId);
        ibBrokerAdapter.raiseAdapterStatus(AdapterStatusType.ONLINE, false, "");
        EReader eReader = new EReader(clientSocket, readerSignal);
        eReader.start();
        new Thread(() -> {
            while (clientSocket.isConnected()) {
                readerSignal.waitForSignal();
                try {
                    eReader.processMsgs();
                } catch (Exception e) {
                    log.error("[IB Response][Thread eReceiver Signal] BrokerAccount(" + brokerAccount + ") Exception: ", e);
                    checkAndFixErrorMsg();
                }
            }
        }).start();
    }

    public void disconnect() {
        activeHost = null;
        activePort = null;
        clientSocket.eDisconnect();
    }

    boolean isConnected() {
        return clientSocket.isConnected();
    }

    private void pushResultIntoQueue(Object object, Keys type) {
        BlockingQueue queue = this.blockingQueueMap.get(type);
        if (queue == null) {
            log.warn("[IB Response][pushResultIntoQueue] No queue available for " + object);
        } else {
            try {
                while (queue.peek() != null) {
                    log.info("[IB Response][pushResultIntoQueue] [InputOrder-queue.remove]:, {}", queue.peek());
                    queue.remove();
                }
                boolean success = queue.offer(object, 5L, TimeUnit.SECONDS);
                if (!success) {
                    log.error("[IB Response][pushResultIntoQueue] Failed to push result after timeout: " + object);
                } else {
                    log.info("[IB Response][pushResultIntoQueue] Successfully put result into queue: " + object);
                }
            } catch (InterruptedException e) {
                log.warn("[IB Response][pushResultIntoQueue] Failed to push result due to interruption");
            }
        }
    }

    private <T> T pollResponseFromQueue(Keys type) throws TradingException {
        if (!blockingQueueMap.containsKey(type)) {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_ERROR,
                    "Queue not found, failed to get type = " + type.toString());
        }
        BlockingQueue queue = this.blockingQueueMap.get(type);
        try {
            Object res = queue.poll(30L, TimeUnit.SECONDS);
            if (res == null || (res.getClass().equals(Integer.class) && (Integer)res == -1)) {
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_ERROR,
                        "Receive null, failed to get type = " + type.toString());
            }
            return (T) res;
        } catch (InterruptedException e) {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_ERROR,
                    "Queue interrupted, failed to get type = " + type.toString());
        }
    }

    public List<Holding> getModelPositions(String brokerAccount, String modelCode) throws TradingException {
        this.blockingQueueMap.put(Keys.PositionsMulti, new LinkedBlockingQueue());
        this.dataCache.put(Keys.PositionsMulti, Collections.synchronizedList(new LinkedList<>()));
        int oneTimeReqId = reqId.incrementAndGet();
        this.clientSocket.reqPositionsMulti(oneTimeReqId, brokerAccount, modelCode);
        log.info("[IB Request][getModelPositions] Requesting model positions: {}, {}, {}",
                oneTimeReqId, brokerAccount, modelCode);
        PositionEndSignal signal;
        try {
            signal = pollResponseFromQueue(Keys.PositionsMulti);
        } finally {
            this.clientSocket.cancelPositionsMulti(oneTimeReqId);
            log.info("[IB Request][getModelPositions] Cancelling model positions, brokerAccount({}), oneTimeReqId({})",
                    brokerAccount, oneTimeReqId);
//            this.dataCache.remove(Keys.PositionsMulti);
        }
        return signal.getHoldings();
    }

    public synchronized List<com.magnumresearch.aqumon.trading.model.Order> getOpenOrders() throws TradingException {
        this.dataCache.put(Keys.Orders, new HashMap<Integer, com.magnumresearch.aqumon.trading.model.Order>());
        this.blockingQueueMap.put(Keys.Orders, new LinkedBlockingQueue());
        this.clientSocket.reqAllOpenOrders();
        OpenOrdersSignal openOrdersSignal = this.pollResponseFromQueue(Keys.Orders);
        this.dataCache.remove(Keys.Orders);
        return openOrdersSignal.getOrders();
    }

    public synchronized List<com.magnumresearch.aqumon.trading.model.Order> getCompletedOrders() throws TradingException {
        this.dataCache.put(Keys.Orders, new HashMap<Integer, com.magnumresearch.aqumon.trading.model.Order>());
        this.blockingQueueMap.put(Keys.Orders, new LinkedBlockingQueue());
        this.clientSocket.reqCompletedOrders(true);
        OpenOrdersSignal openOrdersSignal = this.pollResponseFromQueue(Keys.Orders);
        this.dataCache.remove(Keys.Orders);
        return openOrdersSignal.getOrders();
    }

    public synchronized List<com.magnumresearch.aqumon.trading.model.Execution> getExecutions() throws TradingException {
        this.dataCache.put(
                Keys.Executions, new ConcurrentHashMap<String, com.magnumresearch.aqumon.trading.model.Execution>()
        );
        this.blockingQueueMap.put(Keys.Executions, new LinkedBlockingQueue());
        ExecutionFilter filter = new ExecutionFilter();
        filter.acctCode(brokerAccount);
        this.clientSocket.reqExecutions(reqId.incrementAndGet(), filter);
        ExecutionsSignal executionsSignal = this.pollResponseFromQueue(Keys.Executions);
        this.dataCache.remove(Keys.Executions);
        return executionsSignal.getExecutions();
    }

    public Integer getExtOrderId() {
        return extOrderId.incrementAndGet();
    }

    private void setExtOrderId(Integer currentOrderId) {
        this.extOrderId = new AtomicInteger(currentOrderId);
    }

    @Override
    public void tickSize(int i, int i1, int i2) {

    }

    @Override
    public void tickOptionComputation(int i, int i1, double v, double v1, double v2, double v3, double v4, double v5,
                                      double v6, double v7) {

    }

    @Override
    public void tickGeneric(int i, int i1, double v) {

    }

    @Override
    public void tickString(int i, int i1, String s) {

    }

    @Override
    public void tickEFP(int i, int i1, double v, String s, double v1, int i2, String s1, double v2, double v3) {

    }

    @Override
    public void orderStatus(int orderId, String status, double filled, double remaining, double avgFillPrice,
                            int permId, int parentId, double lastFillPrice, int clientId, String whyHeld, double mktCapPrice) {
        log.info("[IB Response][orderStatus] Received OrderStatus. Id: " + orderId + ", Status: " + status +
                ", Filled" + filled + ", Remaining: " + remaining + ", AvgFillPrice: " + avgFillPrice + ", PermId: " +
                permId + ", ParentId: " + parentId + ", LastFillPrice: " + lastFillPrice + ", ClientId: " + clientId +
                ", WhyHeld: " + whyHeld + ", mktCapPrice: " + mktCapPrice);
        OrderStatusType statusType = getOrderStatusTypeFromIBStatus(status);
        com.magnumresearch.aqumon.trading.model.Order order = new com.magnumresearch.aqumon.trading.model.Order();
        order.setStatus(statusType);
        order.setExtOrderId(String.valueOf(orderId));
        order.setEnvId(clientId);
        order.setBrokerAccount(brokerAccount);
        order.setQuantityFilled(BigDecimal.valueOf(filled).setScale(6, RoundingMode.HALF_UP));
        order.setPriceFilled(BigDecimal.valueOf(avgFillPrice).setScale(6, RoundingMode.HALF_UP));
        try {
            if (BigDecimal.valueOf(mktCapPrice).compareTo(BigDecimal.ZERO) != 0) {
                order.addRemarkList(null, "[IB Response][orderStatus] Market Cap! Cap Price => " + mktCapPrice);
            }
        } catch (Exception e) {
            log.error("[IB Response][orderStatus] Exception Message: " + e.getMessage());
        }
        // 如果是主动获取的，有一个搜集逻辑
        Map<Integer, com.magnumresearch.aqumon.trading.model.Order> orderMap =
                (Map<Integer, com.magnumresearch.aqumon.trading.model.Order>) this.dataCache.get(Keys.Orders);
        if (orderMap != null) {
            synchronized (orderMap) {
                if (orderMap.get(orderId) == null) {
                    orderMap.put(orderId, order);
                } else {
                    order = orderMap.get(orderId);
                    order.setStatus(statusType);
                    order.setExtOrderId(String.valueOf(orderId));
                    order.setEnvId(clientId);
                    order.setQuantityFilled(BigDecimal.valueOf(filled).setScale(6, RoundingMode.HALF_UP));
                    order.setPriceFilled(BigDecimal.valueOf(avgFillPrice).setScale(6, RoundingMode.HALF_UP));
                }
            }
        }
    }

    @Override
    public void openOrder(int i, Contract contract, Order order, OrderState orderState) {

    }

    @Override
    public void openOrderEnd() {

    }

    @Override
    public void updateAccountValue(String key, String value, String currency, String account) {
        log.info("[IB Response][updateAccountValue] Key: " + key + ", Value: " + value
                + ", Currency: " + currency + ", " + "AccountName: " + account);
        SubAccountSummary accountObject = (SubAccountSummary) this.dataCache.get(Keys.AccountSummaryUpdates);
        synchronized (accountObject) {
            //资金信息
            if ("CashBalance".equalsIgnoreCase(key)) {
                Map<CurrencyType, CashInfo> map = accountObject.getCashInfoMap();
                CashInfo cashInfo = map.get(CurrencyType.valueOf(currency));
                if (cashInfo == null) {
                    cashInfo = new CashInfo();
                }
                cashInfo.setBrokerAccount(account);
                cashInfo.setCurrencyType(CurrencyType.valueOf(currency));
                cashInfo.setBalance(new BigDecimal(value));
                cashInfo.setPurchasePower(new BigDecimal(value));
                map.put(CurrencyType.valueOf(currency), cashInfo);
                accountObject.setCash(new BigDecimal(value));
            }
            //账户净值，即所有资金总值加上持仓总值，计量单位为账户对应的基础货币
            if ("NetLiquidation".equalsIgnoreCase(key)) {
                Map<CurrencyType, CashInfo> map = accountObject.getCashInfoMap();
                CashInfo cashInfo = map.get(CurrencyType.valueOf(currency));
                if (cashInfo == null) {
                    cashInfo = new CashInfo();
                }
                cashInfo.setAsset(new BigDecimal(value));
                map.put(CurrencyType.valueOf(currency), cashInfo);
                accountObject.setNetLiquidationValue(new BigDecimal(value));
            }
            //当月利息，当月累积要付的利息
            if ("AccruedCash".equalsIgnoreCase(key) && "BASE".equalsIgnoreCase(currency)) {
                accountObject.setMtdInterest(new BigDecimal(value));
            }
            //合贷款价值的资产
            if ("EquityWithLoanValue".equalsIgnoreCase(key)) {
                accountObject.setEquities(new BigDecimal(value));
            }
            //完成交易的损益
            if ("RealizedPnL".equalsIgnoreCase(key) && "BASE".equalsIgnoreCase(currency)) {
                accountObject.setRealizedPL(new BigDecimal(value));
            }
            //当前未平仓合约的损益
            if ("UnrealizedPnL".equalsIgnoreCase(key) && "BASE".equalsIgnoreCase(currency)) {
                accountObject.setUnrealizedPL(new BigDecimal(value));
            }
            //购买力
            if ("BuyingPower".equalsIgnoreCase(key)) {
                accountObject.setBuyPower(new BigDecimal(value));
            }
            //当前初始保证金
            if ("InitMarginReq".equalsIgnoreCase(key)) {
                accountObject.setInitialMargin(new BigDecimal(value));
            }
            //当前维持保证金
            if ("MaintMarginReq".equalsIgnoreCase(key)) {
                accountObject.setMaintainMargin(new BigDecimal(value));
            }
            //当前剩余流动性
            if ("ExcessLiquidity".equalsIgnoreCase(key)) {
                accountObject.setExcessLiquidity(new BigDecimal(value));
            }
        }
    }

    @Override
    public void accountDownloadEnd(String s) {
        log.info("[IB Response][accountDownloadEnd]");
        if (this.dataCache.get(Keys.AccountSummaryUpdates) != null) {
            synchronized (this.dataCache.get(Keys.AccountSummaryUpdates)) {
                pushResultIntoQueue(
                        new BrokerAccountSummaryUpdatesSignal((SubAccountSummary) this.dataCache.get(Keys.AccountSummaryUpdates)),
                        Keys.AccountSummaryUpdates
                );
                this.dataCache.remove(Keys.AccountSummaryUpdates);
            }
        }
    }

    @Override
    public void updatePortfolio(Contract contract, double v, double v1, double v2, double v3, double v4, double v5,
                                String s) {
        log.info("[IB Response][UpdatePortfolio] " + contract.symbol() + ", " + contract.secType() + " @ "
                + contract.exchange() + ": HTPosition: " + v + ", MarketPrice: " + v1 + ", MarketValue: " + v2 + ", "
                + "AverageCost: " + v3 + ", UnrealisedPNL: " + v4 + ", RealisedPNL: " + v5 + ", AccountName: " + s);
    }

    @Override
    public void updateAccountTime(String s) {

    }

    @Override
    public void nextValidId(int orderId) {
        log.info("Next Valid Id: [" + orderId + "]");
        log.info("[IB Response][nextValidId],{}", orderId);
        setExtOrderId(orderId);
        pushResultIntoQueue(new NextValidIdSignal(orderId), Keys.Connection);
        log.info("[IB Response][nextValidId],{}", orderId);
    }

    @Override
    public void contractDetails(int i, ContractDetails contractDetails) {

    }

    @Override
    public void bondContractDetails(int i, ContractDetails contractDetails) {

    }

    @Override
    public void contractDetailsEnd(int i) {

    }

    @Override
    public void execDetails(int reqId, Contract contract, Execution execution) {
        log.info("[IB Response][execDetails] ExecDetails. " + reqId + " - [" + contract.symbol() + "], " +
                "[" + contract.secType() + "], [" + contract.currency() + "], [" + execution.execId() + "], " +
                "[" + execution.orderId() + "], [" + execution.shares() + "], [" + execution.clientId() + "]");
        if (execution.execId().startsWith("U+")) {
            return;
        }
        com.magnumresearch.aqumon.trading.model.Execution executionUpdateInfo = new com.magnumresearch.aqumon.trading.model.Execution();
        executionUpdateInfo.setEnvId(execution.clientId());
        executionUpdateInfo.setExtOrderId(String.valueOf(execution.orderId()));
        executionUpdateInfo.setExtExecutionId(execution.execId());
        executionUpdateInfo.setPriceFilled(BigDecimal.valueOf(execution.price()));
        executionUpdateInfo.setQuantityFilled(BigDecimal.valueOf(execution.shares()));
        executionUpdateInfo.setAmountFilled(
                BigDecimal.valueOf(execution.price()).multiply(BigDecimal.valueOf(execution.shares()))
        );
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd hh:mm:ss");
        try {
            Date date = simpleDateFormat.parse(execution.time());
            executionUpdateInfo.setFilledTimestamp(date.getTime());
        } catch (Exception e) {
            executionUpdateInfo.setFilledTimestamp(System.currentTimeMillis());
        }
        executionUpdateInfo.setBrokerAccount(brokerAccount);
        Map<String, com.magnumresearch.aqumon.trading.model.Execution> executionMap =
                (Map<String, com.magnumresearch.aqumon.trading.model.Execution>) this.dataCache.get(Keys.Executions);
        if (executionMap != null) {
            InstrumentDataMaster instrumentDataMaster = contractUtils.getInstrumentByIBContract(contract);
            executionUpdateInfo.setInstrumentId(instrumentDataMaster.getInstrumentId());
            executionUpdateInfo.setInstrumentInfo(instrumentUtil.getInstrumentKey(instrumentDataMaster));
            OrderDirectionType direction = execution.side().equals("BOT") ? OrderDirectionType.BUY : OrderDirectionType.SELL;
            executionUpdateInfo.setDirection(direction);
            executionUpdateInfo.setSecType(instrumentDataMaster.getSecType());
            executionMap.put(execution.execId(), executionUpdateInfo);
        }
    }

    @Override
    public void execDetailsEnd(int i) {
        log.info("[IB Response][execDetailsEnd] " + i);
        Map<String, com.magnumresearch.aqumon.trading.model.Execution> executionMap =
                (Map<String, com.magnumresearch.aqumon.trading.model.Execution>) this.dataCache.get(Keys.Executions);
        if (executionMap != null) {
            ExecutionsSignal signal = new ExecutionsSignal(new LinkedList<>(executionMap.values()));
            log.info("[IB Response][execDetailsEnd] " + signal);
            pushResultIntoQueue(signal, Keys.Executions);
        }
    }


    @Override
    public void commissionReport(CommissionReport commissionReport) {
        log.info("[IB Response][commissionReport] CommissionReport. " +
                "[" + commissionReport.execId() + "] - " +
                "[" + commissionReport.commission() + "] " +
                "[" + commissionReport.currency() + "] " +
                "RPNL [" + commissionReport.realizedPNL() + "]");

        if (commissionReport.execId().startsWith("U+")) {
            return;
        }
        Commission commissionUpdateInfo = new Commission();
        commissionUpdateInfo.setEnvId(clientId);
        commissionUpdateInfo.setBrokerAccount(brokerAccount);
        commissionUpdateInfo.setExtExecutionId(commissionReport.execId());
        commissionUpdateInfo.setCommission(BigDecimal.valueOf(commissionReport.commission()));
        log.info("[IB Response][commissionReport] official Execution：" + commissionUpdateInfo);
        log.info("[IB Response][commissionReport] raiseCommission Commission**** " + commissionUpdateInfo);
        Map<String, com.magnumresearch.aqumon.trading.model.Execution> executionMap =
                (Map<String, com.magnumresearch.aqumon.trading.model.Execution>) this.dataCache.get(Keys.Executions);
        if (executionMap != null) {
            com.magnumresearch.aqumon.trading.model.Execution execution = executionMap.get(commissionReport.execId());
            execution.setCommission(BigDecimal.valueOf(commissionReport.commission()));
            executionMap.put(commissionReport.execId(), execution);
        }
    }

    @Override
    public void updateMktDepth(int i, int i1, int i2, int i3, double v, int i4) {

    }

    @Override
    public void updateMktDepthL2(int tickerId, int position, String marketMaker, int operation,
                                 int side, double price, int size, boolean isSmartDepth) {

    }

    @Override
    public void updateNewsBulletin(int i, int i1, String s, String s1) {

    }

    @Override
    public void managedAccounts(String s) {
        log.info("[IB Response][managedAccounts] Received managed accounts: " + s);
        pushResultIntoQueue(new ManagedAcctSignal(s), Keys.ManagedAccts);
        // this.blockingQueueMap.get(Keys.ManagedAccts).put(new ManagedAcctSignal(s));
    }


    @Override
    public void position(String s, Contract contract, double v, double v1) {
        log.info("[IB Response][position] Position: {}, {}, {}, {}", s, contract, v, v1);
    }

    @Override
    public void positionEnd() {
        log.info("[IB Response][positionEnd] Position end");
    }

    @Override
    public void positionMulti(int reqId, String account, String modelCode, Contract contract, double pos, double avgCost) {
        log.info("[IB Response][positionMulti] Position multi: {}, {}, {}, {}, {}, {}",
                reqId, account, modelCode, contract.symbol(), pos, avgCost);
        if (BigDecimal.valueOf(pos).compareTo(BigDecimal.ZERO) == 0 || contract.secType() == Types.SecType.CASH) {
            return;
        }
        List<Holding> holdings = (List<Holding>) this.dataCache.get(Keys.PositionsMulti);
        synchronized (holdings) {
            Holding holding = new Holding();
            InstrumentDataMaster instrumentDataMaster = contractUtils.getInstrumentByIBContract(contract);
            holding.setInstrumentId(instrumentDataMaster.getInstrumentId());
            holding.setInstrumentInfo(instrumentUtil.getInstrumentKey(instrumentDataMaster));
            BigDecimal position = BigDecimal.valueOf(pos);
            BigDecimal price = BigDecimal.valueOf(avgCost);
            holding.setHoldingPosition(position);
            holding.setHoldingPrice(price);
            holding.setHoldingAmount(price.multiply(position));
            holding.setBrokerAccount(account);
            holding.setBrokerType(BrokerType.IB);
            holdings.add(holding);
        }
    }

    @Override
    public void positionMultiEnd(int i) {
        log.info("[IB Response][positionMultiEnd] Position multi end: {}", i);
        List<Holding> holdings = (List<Holding>) this.dataCache.get(Keys.PositionsMulti);
        PositionEndSignal signal;
        if (holdings != null) {
            synchronized (holdings) {
                signal = new PositionEndSignal(holdings);
            }
        } else {
            signal = new PositionEndSignal(new LinkedList<>());
        }
        pushResultIntoQueue(signal, Keys.PositionsMulti);
    }

    @Override
    public void accountSummary(int i, String account, String tag, String value, String currency) {
        CashInfo cashInfo = (CashInfo) this.dataCache.get(Keys.Balance);
        if (cashInfo != null && currency.equalsIgnoreCase(cashInfo.getCurrencyType().getCurrency())
                && account.equalsIgnoreCase(cashInfo.getBrokerAccount())) {
            if (tag.equalsIgnoreCase("CashBalance")) {
                cashInfo.setBalance(new BigDecimal(value));
                cashInfo.setPurchasePower(new BigDecimal(value));
            }
            if (tag.equalsIgnoreCase("NetLiquidationByCurrency")) {
                cashInfo.setAsset(new BigDecimal(value));
            }
        }
    }

    @Override
    public void accountSummaryEnd(int i) {
        CashInfo info = (CashInfo) this.dataCache.get(Keys.Balance);
        pushResultIntoQueue(new BalanceSignal(info.getCurrencyType(), info), Keys.Balance);
        this.dataCache.remove(Keys.Balance);
    }

    @Override
    public void error(Exception e) {
        log.error("[IB Response][error] BrokerAccount(" + brokerAccount + ") Received exception: " + e.getMessage());
    }

    @Override
    public void error(String s) {
        log.error("[IB Response][error] BrokerAccount(" + brokerAccount + ") Received error: " + s);
    }

    @Override
    public void error(int id, int code, String s) {
        if (code == 2104 || code == 2106 || code == 2158 || code == 2108 || code == 2100) {
            // for Market data farm connection is OK, actually this is not a true error
            log.info("[IB Response][error] BrokerAccount(" + brokerAccount + ") Info: " + id + " " + code + " " + s);
            return;
        } else {
            log.error("[IB Response][error] BrokerAccount(" + brokerAccount + ") Error: " + id + " " + code + " " + s);
        }
        if (code == 1100 || code == 1300 || code == 502 || code == 503 || code == 504 || code == 2105) {
            log.info("[IB Response][error]  BrokerAccount(" + brokerAccount + ") Raising adapter offline status");
            ibBrokerAdapter.raiseAdapterStatus(AdapterStatusType.OFFLINE, true, s);
        } else if (code == 507) {
            // If there is a problem with the socket connection between TWS and the API client, for instance if TWS
            // suddenly closes, this will trigger an exception in the EReader thread which is reading from the socket.
            // This exception will also occur if an API client attempts to connect with a client ID that is already in use.
            //
            //The socket EOF is handled slightly differently in different API languages. For instance in Java, it is
            // caught and sent to the client application to IBApi::EWrapper::error with errorCode 507: "Bad Message".
            // In C# it is caught and sent to IBApi::EWrapper::error with errorCode -1. The client application needs to
            // handle this error message and use it to indicate that an exception has been thrown in the socket
            // connection. Associated functions such as IBApi::EWrapper::connectionClosed and IBApi::EClient::IsConnected
            // functions are not called automatically by the API code but need to be handled at the API client-level*.
            log.info("[IB Response][error] BrokerAccount(" + brokerAccount + ") 507 msg! Maybe clientId used by others");
            pushResultIntoQueue(new NextValidIdSignal(0), Keys.Connection); // 抛出连接失败哨兵, 在外面进行disconnect
        } else if (code == 501 || code == 1102) {
            log.info("[IB Response][error] BrokerAccount(" + brokerAccount + ") Raising adapter online status");
            ibBrokerAdapter.raiseAdapterStatus(AdapterStatusType.ONLINE, false, "");
        } else if (code == 10147 || code == 10148 || code == 104) {
            pushResultIntoQueue(CancelStatusType.failed, Keys.CancelStatus); // successfully cancel order
            log.info("[IB Response][error] BrokerAccount(" + brokerAccount + ") Error: Failed to cancel order！");
        } else {
            com.magnumresearch.aqumon.trading.model.Order orderUpdate = new com.magnumresearch.aqumon.trading.model.Order();
            if (code == 103 || (code >= 106 && code <= 120) || code == 122 || code == 125
                    || code == 126 || code == 129 || code == 132 || code == 133 || code == 134
                    || (code >= 140 && code <= 142) || code == 144 || (code >= 148 && code <= 160) || code == 163
                    || code == 167 || code == 168 || code == 200 || code == 201 || code == 203 || (code >= 332 && code <= 342)
                    || (code >= 345 && code <= 353) || code == 355 || code == 356 || (code >= 358 && code <= 364)
                    || (code >= 367 && code <= 380) || code == 382 || code == 383 || (code >= 387 && code <= 390)
                    || code == 392 || code == 393 || (code >= 395 && code <= 398) || code == 400
                    || (code >= 402 && code <= 405) || (code >= 408 && code <= 412) || (code >= 415 && code <= 419)
                    || code == 422 || code == 423 || code == 426 || (code >= 433 && code <= 436)
                    || (code >= 439 && code <= 449) || code == 461 || code == 10006 || code == 10015) {
                orderUpdate.setStatus(OrderStatusType.REJECTED);
            } else if (code == 202) {
                // success to cancel order 有时候cancel成功但是orderStatus丢失了,这里做2次raise
                pushResultIntoQueue(CancelStatusType.success, Keys.CancelStatus); // successfully cancel order
                orderUpdate.setStatus(OrderStatusType.CANCELLED);
            } else if (code == 321) {
                // Mainly for Broker Account check
                log.info("[IB Response][error] BrokerAccount(" + brokerAccount + ") Raising account invalid status");
                BlockingQueue queue = this.blockingQueueMap.get(Keys.AccountSummaryUpdates);
                queue.offer(-1); //To stop queue from waiting for response
            }
            if (id != -1) {
                orderUpdate.setEnvId(clientId);
                orderUpdate.setExtOrderId(String.valueOf(id));
                orderUpdate.addRemarkList(String.valueOf(code), s);
                orderUpdate.setBrokerAccount(brokerAccount);
                log.info("[IB Response][error] BrokerAccount(" + brokerAccount + ") order status(" + orderUpdate.getStatus() + ")! order => " + orderUpdate);
            }
        }
    }

    @Override
    public void accountUpdateMulti(
            int reqId, String account, String modelCode, String key, String value, String currency
    ) {
        log.info("[IB Response][accountUpdateMulti] Account Update Multi. Request: " + reqId + ", " + "Account: " + account + ", " +
                "ModelCode: " + modelCode + ", Key: " + key + ", Value: " + value + ", Currency: " + currency);
        SubAccountSummary accountObject = (SubAccountSummary) this.dataCache.get(Keys.AccountSummaryUpdates);
        synchronized (accountObject) {
            //资金信息
            if ("CashBalance".equalsIgnoreCase(key)) {
                Map<CurrencyType, CashInfo> map = accountObject.getCashInfoMap();
                CashInfo cashInfo = map.get(CurrencyType.valueOf(currency));
                if (cashInfo == null) {
                    cashInfo = new CashInfo();
                }
                cashInfo.setBrokerAccount(account);
                cashInfo.setCurrencyType(CurrencyType.valueOf(currency));
                cashInfo.setBalance(new BigDecimal(value));
                cashInfo.setPurchasePower(new BigDecimal(value));
                map.put(CurrencyType.valueOf(currency), cashInfo);
                accountObject.setCash(new BigDecimal(value));
            }
            //账户净值，即所有资金总值加上持仓总值，计量单位为账户对应的基础货币
            if ("NetLiquidation".equalsIgnoreCase(key)) {
                Map<CurrencyType, CashInfo> map = accountObject.getCashInfoMap();
                CashInfo cashInfo = map.get(CurrencyType.valueOf(currency));
                if (cashInfo == null) {
                    cashInfo = new CashInfo();
                }
                cashInfo.setAsset(new BigDecimal(value));
                map.put(CurrencyType.valueOf(currency), cashInfo);
                accountObject.setNetLiquidationValue(new BigDecimal(value));
            }
            //当月利息，当月累积要付的利息
            if ("AccruedCash".equalsIgnoreCase(key) && "BASE".equalsIgnoreCase(currency)) {
                accountObject.setMtdInterest(new BigDecimal(value));
            }
            //合贷款价值的资产
            if ("EquityWithLoanValue".equalsIgnoreCase(key)) {
                accountObject.setEquities(new BigDecimal(value));
            }
            //完成交易的损益
            if ("RealizedPnL".equalsIgnoreCase(key) && "BASE".equalsIgnoreCase(currency)) {
                accountObject.setRealizedPL(new BigDecimal(value));
            }
            //当前未平仓合约的损益
            if ("UnrealizedPnL".equalsIgnoreCase(key) && "BASE".equalsIgnoreCase(currency)) {
                accountObject.setUnrealizedPL(new BigDecimal(value));
            }
            //购买力
            if ("BuyingPower".equalsIgnoreCase(key)) {
                accountObject.setBuyPower(new BigDecimal(value));
            }
            //当前初始保证金
            if ("InitMarginReq".equalsIgnoreCase(key)) {
                accountObject.setInitialMargin(new BigDecimal(value));
            }
            //当前维持保证金
            if ("MaintMarginReq".equalsIgnoreCase(key)) {
                accountObject.setMaintainMargin(new BigDecimal(value));
            }
            //当前剩余流动性
            if ("ExcessLiquidity".equalsIgnoreCase(key)) {
                accountObject.setExcessLiquidity(new BigDecimal(value));
            }
        }
    }

    @Override
    public void accountUpdateMultiEnd(int i) {
        log.info("[IB Response][accountUpdateMultiEnd] AccountUpdatesMultiEnd, brokerAccount: {}, reqId: {}",
                brokerAccount, i);
        if (this.dataCache.get(Keys.AccountSummaryUpdates) != null) {
            synchronized (this.dataCache.get(Keys.AccountSummaryUpdates)) {
                pushResultIntoQueue(
                        new BrokerAccountSummaryUpdatesSignal((SubAccountSummary) this.dataCache.get(Keys.AccountSummaryUpdates)),
                        Keys.AccountSummaryUpdates
                );
            }
        } else {
            log.error("[IB Response][accountUpdateMultiEnd] ERROR! brokerAccount({}) " +
                    "this.dataCache.get(Keys.AccountSummaryUpdates) == null", brokerAccount);
        }
    }

    private OrderStatusType getOrderStatusTypeFromIBStatus(String status) {
        OrderStatusType statusType;
        switch (status) {
            case "Submitted":
                statusType = OrderStatusType.FILLING;
                break;
            case "Filled":
                statusType = OrderStatusType.FILLED;
                break;
            case "ApiCancelled":
            case "Cancelled":
                statusType = OrderStatusType.CANCELLED;
                // successfully cancel order
                pushResultIntoQueue(CancelStatusType.success, Keys.CancelStatus);
                break;
            case "Inactive":
            default:
                statusType = OrderStatusType.PENDING;
        }
        return statusType;
    }

    private void checkAndFixErrorMsg() {
        threadPoolTaskExecutor.execute(() -> {
            Lock ibResponseErrorLock = null;
            try {
                ibResponseErrorLock = RedisUtil.tryAndAbortFairLock(
                        "TradingEngineLocks-",
                        "ibResponseErrorLock-" + clientId + brokerAccount,
                        120000,
                        0);
                if (ibResponseErrorLock != null) {
                    log.warn("[IB Response][checkAndFixErrorMsg][start] " +
                            "Now query orders and executions of brokerAccount(" + brokerAccount + ") again...");
                    List<com.magnumresearch.aqumon.trading.model.Order> orderList =
                            orderDao.findByBrokerTypeAndBrokerAccount(BrokerType.IB, brokerAccount);
                    for (com.magnumresearch.aqumon.trading.model.Order order : orderList) {
                        if (!order.getStatus().isTerminalStatus() && order.getExtOrderId() != null) {
                            try {
                                getExecutions();
                                getCompletedOrders();
                                getOpenOrders();
                                log.warn("[IB Response][checkAndFixErrorMsg][end] Finish query executions!");
                                return;
                            } catch (TradingException e) {
                                log.error("[IB Response][checkAndFixErrorMsg] ERROR!" +
                                        "Failed to query orders and executions of brokerAccount(" + brokerAccount + ")", e);
                            }
                        }
                    }
                    larkRobotClient.sendMessage("[IB Adapter][checkAndFixErrorMsg] " +
                            "brokerAccount(" + brokerAccount + ") IB消息异常, 触发一次open order状态检查修复逻辑!");
                }
            } finally {
                RedisUtil.releaseLock(ibResponseErrorLock);
            }
        });
    }

    @Override
    public void tickPrice(int quoteId, int type, double price, TickAttrib attrib) {
    }

    @Override
    public void receiveFA(int faDataType, String faXmlData) {

    }

    @Override
    public void connectionClosed() {

    }

    @Override
    public void connectAck() {

    }

    @Override
    public void securityDefinitionOptionalParameter(int i, String s, int i1, String s1, String s2, Set<String> set,
                                                    Set<Double> set1) {

    }

    @Override
    public void securityDefinitionOptionalParameterEnd(int i) {

    }

    @Override
    public void verifyMessageAPI(String s) {

    }

    @Override
    public void verifyCompleted(boolean b, String s) {

    }

    @Override
    public void verifyAndAuthMessageAPI(String s, String s1) {

    }

    @Override
    public void verifyAndAuthCompleted(boolean b, String s) {

    }

    @Override
    public void displayGroupList(int i, String s) {

    }

    @Override
    public void displayGroupUpdated(int i, String s) {

    }

    @Override
    public void softDollarTiers(int i, SoftDollarTier[] softDollarTiers) {

    }

    @Override
    public void historicalData(int reqId, Bar bar) {

    }

    @Override
    public void scannerParameters(String s) {

    }

    @Override
    public void scannerData(int i, int i1, ContractDetails contractDetails, String s, String s1, String s2, String s3) {

    }

    @Override
    public void scannerDataEnd(int i) {

    }

    @Override
    public void realtimeBar(int i, long l, double v, double v1, double v2, double v3, long l1, double v4, int i1) {

    }

    @Override
    public void currentTime(long l) {

    }

    @Override
    public void fundamentalData(int i, String s) {

    }

    @Override
    public void deltaNeutralValidation(int i, DeltaNeutralContract deltaNeutralContract) {

    }

    @Override
    public void tickSnapshotEnd(int i) {

    }

    @Override
    public void marketDataType(int i, int i1) {

    }

    @Override
    public void familyCodes(FamilyCode[] var1) {
    }

    @Override
    public void symbolSamples(int var1, ContractDescription[] var2) {
    }

    @Override
    public void historicalDataEnd(int var1, String var2, String var3) {
    }

    @Override
    public void mktDepthExchanges(DepthMktDataDescription[] var1) {
    }

    @Override
    public void tickNews(int var1, long var2, String var4, String var5, String var6, String var7) {
    }

    @Override
    public void smartComponents(int var1, Map<Integer, Map.Entry<String, Character>> var2) {
    }

    @Override
    public void tickReqParams(int var1, double var2, String var4, int var5) {
    }

    @Override
    public void newsProviders(NewsProvider[] var1) {
    }

    @Override
    public void newsArticle(int var1, int var2, String var3) {
    }

    @Override
    public void historicalNews(int var1, String var2, String var3, String var4, String var5) {
    }

    @Override
    public void historicalNewsEnd(int var1, boolean var2) {
    }

    @Override
    public void headTimestamp(int var1, String var2) {
    }

    @Override
    public void histogramData(int var1, List<HistogramEntry> var2) {
    }

    @Override
    public void historicalDataUpdate(int var1, Bar var2) {
    }

    @Override
    public void rerouteMktDataReq(int var1, int var2, String var3) {
    }

    @Override
    public void rerouteMktDepthReq(int var1, int var2, String var3) {
    }

    @Override
    public void marketRule(int var1, PriceIncrement[] var2) {
    }

    @Override
    public void pnl(int var1, double var2, double var4, double var6) {
    }

    @Override
    public void pnlSingle(int var1, int var2, double var3, double var5, double var7, double var9) {
    }

    @Override
    public void historicalTicks(int var1, List<HistoricalTick> var2, boolean var3) {
    }

    @Override
    public void historicalTicksBidAsk(int var1, List<HistoricalTickBidAsk> var2, boolean var3) {
    }

    @Override
    public void historicalTicksLast(int var1, List<HistoricalTickLast> var2, boolean var3) {
    }

    @Override
    public void tickByTickAllLast(int var1, int var2, long var3, double var5, int var7, TickAttribLast var8, String var9, String var10) {
    }

    @Override
    public void tickByTickBidAsk(int var1, long var2, double var4, double var6, int var8, int var9, TickAttribBidAsk var10) {
    }

    @Override
    public void tickByTickMidPoint(int var1, long var2, double var4) {
    }

    @Override
    public void orderBound(long var1, int var3, int var4) {
    }

    @Override
    public void completedOrder(Contract contract, Order order, OrderState orderState) {

    }

    @Override
    public void completedOrdersEnd() {

    }

//    public Integer getNextOrderId() throws InterruptedException, TradingException {
//        // 请求获得下一个extOrderId
//        this.blockingQueueMap.put(Keys.OrderId, new LinkedBlockingQueue());
//        // Parameter deprecated, input any number, won't affect the request
//        this.clientSocket.reqIds(1);
//        // Fixed(joseph): make sure we've got reqId from IB before we poll response from queue
//        while (true) {
//            BlockingQueue queue = this.blockingQueueMap.get(Keys.OrderId);
//            if (queue.peek() != null) {
//                break;
//            } else {
//                TimeUnit.MILLISECONDS.sleep(1);
//            }
//        }
//        log.info("[InputOrder-pollResponseFromQueueStarts]");
//        NextValidIdSignal res = pollResponseFromQueue(Keys.OrderId);
//        log.info("[InputOrder-pollResponseFromQueueEnds]");
//        return res.getNextValidId();
//    }

    public Map<CurrencyType, CashInfo> getCashBalance(String account) throws TradingException {
        try {
//            checkFAInfo(FAInfoBase.FAGroup, account);
            Map<CurrencyType, CashInfo> resultMap = new HashMap<>();
            int oneTimeReqId = reqId.incrementAndGet();
            this.blockingQueueMap.put(Keys.Balance, new LinkedBlockingQueue());
            this.dataCache.put(Keys.Balance, new CashInfo(CurrencyType.USD, account));
            this.clientSocket.reqAccountSummary(oneTimeReqId, FAInfoBase.FA_GROUP_NETLIQ_PREFIX + account,
                    "$LEDGER:" + CurrencyType.USD.getCurrency());

            BalanceSignal signal = pollResponseFromQueue(Keys.Balance);
            if (signal != null) {
                resultMap.put(CurrencyType.USD, signal.getCashInfo());
            }
            this.clientSocket.cancelAccountSummary(oneTimeReqId);

            oneTimeReqId = reqId.incrementAndGet();
            this.blockingQueueMap.put(Keys.Balance, new LinkedBlockingQueue());
            this.dataCache.put(Keys.Balance, new CashInfo(CurrencyType.HKD, account));
            this.clientSocket.reqAccountSummary(oneTimeReqId, FAInfoBase.FA_GROUP_NETLIQ_PREFIX + account,
                    "$LEDGER:" + CurrencyType.HKD.getCurrency());

            signal = pollResponseFromQueue(Keys.Balance);
            if (signal != null) {
                resultMap.put(CurrencyType.HKD, signal.getCashInfo());
            }
            this.clientSocket.cancelAccountSummary(oneTimeReqId);

            oneTimeReqId = reqId.incrementAndGet();
            this.blockingQueueMap.put(Keys.Balance, new LinkedBlockingQueue());
            this.dataCache.put(Keys.Balance, new CashInfo(CurrencyType.CNH, account));
            this.clientSocket.reqAccountSummary(oneTimeReqId, FAInfoBase.FA_GROUP_NETLIQ_PREFIX + account,
                    "$LEDGER:" + CurrencyType.CNH.getCurrency());

            signal = pollResponseFromQueue(Keys.Balance);
            if (signal != null) {
                resultMap.put(CurrencyType.CNH, signal.getCashInfo());
            }
            this.clientSocket.cancelAccountSummary(oneTimeReqId);
            return resultMap;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public SubAccountSummary getAccountUpdates(String brokerAccount) throws TradingException {
        // 弃用, 使用getModelAccountUpdates替代
        this.blockingQueueMap.put(Keys.AccountSummaryUpdates, new LinkedBlockingQueue());
        this.dataCache.put(Keys.AccountSummaryUpdates, new SubAccountSummary());
        RLock queryIbSubAccountSummaryLock = null; // querySubAccountSummary加锁
        // The TWS only allows one IBApi.EClient.reqAccountUpdates request at a time. If the client application
        // attempts to subscribe to a second account without canceling the previous subscription, the new request
        // will override the old one and the TWS will send this message notifying so.
        // 所以加一个锁保证正常请求
        try {
            queryIbSubAccountSummaryLock = RedisUtil.acquireFairLock(
                    "TradingEngineLocks-", "QueryIbSubAccountSummaryLock"
            );
            this.clientSocket.reqAccountUpdates(true, brokerAccount);
            BrokerAccountSummaryUpdatesSignal signal = pollResponseFromQueue(Keys.AccountSummaryUpdates);
            if (signal != null) {
                return signal.getAccountSummaryMap();
            } else {
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_IB_ERROR,
                        "Failed to retrieve account summary updates");
            }
        } finally {
            this.clientSocket.reqAccountUpdates(false, brokerAccount);
            RedisUtil.releaseLock(queryIbSubAccountSummaryLock);
        }
    }

    public SubAccountSummary getModelAccountUpdates(String brokerAccount, String modelCode) throws TradingException {
        this.blockingQueueMap.put(Keys.AccountSummaryUpdates, new LinkedBlockingQueue());
        this.dataCache.put(Keys.AccountSummaryUpdates, new SubAccountSummary());
        int oneTimeReqId = reqId.incrementAndGet();
        this.clientSocket.reqAccountUpdatesMulti(oneTimeReqId, brokerAccount, modelCode, false);
        log.info("[IB Request][getModelAccountUpdates] req model accountUpdates, brokerAccount({}), oneTimeReqId({})",
                brokerAccount, oneTimeReqId);
        BrokerAccountSummaryUpdatesSignal signal;
        try {
            signal = pollResponseFromQueue(Keys.AccountSummaryUpdates);
        } finally {
            this.clientSocket.cancelAccountUpdatesMulti(oneTimeReqId);
            log.info("[IB Request][getModelAccountUpdates] Cancelling model accountUpdates, brokerAccount({}), oneTimeReqId({})",
                    brokerAccount, oneTimeReqId);
//            this.dataCache.remove(Keys.AccountSummaryUpdates);
        }
        return signal.getAccountSummaryMap();
    }

    public List<String> getManagedAccounts() throws TradingException {
        this.blockingQueueMap.put(Keys.ManagedAccts, new LinkedBlockingQueue());
        this.clientSocket.reqManagedAccts();
        ManagedAcctSignal res = pollResponseFromQueue(Keys.ManagedAccts);
        String[] accts = res.getManagedAcctStr().split(",");
        return Arrays.asList(accts);
    }

    public List<Holding> getPositions() throws TradingException {
        log.info("[IB Request][getPositions] Requesting positions");
        LinkedBlockingQueue queue = new LinkedBlockingQueue();
        this.blockingQueueMap.put(Keys.Positions, queue);
        this.clientSocket.reqPositions();
        PositionEndSignal signal = pollResponseFromQueue(Keys.Positions);
        log.info("[IB Request][getPositions] Cancelling positions");
        this.clientSocket.cancelPositions();
        return signal.getHoldings();
    }
}
