package com.magnumresearch.aqumon.riskengine.adapter.ayers;

import com.magnumresearch.aqumon.common.config.RedisConfig;
import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.common.exception.CommonException;
import com.magnumresearch.aqumon.common.utils.RedisUtil;
import com.magnumresearch.aqumon.riskengine.adapter.BaseBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.ayers.constants.MessageConstants;
import com.magnumresearch.aqumon.riskengine.adapter.ayers.pojos.*;
import com.magnumresearch.aqumon.riskengine.adapter.ayers.util.MessageUtil;
import com.magnumresearch.aqumon.trading.constants.AdapterStatusType;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.constants.CurrencyType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.Holding;
import com.magnumresearch.aqumon.trading.model.SubAccount;
import com.magnumresearch.aqumon.trading.model.SubAccountSummary;
import com.magnumresearch.aqumon.trading.pojo.CashInfo;
import com.magnumresearch.aqumon.trading.pojo.InstrumentDataMaster;
import com.magnumresearch.aqumon.trading.utils.InstrumentUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;

import static java.nio.charset.StandardCharsets.UTF_8;

@Service
@ConditionalOnProperty(prefix = "adapter.ayers", value = {"enabled"})
public class AyersBrokerAdapter extends BaseBrokerAdapter {

    @Value("${adapter.ayers.host}")
    private String host;
    @Value("${adapter.ayers.port}")
    private Integer port;
    @Value("${adapter.ayers.site}")
    private String site;
    @Value("${adapter.ayers.station}")
    private String station;
    @Value("${adapter.ayers.user}")
    private String user;
    @Value("${adapter.ayers.password}")
    private String password;
    @Value("${adapter.ayers.orderRecovery}")
    private String orderRecovery;

    private static final Integer clientId = 0;
    private final String msgIdName = "RiskEngineAyersMsgId";

    private Socket socket;
    private volatile BufferedInputStream bis;
    private volatile BufferedOutputStream bos;
    private volatile Thread processingThread;
    Map<Object, BlockingQueue> blockingQueueMap = new ConcurrentHashMap<>();
    private AtomicBoolean conenctionLock = new AtomicBoolean(false);
    private AtomicBoolean loggedIn = new AtomicBoolean(false);

    @Autowired
    ThreadPoolTaskExecutor threadPoolTaskExecutor;
    @Autowired
    InstrumentUtil instrumentUtil;

    Logger log = LoggerFactory.getLogger(this.getClass());


    @PostConstruct
    public void init() {
        try {
            this.socket = new Socket(this.host, this.port);
            this.bis = new BufferedInputStream(this.socket.getInputStream());
            this.bos = new BufferedOutputStream(this.socket.getOutputStream());
            threadPoolTaskExecutor.execute(this::processResponse);
            login();
        } catch (IOException e) {
            log.error("[Ayers Adapter][Login] ERROR! Failed to connect to GTS, error: {}", e.getMessage());
        }
    }

    @Override
    public BrokerType getBrokerType() {
        return BrokerType.AYERS;
    }

    @Override
    public boolean getBrokerAccountStatus(String brokerAccount) {
        return this.loggedIn.get();
    }

    private void login() {
        try {
            long msgid = RedisUtil.incrementAndGetAtomicLong(msgIdName);
            this.blockingQueueMap.put(msgid, new LinkedBlockingQueue());
            LoginRequest request = new LoginRequest(msgid, site, station, user, password, orderRecovery);
            this.sendRequest(request.toXML());
            this.loggedIn.set(true);
            raiseAdapterStatus(AdapterStatusType.ONLINE, false, null);
        } catch (JAXBException e) {
            log.error("[Ayers Adapter][Login] ERROR! " + e.getMessage());
        }
    }

    @Override
    public void connect() {
        init();
    }

    @Override
    public void disconnect(String brokerAccount) { //brokerAccount=null, 这里不需要这个字段但是为了格式一致
        try {
            while (this.conenctionLock.get()) {
                // 为了不让消息收到一半就被打断，所以这里会等一等。
                // 发消息是先 buffer 再 flush，而且 java.io.BufferedOutputStream.flush() 是 guarantee 消息可以被发送出去的，
                // 所以不会出现发到一半就被打断的情况。
            }
            log.info("[Ayers Adapter][Reconnect] Reconnecting to Ayers API");
            log.info("[Ayers Adapter][Reconnect] Closing previous Ayers connection");
            this.loggedIn.set(false);
            raiseAdapterStatus(AdapterStatusType.OFFLINE, false, null);
            this.bis.close();
            this.bos.close();
            this.socket.close();
            this.processingThread.interrupt();
            log.info("[Ayers Adapter][Reconnect] Ayers connection closed");
        } catch (IOException e) {
            log.error("[Ayers Adapter][Reconnect] ERROR! Failed to close previous Ayers connection", e);
        }
    }

    @Scheduled(fixedRate = 60000)
    public void keepAlive() {
        if (this.loggedIn.get()) {
            long msgnum = RedisUtil.incrementAndGetAtomicLong(msgIdName);
            this.blockingQueueMap.put(msgnum, new LinkedBlockingQueue());
            KeepAliveRequest keepAliveRequest = new KeepAliveRequest(msgnum);
            try {
                sendRequest(keepAliveRequest.toXML());
                Response response = pollResponseFromQueue(msgnum);
                if (response.getStatus() != 0) {
                    log.error("[Ayers Adapter][Keep Alive] ERROR! Failed (status != 0), Error: {}, ErrorCode: {}",
                            response.getError(), response.getErrorCode());
                }
            } catch (Exception e) {
                log.error("[Ayers Adapter][Keep Alive] ERROR! Exception. ErrMsg: {}", e.toString());
            }
        }
    }

    public void checkAndReconnect() {
        if (this.loggedIn.get()) {
            log.info("[Ayers Adapter][checkAndReconnect] tryAndAbortFairLock => ayersCheckAndReconnectLock");
            Lock ayersCheckAndReconnectLock = null;
            try {
                ayersCheckAndReconnectLock = RedisUtil.tryAndAbortFairLock(
                        "TradingEngineLocks-",
                        "AyersCheckAndReconnectLock",
                        60000,
                        0);
                if (ayersCheckAndReconnectLock == null) {
                    throw new TradingException(ResultStatusConstants.GET_LOCK_ERROR,
                            "[Ayers Adapter][checkAndReconnect] Fail to get ayersCheckAndReconnectLock!");
                }
                disconnect(null);
                try {
                    TimeUnit.SECONDS.sleep(1); // 不能重连过快
                } catch (Exception e) {
                    e.printStackTrace();
                }
                connect();
                try {
                    TimeUnit.SECONDS.sleep(30); // 连接以后不能释放锁, 否则之前的错误会不断触发重连
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } catch (TradingException e) {
                e.printStackTrace();
            } finally {
                RedisUtil.releaseLock(ayersCheckAndReconnectLock);
                log.info("[Ayers Adapter][checkAndReconnect] releaseLock => ayersCheckAndReconnectLock!");
            }
        }
    }

    public void sendRequest(String xml) {
        log.info("[Ayers Request][sendRequest] Sending request: {}", xml);
        try {
            byte[] bytes = MessageUtil.buildBytesFromString(xml);
            this.bos.write(bytes);
            this.bos.flush();
            log.info("[Ayers Request][sendRequest] sent");
        } catch (IOException e) {
            log.error("[Ayers Request][sendRequest] ERROR! " + e.getMessage());
        }
    }

    private void processResponse() {
        do {
            this.processingThread = Thread.currentThread();
            byte[] lengthBytes = new byte[4];
            try {
                this.bis.read(lengthBytes);
                this.conenctionLock.set(true);
                ByteBuffer byteBuffer = ByteBuffer.wrap(lengthBytes);
                byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
                int len = byteBuffer.getInt();
                if (len == 0) {
                    log.error("[Ayers Response][processResponse] ERROR! len=0");
                }
                byte[] bytes = new byte[len];
                byte[] shortenedBytes;
                byte[] actualBytes = new byte[0];

                int curReadLength = this.bis.read(bytes);
                int curEndingIdx = curReadLength;
                while (curEndingIdx < len) {
                    shortenedBytes = Arrays.copyOfRange(bytes, 0, curReadLength);
                    actualBytes = ArrayUtils.addAll(actualBytes, shortenedBytes);
                    bytes = new byte[len - curEndingIdx];
                    curReadLength = this.bis.read(bytes);
                    curEndingIdx += curReadLength;
                }
                shortenedBytes = Arrays.copyOfRange(bytes, 0, curReadLength);
                actualBytes = ArrayUtils.addAll(actualBytes, shortenedBytes);
                String strmsg = new String(actualBytes, UTF_8);
                if (StringUtils.isEmpty(strmsg)) {
                    log.warn("[Ayers Response][processResponse] Received data is empty");
                    continue;
                }
                log.info("[Response] Received data: {}", strmsg);
                this.conenctionLock.set(false);
                String str = strmsg.trim();
                JAXBContext jc;
                Unmarshaller unmarshaller;
                StringReader sr;
                try {
                    String messageType = MessageUtil.getMessageType(str);
                    switch (messageType) {
                        case MessageConstants.MESSAGE_TYPE_PORTFOLIO_RESPONSE:
                            jc = JAXBContext.newInstance(PortfolioResponse.class);
                            unmarshaller = jc.createUnmarshaller();
                            sr = new StringReader(str);
                            PortfolioResponse portfolioResponse = (PortfolioResponse) unmarshaller.unmarshal(sr);
                            log.info("[Ayers Response][processResponse][Portfolio Response] {}", portfolioResponse);
                            pushResultIntoQueue(portfolioResponse, Long.valueOf(portfolioResponse.getMsgnum()));
                            break;
                        case MessageConstants.MESSAGE_TYPE_RESPONSE:
                            jc = JAXBContext.newInstance(Response.class);
                            unmarshaller = jc.createUnmarshaller();
                            sr = new StringReader(str);
                            Response response = (Response) unmarshaller.unmarshal(sr);
                            log.info("[Ayers Response][processResponse][Response] {}", response);
                            pushResultIntoQueue(response, Long.valueOf(response.getMsgnum()));
                            break;
                        case MessageConstants.MESSAGE_TYPE_ORDER_RESPONSE:
                            jc = JAXBContext.newInstance(OrderEnquiryResponse.class);
                            unmarshaller = jc.createUnmarshaller();
                            sr = new StringReader(str);
                            OrderEnquiryResponse orderEnquiryResponse = (OrderEnquiryResponse) unmarshaller.unmarshal(sr);
                            log.info("[Ayers Response][processResponse][Order Enquiry Response] {}", orderEnquiryResponse);
                            pushResultIntoQueue(orderEnquiryResponse, Long.valueOf(orderEnquiryResponse.getMsgnum()));
                            break;
                        case MessageConstants.MESSAGE_TYPE_CASH_IO_NOTIFICATION:
                            jc = JAXBContext.newInstance(CashIONotification.class);
                            unmarshaller = jc.createUnmarshaller();
                            sr = new StringReader(str);
                            CashIONotification cashIONotification = (CashIONotification) unmarshaller.unmarshal(sr);
                            log.info("[Ayers Response][processResponse][Cash IO Notification] {}", cashIONotification);
                            break;
                        case MessageConstants.MESSAGE_TYPE_BALANCE_RESPONSE:
                            jc = JAXBContext.newInstance(BalanceResponse.class);
                            unmarshaller = jc.createUnmarshaller();
                            sr = new StringReader(str);
                            BalanceResponse balanceResponse = (BalanceResponse) unmarshaller.unmarshal(sr);
                            log.info("[Ayers Response][processResponse][Balance Response] {}", balanceResponse);
                            pushResultIntoQueue(balanceResponse, Long.valueOf(balanceResponse.getMsgnum()));
                            break;
                        default:
                            log.info("[Ayers Response][processResponse] Unknown message: {}", str);
                    }
                } catch (JAXBException e) {
                    log.error("[Ayers Response][processResponse] ERROR! Unable to parse XML due to: {}\n Full error: {}", e.getMessage(), e.toString());
                } catch (NullPointerException e) {
                    log.error("[Ayers Response][processResponse] ERROR! NullPointer Exception when parsing xml," +
                            "str: {} Message: {}\n Full error: {}", str, e.getMessage(), e.toString());
                }
            } catch (IOException e) {
                log.error("[Ayers Response][processResponse] ERROR! Ayers Reader closed: " + e.getMessage());
                checkAndReconnect();
                break;
            } catch (Exception ex) {
                log.error("[Ayers Response][processResponse] ERROR! " + ex.getMessage(), ex);
                checkAndReconnect();
                break;
            }
        } while (true);
    }

    private void pushResultIntoQueue(Object object, Object qType) {
        BlockingQueue queue = this.blockingQueueMap.get(qType);
        if (queue == null) {
            log.warn("[Ayers Adapter][pushResultIntoQueue] No queue available for " + object);
            queue = new LinkedBlockingQueue();
            this.blockingQueueMap.put(qType, queue);
        }
        try {
            while (queue.peek() != null) {
                queue.remove();
            }
            boolean success = queue.offer(object, 1, TimeUnit.SECONDS);
            if (!success) {
                log.error("[Ayers Adapter][pushResultIntoQueue] ERROR! Failed to push result after timeout: " + object);
            }
        } catch (InterruptedException e) {
            log.warn("[Ayers Adapter][pushResultIntoQueue] Failed to push result due to interruption");
        }
    }

    public <T> T pollResponseFromQueue(Object qType) throws TradingException {
        if (!blockingQueueMap.containsKey(qType)) {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_AYERS_ERROR,
                    "Queue not found, failed to get msg type " + qType);
        }
        BlockingQueue queue = this.blockingQueueMap.get(qType);
        try {
            Object res = queue.poll(60L, TimeUnit.SECONDS);
            if (res == null) {
                log.error("[Ayers Adapter][pollResponseFromQueue] ERROR! " +
                        "Can not receive any response and will try to updateBrokerAllOpenOrderAndExecutions()...");
                log.error("[Ayers Adapter][pollResponseFromQueue] ERROR! Can not receive any response and will reconnect...");
                checkAndReconnect();
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_AYERS_ERROR,
                        "Receive null, failed to get type = " + qType);
            }
            blockingQueueMap.remove(qType);
            return (T) res;
        } catch (InterruptedException e) {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_AYERS_ERROR,
                    "Queue interrupted, failed to get type = " + qType);
        }
    }

    @Override
    public SubAccount querySubAccountSummary(SubAccount subAccount) throws TradingException {
        String brokerAccountId = subAccount.getAccount().getBrokerAccount();
        long msgnum = RedisUtil.incrementAndGetAtomicLong(msgIdName);
        this.blockingQueueMap.put(msgnum, new LinkedBlockingQueue());

        EnquiryRequest request = new EnquiryRequest(brokerAccountId, msgnum);
        String xml;
        try {
            xml = request.toXML();
        } catch (JAXBException e) {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_AYERS_ERROR, "Failed to send portfolio request due to: " + e.getMessage());
        }
        Object response;
        try {
            this.sendRequest(xml);
            response = pollResponseFromQueue(msgnum);
        } catch (TradingException tradingException) {
            if (tradingException.getMessage().startsWith("Receive null")) {
                this.sendRequest(xml);
                response = pollResponseFromQueue(msgnum);
            }
            else
                throw tradingException;
        }
        log.info("[Ayers Adapter][querySubAccountSummary] " + response);
        if (response instanceof PortfolioResponse) {
            PortfolioResponse summaryResponse = (PortfolioResponse) response;
            SubAccountSummary subAccountSummary = subAccount.getSubAccountSummary();
            // 记录 基础信息
            subAccountSummary.setCash(summaryResponse.getTotalCash());
            subAccountSummary.setBuyPower(summaryResponse.getTotalCash());
            subAccountSummary.setAssetValue(summaryResponse.getTotalPrevMktValue());
            Map<CurrencyType, CashInfo> cashInfoMap = new HashMap<>();
            if (summaryResponse.getCashPos() != null && summaryResponse.getCashPos().getCash() != null) {
                // 组装 多币种 cashInfo Map
                List<CashPosition> cashPositions = summaryResponse.getCashPos().getCash();
                for (CashPosition cashPosition : cashPositions) {
                    CashInfo cashInfo = new CashInfo();
                    CurrencyType currencyType = CurrencyType.valueOf(cashPosition.getCcy());
                    cashInfo.setCurrencyType(currencyType);
                    cashInfo.setBalance(cashPosition.getAmt());
                    cashInfo.setPurchasePower(cashPosition.getAmt());
                    cashInfoMap.put(currencyType, cashInfo);
                }
            }
            subAccountSummary.setCashInfoMap(cashInfoMap);
        }
        if (response instanceof Response) {
            if (((Response) response).getStatus() != 0) {
                log.error("[Ayers Adapter][pushResultIntoQueue] ERROR! Failed to retrieve account info due to: " + ((Response) response).getError());
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_AYERS_ERROR, "Invalid account");
            }
        }
        log.info("[Ayers Adapter][querySubAccountSummary] save=> " + subAccount.toString());
        return subAccount;
    }

    @Override
    public List<Holding> queryHolding(String brokerAccount) throws TradingException {
        long msgnum = RedisUtil.incrementAndGetAtomicLong(msgIdName);
        this.blockingQueueMap.put(msgnum, new LinkedBlockingQueue());

        PortfolioRequest request = new PortfolioRequest(brokerAccount, msgnum);
        String xml;
        try {
            xml = request.toXML();
        } catch (JAXBException e) {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_AYERS_ERROR, "Failed to send portfolio request due to: " + e.getMessage());
        }
        Object response;
        try {
            this.sendRequest(xml);
            response = pollResponseFromQueue(msgnum);
        } catch (TradingException tradingException) {
            if (tradingException.getMessage().startsWith("Receive null")) {
                this.sendRequest(xml);
                response = pollResponseFromQueue(msgnum);
            }
            else
                throw tradingException;
        }
        log.info("[Ayers Adapter][queryHolding] " + response);
        List<Holding> holdingList = new ArrayList<>();
        if (response instanceof PortfolioResponse) {
            if (((PortfolioResponse) response).getProductPos() == null) {
                log.error("[Ayers Adapter][queryHolding] ERROR! getProductPos is null");
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_AYERS_ERROR, "getProductPos is null, can not get holding!");
            }
            if (((PortfolioResponse) response).getProductPos().getProduct() == null) { // getProduct()为空表示空仓
                return holdingList;
            }
            List<ProductPosition> productPos = ((PortfolioResponse) response).getProductPos().getProduct();
            for (ProductPosition productPosition : productPos) {
                try {
                    log.info("[Ayers Adapter][queryHolding] " + productPosition);
                    if (productPosition.getQty().compareTo(BigDecimal.ZERO) != 0) {
                        Holding holding = new Holding();
                        holding.setBrokerType(BrokerType.AYERS);
                        holding.setBrokerAccount(brokerAccount);
                        String symbol = productPosition.getProductCode();
                        String country = productPosition.getExchangeCode();
                        if (productPosition.getExchangeCode().equals("HKEX")) {
                            symbol = productPosition.getProductCode().substring(1);
                            country = "HK";
                        }
                        if (productPosition.getExchangeCode().equals("SHA")) {
                            country = "CN";
                        }
                        if (productPosition.getExchangeCode().equals("SZA")) {
                            country = "CN";
                        }
                        InstrumentDataMaster instrumentDataMaster = instrumentUtil.getInstrumentFromDM(
                                symbol, null, null, null, country
                        );
                        holding.setInstrumentId(instrumentDataMaster.getInstrumentId());
                        holding.setInstrumentInfo(instrumentUtil.getInstrumentKey(instrumentDataMaster));
                        holding.setHoldingPosition(productPosition.getQty());
                        holding.setHoldingPrice(productPosition.getAvgCost());
                        holding.setHoldingAmount(productPosition.getQty().multiply(productPosition.getAvgCost()));
                        holdingList.add(holding);
                    }
                } catch (CommonException e) {
                    log.error("[Ayers Adapter][queryHolding] ERROR! fail to add one holding => " + productPosition);
                    e.printStackTrace();
                }
            }
        } else if (response instanceof Response) {
            if (((Response) response).getStatus() != 0) {
                log.error("[Ayers Adapter][queryHolding] ERROR! Failed to retrieve account info due to: " + ((Response) response).getError());
            }
            log.error("[Ayers Adapter][queryHolding] ERROR! fail to receive the PortfolioResponse");
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_AYERS_ERROR, " fail to receive the PortfolioResponse, can not get holding!");
        }
        return holdingList;
    }

    @Override
    public boolean isAccountValid(String brokerAccount) {
        long msgnum = RedisUtil.incrementAndGetAtomicLong(msgIdName);
        this.blockingQueueMap.put(msgnum, new LinkedBlockingQueue());

        EnquiryRequest request;
        try {
            request = new EnquiryRequest(brokerAccount, msgnum);
            this.sendRequest(request.toXML());
        } catch (JAXBException jaxbException) {
            log.error("[Ayers Adapter][Connect] BrokerAccount({}) connect failed to send request due to: {}", brokerAccount, jaxbException.getMessage());
            return true;
        }

        Object response;
        try {
            response = pollResponseFromQueue(msgnum);
        } catch (TradingException tradingException) {
            log.error("[Ayers Request][Connect] BrokerAccount({}) connect failed to poll response due to: {}", brokerAccount, tradingException.getMessage());
            return true;
        }
        if (response instanceof Response) {
            if (((Response) response).getStatus() != 0 && ((Response) response).getErrorCode().equalsIgnoreCase("Error.InvalidUserOrPassword" )) {
                log.error("[Ayers Adapter][Connect] BrokerAccount({}) connect is not valid, failed to retrieve account info due to: " + ((Response) response).getError());
                return false;
            }
        }
        return true;
    }
}
