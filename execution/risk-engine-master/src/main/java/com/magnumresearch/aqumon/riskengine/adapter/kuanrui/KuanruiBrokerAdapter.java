package com.magnumresearch.aqumon.riskengine.adapter.kuanrui;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.common.exception.CommonException;
import com.magnumresearch.aqumon.riskengine.adapter.BaseBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.adapter.kuanrui.config.AccountSettings;
import com.magnumresearch.aqumon.riskengine.adapter.kuanrui.constants.KuanruiConstants;
import com.magnumresearch.aqumon.riskengine.adapter.kuanrui.util.KuanruiUtils;
import com.magnumresearch.aqumon.riskengine.dao.*;
import com.magnumresearch.aqumon.riskengine.model.ExtExecution;
import com.magnumresearch.aqumon.riskengine.pojo.OfficialAndDBCashInfo;
import com.magnumresearch.aqumon.riskengine.service.HoldingDataService;
import com.magnumresearch.aqumon.trading.constants.*;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.*;
import com.magnumresearch.aqumon.trading.pojo.HoldingFilter;
import com.magnumresearch.aqumon.trading.pojo.InstrumentDataMaster;
import com.magnumresearch.aqumon.trading.utils.InstrumentUtil;
import com.quant360.api.callback.OesCallBack;
import com.quant360.api.client.Client;
import com.quant360.api.client.OesClient;
import com.quant360.api.exception.IllegalStatusException;
import com.quant360.api.model.oes.*;
import com.quant360.api.model.oes.enu.*;
import com.quant360.api.utils.TimeUtils;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@ConditionalOnProperty(prefix = "adapter.kuanrui", value = {"enabled"})
public class KuanruiBrokerAdapter extends BaseBrokerAdapter {

    @Autowired
    OesClient oesClient;
    @Autowired
    OrderDao orderDao;
    @Autowired(required = false)
    KuanruiUtils kuanruiUtils;
    @Autowired
    AccountSettings accountSettings;
    @Autowired
    HoldingDao holdingDao;
    @Autowired
    InstrumentUtil instrumentUtil;
    @Autowired
    HoldingDataService holdingDataService;
    @Autowired
    PortfolioTaskDao portfolioTaskDao;
    @Autowired
    SubAccountSummaryDao subAccountSummaryDao;

    Logger oeslog = LoggerFactory.getLogger(this.getClass());
    Logger log = LoggerFactory.getLogger(this.getClass());

    String username;
    String pwd;
    int onDisConnCounter = 0;
    private final Integer envId = 0;
    public static AtomicInteger ordSeq = null;
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    private int C_Test_Temp_ExtOrderId_Add = 0;
    private static LocalTime tradingStart = LocalTime.of(8, 30, 0);
    private static LocalTime tradingEnd = LocalTime.of(17, 30, 0);

    private class OesCallbackImpl extends OesCallBack {

        @Override
        public void onDisConn(OesClient client) {
            if (inTradingSession() && getKrIsLive()) {
                oeslog.debug("OES Client disconnected. Try to reconnect.");
                if (oesClient.getStatus() == Client.ClientStatus.Start) {
                    log.info("[断线重连]交易系统状态: " + oesClient.getStatus() + ", 不进行重登录");
                } else {
                    log.info("[断线重连]交易系统状态: " + oesClient.getStatus() + ", 进行重新登录......");
                    oesClient.close();
                    try {
                        init_with_username_and_password(username, pwd);
                    } catch (CommonException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public boolean inTradingSession() {
        LocalTime now = LocalTime.now();
        if (getKrIsLive() == false) {
            return true;
        } else if (now.compareTo(tradingStart) > 0 && now.compareTo(tradingEnd) < 0) {
            return true;
        } else {
            return false;
        }
    }

    @PostConstruct
    public void init() {
        if (inTradingSession()) {
            username = accountSettings.getKrAccountActive();
            pwd = accountSettings.getKrAccountPasswordActive();

            oesClient.initCallBack(new OesCallbackImpl());
            OesReportSynchronizationReq rptSyncReq = new OesReportSynchronizationReq();
            rptSyncReq.setLastRptSeqNum(-1);
            rptSyncReq.setSubscribeEnvId(99);
            rptSyncReq.setSubscribeRptTypes(OesSubscribeReportType.OES_SUB_RPT_TYPE_ALL.value());
            oesClient.initRptSync(rptSyncReq);
            OesLogonReq logonReq = new OesLogonReq();
            logonReq.setUsername(username);
            logonReq.setPassword(pwd);

            OesReq msg = new OesReq();
            msg.setLogonReq(logonReq);

            log.info("OES Client logging in");
            try {
                ErrorCode code = oesClient.start(msg);
                switch (code) {
                    case SUCCESS:
                        log.info("OES客户端: {} 已成功登陆!", username);
                        raiseAdapterStatus(AdapterStatusType.ONLINE, false, null);
                        break;
                    case CLIENT_DISABLE:
                        log.info("OESClient: {} login failed! [Client已被禁用] errorCode = {}", username, code);
                    case INVALID_CLIENT_NAME:
                        log.info("OESClient: {} login failed！[非法的Client登陆用户名称 ] errorCode = {}", username, code);
                    case INVALID_CLIENT_PASSWORD:
                        log.info("OESClient: {} login failed！ [Client密码不正确] errorCode = {}", username, code);
                    case CLIENT_REPEATED:
                        log.info("OESClient: {} login failed！ [Client重复登录] errorCode = {}", username, code);
                    case CLIENT_CONNECT_OVERMUCH:
                        log.info("OESClient: {} login failed！ [Client连接数量过多] errorCode = {}", username, code);
                    case INVALID_PROTOCOL_VERSION:
                        log.info("OESClient: {} login failed! [API协议版本不兼容] errorCode = {}", username, code);
                    case OTHER_ERROR:
                        log.info("OESClient: {} login failed！ [其他错误] errorCode = {}", username, code);
                        raiseAdapterStatus(AdapterStatusType.OFFLINE, true, code.name());
                }
            } catch (Exception e) {
                raiseAdapterStatus(AdapterStatusType.OFFLINE, true, e.getLocalizedMessage());
                log.error("Failed to initiate OES client due to: " + e.getMessage());
            }
        }
    }

    public void init_with_username_and_password(String username, String pwd) throws TradingException {
        if (inTradingSession()) {
            this.username = username;
            this.pwd = pwd;

            oesClient.initCallBack(new OesCallbackImpl());
            OesReportSynchronizationReq rptSyncReq = new OesReportSynchronizationReq();
            rptSyncReq.setLastRptSeqNum(-1);
            rptSyncReq.setSubscribeEnvId(99);
            rptSyncReq.setSubscribeRptTypes(OesSubscribeReportType.OES_SUB_RPT_TYPE_ALL.value());
            oesClient.initRptSync(rptSyncReq);
            OesLogonReq logonReq = new OesLogonReq();
            logonReq.setUsername(username);
            logonReq.setPassword(pwd);

            OesReq msg = new OesReq();
            msg.setLogonReq(logonReq);

            log.info("OES Client logging in");
            try {
                ErrorCode code = oesClient.start(msg);
                switch (code) {
                    case SUCCESS:
                        log.info("OES客户端: {} 已成功登陆!", username);
                        raiseAdapterStatus(AdapterStatusType.ONLINE, false, null);
                        break;
                    case CLIENT_DISABLE:
                        log.info("OESClient: {} login failed! [Client已被禁用] errorCode = {}", username, code);
                    case INVALID_CLIENT_NAME:
                        log.info("OESClient: {} login failed！[非法的Client登陆用户名称 ] errorCode = {}", username, code);
                    case INVALID_CLIENT_PASSWORD:
                        log.info("OESClient: {} login failed！ [Client密码不正确] errorCode = {}", username, code);
                    case CLIENT_REPEATED:
                        log.info("OESClient: {} login failed！ [Client重复登录] errorCode = {}", username, code);
                    case CLIENT_CONNECT_OVERMUCH:
                        log.info("OESClient: {} login failed！ [Client连接数量过多] errorCode = {}", username, code);
                    case INVALID_PROTOCOL_VERSION:
                        log.info("OESClient: {} login failed! [API协议版本不兼容] errorCode = {}", username, code);
                    case OTHER_ERROR:
                        log.info("OESClient: {} login failed！ [其他错误] errorCode = {}", username, code);
                        raiseAdapterStatus(AdapterStatusType.OFFLINE, true, code.name());
                }
            } catch (Exception e) {
                raiseAdapterStatus(AdapterStatusType.OFFLINE, true, e.getLocalizedMessage());
                log.error("Failed to initiate OES client due to: " + e.getMessage());
            }
        } else {
            oesClient.close();
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_KUANRUI_RELOGON_ERROR, "Not in trading time!");
        }
    }

    @SneakyThrows
    @Override
    //Get account info and account summary info
    public SubAccount querySubAccountSummary(SubAccount subAccount) throws TradingException {
        SubAccountSummary subAccountSummary = subAccount.getSubAccountSummary();
        if (inTradingSession()) {
            BigDecimal net_liquidation_value = subAccountSummary.getCash();
            HoldingFilter filter = new HoldingFilter();
            filter.setSubAccountId(subAccount.getId());
            filter.setBrokerType(BrokerType.KR);
            Set<String> accountSet = new HashSet<>();
            accountSet.add(subAccount.getBrokerAccount());
            List<Holding> List_Holding = holdingDataService.getCurrentHoldingsFromDB(BrokerType.KR, accountSet, null);
            List<String> instrumentIds = new LinkedList<>();
            for (Holding h : List_Holding) {
                instrumentIds.add(h.getInstrumentId());
            }
            Map<String, BigDecimal> price_map = kuanruiUtils.getPrice(instrumentIds);
            for (Holding h : List_Holding) {
                BigDecimal price;
                price = new BigDecimal(price_map.get(h.getInstrumentId()).toString());
                BigDecimal holding_position = h.getHoldingPosition();
                BigDecimal holding_value = price.multiply(holding_position);
                net_liquidation_value = net_liquidation_value.add(holding_value);
            }
            subAccountSummary.setNetLiquidationValue(net_liquidation_value);
            subAccountSummary.setAssetValue(subAccountSummary.getCash());
            subAccountSummaryDao.save(subAccountSummary);
            return subAccount;
        } else {
            /*
            宽睿实盘收市后，api并不会立即关闭，api会持续可用到17:30左右。
            而如果在这个时间后，task engine post宽睿实盘任务，会由于拿不到net_liquidation_value，而失败。
            为此，添加了这里的逻辑。
            这里的逻辑是，如果api已经不可用，会检查数据库里的net_liquidation_value的值是否是在当天15:00收市后计算的。
            如果是，那么可以认为这个值是准确的，并返回subAccount信息；否则，直接抛出异常，防止task engine post任务时获取到错误的net_liquidation_value。
             */
            SimpleDateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd");
            String currentDate = dateFormat.format(new Date());
            String updateDateAndHour = new SimpleDateFormat("yyyy-MM-dd:HH").format(subAccountSummary.getUpdateTime());
            String updateDate = updateDateAndHour.split(":")[0];
            String updateHour = updateDateAndHour.split(":")[1];
            if (currentDate.equals(updateDate) && (Integer.parseInt(updateHour) >= 15)) {
                return subAccount;
            } else {
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_KUANRUI_GET_SUBACOUNTSUMMARY_ERROR, "The kuanrui api was closed and the subaccount summary in database was updated at " + updateDateAndHour);
            }
        }
    }

    @Override
    public List<Holding> queryHolding(String brokerAccount) throws TradingException {
        List<Holding> resultHoldings = new LinkedList<>();
        String[] mktBrokerAccountMap = KuanruiUtils.getMktBrokerAccountMap(brokerAccount);
        for (String mktBrokerAccount : mktBrokerAccountMap) {
            List<Holding> resultHolding = queryHoldingBySubBrokerAccount(mktBrokerAccount);
            resultHoldings.addAll(resultHolding);
        }
        //将所有券商持仓的broker_account改为cash账户，不区分深圳或上海
        for (Holding holding : resultHoldings) {
            holding.setBrokerAccount(kuanruiUtils.getKRCashAccount(holding.getBrokerAccount()));
        }
        return resultHoldings;
    }

    private List<Holding> queryHoldingBySubBrokerAccount(String brokerAccount) throws TradingException {
        OesQryStkHoldingFilter filter = new OesQryStkHoldingFilter();
        List<Holding> results = new LinkedList<>();
        filter.setInvAcctId(brokerAccount);
        oeslog.debug("[REQUEST] queryHolding, filter = {}", gson.toJson(filter));
        OesQryStkHoldingRsp stkHoldingRsp;
        try {
            stkHoldingRsp = oesClient.queryStkHolding(filter, Client.QueryMode.ALL);
        } catch (IOException e) {
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_KUANRUI_ERROR, "no response when oesClient.queryStkHolding()!");
        } catch (IllegalStatusException illegalStatusException) {
            testOrdChannel();
            try {
                stkHoldingRsp = oesClient.queryStkHolding(filter, Client.QueryMode.ALL);
            } catch (IOException e1) {
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_KUANRUI_ERROR, "no response when oesClient.queryStkHolding()!");
            } catch (IllegalStatusException illegalStatusException1) {
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_KUANRUI_ERROR, illegalStatusException1);
            }
        }
        if (Objects.isNull(stkHoldingRsp))
            throw new TradingException(ResultStatusConstants.TRADING_ENGINE_KUANRUI_ERROR, "oesClient.queryStkHolding() returns null!");

        oeslog.debug("[RESPONSE] queryHolding. response:\n{}", gson.toJson(stkHoldingRsp));

        if (stkHoldingRsp.getQryItems() != null) {
            for (OesStkHoldingItem item : stkHoldingRsp.getQryItems()) {
                Holding holding = new Holding();
                holding.setBrokerType(BrokerType.KR);
                holding.setBrokerAccount(brokerAccount);
                String exchange;
                if (item.getMktId() == OesMarketId.OES_MKT_ID_SZ_A) {
                    exchange = "SEHKSZSE";
                } else {
                    exchange = "SEHKNTL";
                }
                SecType secType = kuanruiUtils.getSecTypeByOESSecurityType(item.getSecurityType());
                try {
                    String instrumentId = kuanruiUtils.getInstrumentIdBySecurityId(exchange, item.getSecurityType(), item.getSecurityId());
                    holding.setInstrumentId(instrumentId);
                    InstrumentDataMaster instrumentDataMaster = instrumentUtil.getInstrumentFromDM(instrumentId);
                    holding.setInstrumentInfo(instrumentUtil.getInstrumentKey(instrumentDataMaster));
                } catch (CommonException e) {
                    // 如果datamaster里面没有，就记为unknown，继续查询下一个
                    log.error(e.getMessage());
                    holding.setInstrumentId("unknown");
                    holding.setInstrumentInfo(item.getSecurityId() + "_" + null + "_" + CurrencyType.CNY.getCurrency() + "_" + secType.toString());
                }
                holding.setHoldingPosition(BigDecimal.valueOf(item.getSumHld()));
                holding.setHoldingPrice(BigDecimal.valueOf(item.getCostPrice(), KuanruiConstants.PRICE_PRECISION));
                if (item.getOriginalHld() > 0) {
                    holding.setLongVolumeHistory(BigDecimal.valueOf(item.getOriginalHld()));
                    holding.setShortVolumeHistory(BigDecimal.ZERO);
                } else if (item.getOriginalHld() < 0) {
                    holding.setLongVolumeHistory(BigDecimal.ZERO);
                    holding.setShortVolumeHistory(BigDecimal.valueOf(item.getOriginalHld()));
                } else {
                    holding.setLongVolumeHistory(BigDecimal.ZERO);
                    holding.setShortVolumeHistory(BigDecimal.ZERO);
                }
                holding.setLongVolumeToday(BigDecimal.valueOf(item.getTotalBuyHld()));
                holding.setShortVolumeToday(BigDecimal.valueOf(item.getTotalSellHld()));
                results.add(holding);
            }
        }

        return results;
    }

    @Override
    public BrokerType getBrokerType() {
        return BrokerType.KR;
    }

    public synchronized int getNextOrderId() {
        if (ordSeq != null) {
            return ordSeq.incrementAndGet();
        } else {
            // Fixed (jopseh): cast ext_order_id from varchar to int, in order to sort the data correctly
            // Order order1 = orderDao.findFirstByBrokerIdOrderByExtOrderIdDesc(getBrokerType());
            Order order = orderDao.findFirstByBrokerTypeOrderByExtOrderIdAsIntDesc(getBrokerType());

            if (!(order == null || order.getExtOrderId() == null || order.getExtOrderId().isEmpty())) {
                try {
                    ordSeq = new AtomicInteger(Integer.valueOf(order.getExtOrderId()));
                    return ordSeq.incrementAndGet();

                } catch (NumberFormatException e) {
                    log.warn("Failed to recognize last used order id, resorting to random number");
                }
            }
            ordSeq = new AtomicInteger(new Random().nextInt(1000));
        }
        return ordSeq.incrementAndGet();
    }

    // 测试连接，如果测试失败，视oesclient状态尝试进行重登录
    public void testOrdChannel() throws TradingException {
        if (inTradingSession()) {
            try {
                OesTestRequestReq req = new OesTestRequestReq();
                req.setTestReqId("test001");
                req.setSendTime(TimeUtils.timeToyyyyMMddHHmmssSSS());
                oesClient.testOrdChannel(req);
                onDisConnCounter = 0;
            } catch (Exception e) {
                log.error("提交委托通道测试请求失败！", e);
                if (getKrIsLive()) {
                    onDisConnCounter++;
                    if (onDisConnCounter >= 3) {
                        log.info("[断线重连]交易系统状态: " + oesClient.getStatus() + ", 由于测试交易通道失败达到 " + onDisConnCounter + " 次，进行重新登录......");
                        oesClient.close();
                        init_with_username_and_password(username, pwd);
                    } else if (oesClient.getStatus() == Client.ClientStatus.Start) {
                        log.info("[断线重连]交易系统状态: " + oesClient.getStatus() + ", 不进行重登录");
                    } else {
                        log.info("[断线重连]交易系统状态: " + oesClient.getStatus() + ", 进行重新登录......");
                        onDisConnCounter = 0;
                        oesClient.close();
                        init_with_username_and_password(username, pwd);
                    }
                }
            }
        }
    }

    public Map<String, ExtExecution> qryTrade(BrokerType brokerType, String brokerAccount, int startTime, int endTime, int ext_Order_Id, int cl_env_id) {
        //List<ExtOrder> extOrder_list = new LinkedList<ExtOrder>();
        Map<String, ExtExecution> ids_extExecution_map = new HashMap<String, ExtExecution>();
        OesQryTrdFilter qryFilter = new OesQryTrdFilter();
        if (!brokerAccount.equals("-1")) {
            qryFilter.setCustId(brokerAccount);
        }
        if (startTime != -1) {
            qryFilter.setTrdStartTime(startTime);
        }
        if (endTime != -1) {
            qryFilter.setTrdEndTime(endTime);
        }
        if (ext_Order_Id != -1) {
            qryFilter.setClSeqNo(ext_Order_Id);
        }
        if (ext_Order_Id != -1) {
            qryFilter.setClEnvId(cl_env_id);
        }
        OesQryTrdRsp rsp = null;
        try {
            rsp = oesClient.queryTrade(qryFilter, Client.QueryMode.ALL);
            System.out.println(rsp.getQryItems());
            List<OesTrdItem> items = rsp.getQryItems();
            for (OesTrdItem item : items) {
                ExtExecution extExecution = new ExtExecution();
                if (item.getTrdSide() == OesBuySellType.OES_BS_TYPE_B) {
                    extExecution.setBuy_sell_type("BUY");
                    extExecution.setDirection(0);
                } else {
                    extExecution.setBuy_sell_type("SELL");
                    extExecution.setDirection(1);
                }
                extExecution.setPriceSent(new BigDecimal(item.getOrigOrdPrice()).divide(new BigDecimal(10000), 8, BigDecimal.ROUND_DOWN)); //original order price
                extExecution.setPriceFilled(new BigDecimal(item.getTrdPrice()).divide(new BigDecimal(10000), 8, BigDecimal.ROUND_DOWN)); //Price
                extExecution.setQuantitySent(new BigDecimal(item.getOrigOrdQty())); //Total quantity
                extExecution.setQuantityFilled(new BigDecimal(item.getTrdQty())); //Filled quantity
                extExecution.setAmountFilled(extExecution.getPriceFilled().multiply(extExecution.getQuantityFilled()));

                extExecution.setBrokerType(brokerType);

                SecType secType = kuanruiUtils.getSecTypeByOESSecurityType(item.getSecurityType());
                String instrumentId = "";
                String exchange;
                if (item.getMktId() == OesMarketId.OES_MKT_ID_SZ_A) {
                    exchange = "SEHKSZSE";
                } else {
                    exchange = "SEHKNTL";
                }
                try {
                    instrumentId = kuanruiUtils.getInstrumentIdBySecurityId(exchange, item.getSecurityType(), item.getSecurityId());
                    //instrumentId = instrumentUtil.getInstrumentIdFromDM(item.getSecurityId(), exchange, CurrencyType.CNY.getCurrency(), secType.toString());
                } catch (Exception e) {
                    instrumentId = "Can not fetch instrument id from datamaster, " + item;
                    e.printStackTrace();
                }
                //extExecution.setInstrumentId(KuanruiUtils.getInstrumentIdBySecurityId(item.getSecurityId()));
                extExecution.setInstrumentId(instrumentId);
                extExecution.setTrade_time(item.getTrdTime()); //Trade time
                extExecution.setExt_order_id(String.valueOf(item.getClSeqNo())); //ext_order_id
                extExecution.setExt_execution_id(String.valueOf(item.getExchTrdNum())); //external trade num
                extExecution.setOrder_status(item.getOrdStatus()); //status
                extExecution.setCl_env_id(item.getClEnvId()); //environment id
                extExecution.setBroker_account(item.getInvAcctId()); //set broker account
                extExecution.setOes_ord_type(item.getOrdType());
                String date = item.getTrdDate() + "";
                date = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8);
                extExecution.setDate(date);
                //extexecutionDao.save(extExecution);

                ids_extExecution_map.put(extExecution.getEOTId(), extExecution);
                //extOrder_list.add(extOrder);
            }
        } catch (IOException | CommonException e) {
            e.printStackTrace();
        }
        //return extOrder_list;
        return ids_extExecution_map;
    }

    public OfficialAndDBCashInfo checkAccountInfo(String brokerAccount) throws Exception {
        OesQryCashAssetFilter filter = new OesQryCashAssetFilter();
        filter.setCashAcctId(brokerAccount);
        OesQryCashAssetRsp rsp = oesClient.queryCashAsset(filter, Client.QueryMode.ALL);
        OfficialAndDBCashInfo officialAndDBCashInfo = new OfficialAndDBCashInfo();

        for (OesCashAssetItem item : rsp.getQryItems()) {
            officialAndDBCashInfo.setOfficial_balance(officialAndDBCashInfo.getOfficial_balance().add(BigDecimal.valueOf(item.getCurrentTotalBal(), KuanruiConstants.PRICE_PRECISION)));
            officialAndDBCashInfo.setOfficial_freeze_balance(officialAndDBCashInfo.getOfficial_freeze_balance().add(BigDecimal.valueOf(item.getBuyFrzAmt(), KuanruiConstants.PRICE_PRECISION)));
        }

        return officialAndDBCashInfo;
    }

    public BigDecimal getSubAccountHoldingsValue(SubAccount subAccountInfo) {
        BigDecimal asset_value = BigDecimal.ZERO;
        List<Holding> sub_account_holding_list = new LinkedList<Holding>();
        sub_account_holding_list = holdingDao.findBySubAccountId(subAccountInfo.getId());
        for (Holding h : sub_account_holding_list) {
            BigDecimal price = null;
            try {
                //price = BigDecimal.valueOf(kuanruiUtils.getPrice(h.getInstrumentId()), KuanruiConstants.PRICE_PRECISION);
                price = kuanruiUtils.getPrice(Collections.singletonList(h.getInstrumentId())).get(h.getInstrumentId());
            } catch (Exception e) {
                //Need other actions
                e.printStackTrace();
            }
            BigDecimal quantity = h.getHoldingPosition().abs();
            BigDecimal local_value = price.multiply(quantity);
            asset_value = asset_value.add(local_value);
        }
        return asset_value;
    }

    public boolean reLogon(String username, String pwd) throws CommonException {
        oesClient.close();
        init_with_username_and_password(username, pwd);
        if (oesClient.getStatus() == Client.ClientStatus.Start) {
            return true;
        } else {
            return false;
        }
    }

    public String getKRUsername() {
        return this.username;
    }

    public boolean getKrIsLive() {
        return accountSettings.getKrIsLive();
    }

    public Map checkSubtaskAvailabilityWithDM(long taskId) throws TradingException {
        String target_weight = portfolioTaskDao.findById(taskId).get().getTargetWeight();
        String[] target_weight_splitted = target_weight.split("\"");
        List<String> instrumentIdList = new LinkedList<>();
        Map<String, String> errorInstrumentIdMap = new HashMap<>();
        for (int i = 1; i < target_weight_splitted.length; i = i + 2) {
            String s = target_weight_splitted[i].replaceAll("\"", "");
            instrumentIdList.add(s);
        }
        for (String instrumentId : instrumentIdList) {
            instrumentUtil.clearInstrumentCache();
            String exchange;
            String securityId;
            Object securityType;
            SecType secType;
            try {
                InstrumentDataMaster instrumentDataMaster = instrumentUtil.getInstrumentFromDM(instrumentId);
                secType = instrumentDataMaster.getSecType();
                if (secType == SecType.OPT) {
                    securityType = OesSecurityType.OES_SECURITY_TYPE_OPTION;
                } else if (secType == SecType.STK || secType == SecType.ETF) {
                    securityType = OesSecurityType.OES_SECURITY_TYPE_STOCK;
                } else {
                    errorInstrumentIdMap.put("InstrumentId >>> symbol error: SecType " + secType + " not supported.", instrumentId);
                    continue;
                }
                exchange = instrumentDataMaster.getExchange();
                securityId = instrumentDataMaster.getSymbol();
            } catch (Exception e) {
                errorInstrumentIdMap.put("InstrumentId >>> symbol error.", instrumentId);
                continue;
            }
            instrumentUtil.clearInstrumentCache();
            try {
                String instrumentId_fetched = kuanruiUtils.getInstrumentIdBySecurityId(exchange, securityType, securityId);
                boolean check = instrumentId.equals(instrumentId_fetched);
                if (!check) {
                    errorInstrumentIdMap.put("Symbol >>> instrumentId error: InstrumentId fetched not matched. Instrument Id is: " + instrumentId + ", InstrumentId fetched is: " + instrumentId_fetched, instrumentId);
                }
            } catch (Exception e) {
                errorInstrumentIdMap.put("Symbol >>> instrumentId error.", instrumentId);
            }

        }
        if (errorInstrumentIdMap.size() == 0) {
            errorInstrumentIdMap.put("All test pass", instrumentIdList.toString());
        }
        return errorInstrumentIdMap;
    }

    @Scheduled(cron = "0 * * * * 1-5")
    public void keepAlive() throws TradingException {
        // 测试连接，如果测试失败，视oesclient状态尝试进行重登录
        log.info("[OES] Heartbeat!");
        testOrdChannel();
    }


}
