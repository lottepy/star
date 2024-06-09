package com.magnumresearch.aqumon.riskengine.adapter.kuanrui.util;

import com.magnumresearch.aqumon.common.constants.ResultStatusConstants;
import com.magnumresearch.aqumon.common.exception.CommonException;
import com.magnumresearch.aqumon.common.feign.QuoteEngineFeignClient;
import com.magnumresearch.aqumon.riskengine.adapter.kuanrui.constants.KuanruiConstants;
import com.magnumresearch.aqumon.riskengine.adapter.kuanrui.pojo.MarketDataL2Quote;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.pojo.InstrumentDataMaster;
import com.magnumresearch.aqumon.trading.constants.CurrencyType;
import com.magnumresearch.aqumon.trading.constants.OrderStatusType;
import com.magnumresearch.aqumon.trading.constants.SecType;
import com.magnumresearch.aqumon.trading.model.Holding;
import com.magnumresearch.aqumon.trading.utils.InstrumentUtil;
import com.quant360.api.client.MdsClient;
import com.quant360.api.model.mds.*;
import com.quant360.api.model.mds.enu.MdsExchangeId;
import com.quant360.api.model.mds.enu.MdsSecurityType;
import com.quant360.api.model.oes.enu.OesMarketId;
import com.quant360.api.model.oes.enu.OesOrdStatus;
import com.quant360.api.model.oes.enu.OesSecurityType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static com.quant360.api.model.mds.enu.MdsL2OrderSide.MDS_L2_ORDER_SIDE_BUY;
import static com.quant360.api.model.mds.enu.MdsL2OrderSide.MDS_L2_ORDER_SIDE_SELL;

@Slf4j
@Component
@ConditionalOnProperty(prefix = "adapter.kuanrui", value = {"enabled"})
public class KuanruiUtils {

    public static Map<String, String[]> accountMap = new HashMap<String, String[]>();

    public static Map<String, BigDecimal> priceMap = new HashMap<String, BigDecimal>();

    public static Map<String, List<Holding>> HoldingsListSH = new HashMap<String, List<Holding>>();

    public static Map<String, List<Holding>> HoldingsListSZ = new HashMap<String, List<Holding>>();

    @Autowired
    MdsClient mdsClient;

    @Autowired
    KuanruiUtils kuanruiUtils;

    @Autowired
    QuoteEngineFeignClient quoteEngineFeignClient;

    @Autowired
    InstrumentUtil instrumentUtil;


    public static long startTime;

    public static Map<String, String> username_pwd_map = new HashMap<>();

    static
    {
        username_pwd_map.put("20050178", "20050178");
        username_pwd_map.put("customer385", "ej923y");
        username_pwd_map.put("customer482", "vgfno0");
        username_pwd_map.put("customer489", "ulwe33");
        username_pwd_map.put("customer491", "4kv60t");
    }

    static
    {
        accountMap.put("1888000385", new String[]{"0188800385","A188800385"});
        accountMap.put("1888000482",  new String[]{"0188800482","A188800482"});
        accountMap.put("1888000489",  new String[]{"0188800489","A188800489"});
        accountMap.put("1888000491",  new String[]{"0188800491","A188800491"});
        accountMap.put("20050178",  new String[]{"0157563099","A454543295"});
    }

    public SecType getSecTypeByOESSecurityType(OesSecurityType oesSecurityType) throws TradingException {
        switch(oesSecurityType){
            case OES_SECURITY_TYPE_OPTION:
                return SecType.OPT;
            case OES_SECURITY_TYPE_ETF:
                return SecType.ETF;
            case OES_SECURITY_TYPE_STOCK:
                return SecType.STK;
            default:
                //如果不匹配，直接报错
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_SECTYPE_OESSECURITYTYPE_MISMATCH_ERROR, "Can not find the corresponding OesSecurityType of SecType");
        }
    }

    public MdsSecurityType getMdsSecurityTypeBySecType(SecType secType) throws TradingException {
        switch(secType){
            case OPT:
                return MdsSecurityType.MDS_SECURITY_TYPE_OPTION;
            case ETF:
                return MdsSecurityType.MDS_SECURITY_TYPE_STOCK;
            case STK:
                return MdsSecurityType.MDS_SECURITY_TYPE_STOCK;
            default:
                //如果不匹配，直接报错
                throw new TradingException(ResultStatusConstants.TRADING_ENGINE_SECTYPE_MDSSECURITYTYPE_MISMATCH_ERROR, "Can not find the corresponding MdsSecurityType of SecType");
        }
    }

    public static String getPwd(String username)
    {
        if(username_pwd_map.containsKey(username)) {
            String pwd = username_pwd_map.get(username);
            return pwd;
        }else {
            return null;
        }
    }

    //Check if the account is a Kuanrui Account
    public static boolean isKuanRui(String accountId)
    {
        String kuanruiAccounts = "1888000385,0188800385,A188800385,20050178,0157563099,A454543295,1888000482,0188800482,A188800482,1888000489,0188800489,A188800489,1888000491,0188800491,A188800491";
        return kuanruiAccounts.contains(accountId);
    }


    public static void setStartTime()
    {
        startTime = new Date().getTime();
    }

    public static long getTimeInterval()
    {
        long currentTime = new Date().getTime();
        long interval = currentTime - startTime;
        return interval;
    }

    public Map<String, BigDecimal> getPrice(List<String> instrumentIds) throws CommonException {
        Map<String, BigDecimal> price_map = quoteEngineFeignClient.getKRSnapshot(instrumentIds).getData();
        List<String> returned_instrumentIds = new ArrayList<>(price_map.keySet());
        List<String> missing_instrumentIds = new LinkedList<>();
        for(String instrumentId: instrumentIds){
            if(!returned_instrumentIds.contains(instrumentId)){
                missing_instrumentIds.add(instrumentId);
            }
        }
        if(missing_instrumentIds.size() > 0){
            throw new CommonException(ResultStatusConstants.QUOTE_ENGINE_GET_SNAPSHOT_ERROR, "Missing KRSnapshot info of instrumentIds " + missing_instrumentIds);
        }else {
            return price_map;
        }
    }

    private long qryMktDataHandle(MdsMktDataSnapshotBase data){
        if(data == null){
            System.out.println("查询数据为空，数据不存在!");
            return 0;
        }
        return qryStockHandle(data.getHead(), data.getStock());
    }

    /**
     * Level1 市场行情查询回调 (股票、债券、基金行情数据)
     * @param head 行情全幅消息的消息头
     * @param data 股票(A、B股)、债券 、基金的行情全幅消息
     */
    /**
     * Level1 市场行情查询回调 (股票、债券、基金行情数据)
     * @param head 行情全幅消息的消息头
     * @param data 股票(A、B股)、债券 、基金的行情全幅消息
     */
    private long qryStockHandle(MdsMktDataSnapshotHead head, MdsStockSnapshotBody data){
        return data.getTradePx();
    }

    public static String[] getMktBrokerAccountMap(String accountId)
    {
        if(accountMap.containsKey(accountId)) {
            return accountMap.get(accountId);
        }
        else
        {
            return new String[]{accountId};
        }
    }

    public static String getMktBrokerAccount(String brokerAccount,InstrumentDataMaster instrumentDataMaster){
        String[] brokerAccounts = KuanruiUtils.getMktBrokerAccountMap(brokerAccount);
        if (instrumentDataMaster.getExchange().equals("SEHKSZSE")||instrumentDataMaster.getExchange().equals(("CHINEXT"))) {
            return brokerAccounts[0];
        } else {
            //SEHKNTL
            return brokerAccounts[1];
        }
    }

    public static void putpricemap(MarketDataL2Quote quote)
    {
        priceMap.put(quote.getInstrumentId(), quote.getLast());
    }

    public static BigDecimal getpricemap(String iuid)
    {
        if(priceMap.containsKey(iuid))
        {
            return priceMap.get(iuid);
        }
        else
        {
            return BigDecimal.ZERO;
        }
    }

    public BigDecimal countAssetValue(String brokerAccount) throws CommonException, InterruptedException {

        BigDecimal assets = new BigDecimal(0);
        List<Holding> listSH = HoldingsListSH.get(brokerAccount);
        List<Holding> listSZ = HoldingsListSZ.get(brokerAccount);
        for(Holding h: listSH)
        {
            //BigDecimal price = BigDecimal.valueOf(kuanruiUtils.getPrice(h.getInstrumentId()), KuanruiConstants.PRICE_PRECISION);
            BigDecimal price = kuanruiUtils.getPrice(Collections.singletonList(h.getInstrumentId())).get(h.getInstrumentId());
            BigDecimal quantity = h.getHoldingPosition().abs();
            BigDecimal localValue = price.multiply(quantity);
            assets = assets.add(localValue);
        }

        for(Holding h: listSZ)
        {
            //BigDecimal price = BigDecimal.valueOf(kuanruiUtils.getPrice(h.getInstrumentId()), KuanruiConstants.PRICE_PRECISION);
            BigDecimal price = kuanruiUtils.getPrice(Collections.singletonList(h.getInstrumentId())).get(h.getInstrumentId());
            BigDecimal quantity = h.getHoldingPosition().abs();
            BigDecimal localValue = price.multiply(quantity);
            assets = assets.add(localValue);
        }

        return assets;
    }

    public static void serTwoLists(List<Holding> _ListSZ, List<Holding> _ListSH, String brokerAccount)
    {
        HoldingsListSZ.put(brokerAccount, _ListSZ);
        HoldingsListSH.put(brokerAccount, _ListSH);
    }

    public static OesMarketId retrieveMarketId(String primaryExch) {
        //TODO: Change the input parameter usage
        if (primaryExch.equals("SEHKSZSE") || primaryExch.equals(("CHINEXT"))) {
            return OesMarketId.OES_MKT_ID_SZ_A;
        } else return OesMarketId.OES_MKT_ID_SH_A;
    }

    public static OrderStatusType convertOrderStatus(OesOrdStatus oesOrdStatus) {
        switch (oesOrdStatus) {
            case OES_ORD_STATUS_NEW:
                return OrderStatusType.PENDING;
            case OES_ORD_STATUS_DECLARED:
            case OES_ORD_STATUS_PARTIALLY_FILLED:
                return OrderStatusType.FILLING;
            case OES_ORD_STATUS_FILLED:
                return OrderStatusType.FILLED;
            case OES_ORD_STATUS_CANCEL_DONE:
            case OES_ORD_STATUS_PARTIALLY_CANCELED:
            case OES_ORD_STATUS_CANCELED:
                return OrderStatusType.CANCELLED;
            case __OES_ORD_STATUS_INVALID_MIN:
            case OES_ORD_STATUS_INVALID_OES:
            case OES_ORD_STATUS_INVALID_SH_E:
            case OES_ORD_STATUS_INVALID_SH_F:
            case OES_ORD_STATUS_INVALID_SZ_E:
            case OES_ORD_STATUS_INVALID_SZ_F:
            case OES_ORD_STATUS_INVALID_SH_COMM:
            case OES_ORD_STATUS_INVALID_SZ_REJECT:
            case OES_ORD_STATUS_INVALID_SZ_TRY_AGAIN:
                return OrderStatusType.REJECTED;
            case __OES_ORD_STATUS_FINAL_MIN:
            case OES_ORD_STATUS_UNDEFINE:
            default:
                return OrderStatusType.UNKNOWN;
        }
    }

    public static int iuidToInstrumentId(String iuid) {
        iuid = iuid.substring(6);
        int instrumentId = Integer.parseInt(iuid);
        return instrumentId;
    }

    public String getInstrumentIdBySecurityId(String input_exchange, Object securityType, String securityID) throws CommonException {
        String instrumentId = null;
        String [] exchangeArray = {input_exchange, "CHINEXT", ""};
        GetInstrumentId:{
            for (String exchange : exchangeArray) {
                if (securityType == MdsSecurityType.MDS_SECURITY_TYPE_OPTION || securityType == OesSecurityType.OES_SECURITY_TYPE_OPTION) {
                    SecType secType = SecType.OPT;
                    instrumentId = instrumentUtil.getInstrumentFromDM(
                            securityID, exchange, CurrencyType.CNY.getCurrency(), secType.toString(), null
                    ).getInstrumentId();
                } else if (securityType == MdsSecurityType.MDS_SECURITY_TYPE_STOCK || securityType == OesSecurityType.OES_SECURITY_TYPE_STOCK
                        || securityType == OesSecurityType.OES_SECURITY_TYPE_ETF ) {
                    SecType secTypeArray[] = {SecType.STK, SecType.ETF};
                    //依次测试SecType.STK和SecType.ETF，得出一个正确的
                    for (SecType secType : secTypeArray) {
                        try {
                            instrumentId = instrumentUtil.getInstrumentFromDM(
                                    securityID, exchange, CurrencyType.CNY.getCurrency(), secType.toString(), null
                            ).getInstrumentId();
                        } catch (Exception e) {
                            continue;
                        }
                        break GetInstrumentId;
                    }
                }else{
                    throw new CommonException(ResultStatusConstants.TRADING_ENGINE_INSTRUMENTID_NOTFOUND_ERROR, "Security Type " + securityType + " not supported!");
                }
            }
        }
        if(instrumentId == null)
        {
            //如果找不到，直接报错
            throw new CommonException(ResultStatusConstants.TRADING_ENGINE_INSTRUMENTID_NOTFOUND_ERROR, "Can not find the corresponding InstrumentId by MdsSecurityType and SecurityId with "
                    + "input_exchange: " + input_exchange + ", securityType: " + securityType + ", securityID: " + securityID);
        }
        return instrumentId;
    }

//    public String getInstrumentIdBySecurityId(String input_exchange, OesSecurityType oesSecurityType, String securityID) throws CommonException {
//        String instrumentId = null;
//        String [] exchangeArray = {input_exchange, "CHINEXT", ""};
//        GetInstrumentId:{
//            for (String exchange : exchangeArray) {
//                if (oesSecurityType == OesSecurityType.OES_SECURITY_TYPE_OPTION) {
//                    SecType secType = SecType.OPT;
//                    instrumentId = instrumentUtil.getInstrumentIdFromDM(securityID, exchange, CurrencyType.CNY.getCurrency(), secType.toString());
//                } else if (oesSecurityType == OesSecurityType.OES_SECURITY_TYPE_STOCK) {
//                    SecType secTypeArray[] = {SecType.STK, SecType.ETF};
//                    //依次测试SecType.STK和SecType.ETF，得出一个正确的
//                    for (SecType secType : secTypeArray) {
//                        try {
//                            instrumentId = instrumentUtil.getInstrumentIdFromDM(securityID, exchange, CurrencyType.CNY.getCurrency(), secType.toString());
//                        } catch (Exception e) {
//                            log.error("!!!!!!abcdef" + securityID + "," + exchange + "," + "CNY" +"," + secType.toString());
//                            continue;
//                        }
//                        break GetInstrumentId;
//                    }
//                }
//            }
//        }
//        if(instrumentId == null)
//        {
//            //如果找不到，直接报错
//            throw new CommonException(ResultStatusConstants.TRADING_ENGINE_INSTRUMENTID_NOTFOUND_ERROR, "Can not find the corresponding InstrumentId by MdsSecurityType and SecurityId");
//        }
//        return instrumentId;
//    }

    public static long convertTimestamp(String tradeDate, String transactTime){
        long ts = 0;
        String timeStr = tradeDate + " " + transactTime;
        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd hhmmssSSS");
            Date parsedDate = dateFormat.parse(timeStr);
            //Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
            ts = parsedDate.getTime();
        } catch(Exception e) { //this generic but you can control another types of exception
            e.printStackTrace();
        }
        return ts;
    }

    public static long systemTimestamp(){
        Date date = new Date();
        long time = date.getTime();
        return time;
    }

    //TODO: KuanruiUtils.getInstrumentId should be changed
    public MarketDataL2Quote buildMarketDataL2QuoteFromMdsStockSnapshotBody(MdsMktDataSnapshotHead mdsMktDataSnapshotHead, MdsStockSnapshotBody data) throws CommonException {
        MarketDataL2Quote result = new MarketDataL2Quote();
        String securityId = data.getSecurityID();
        result.setSysTimeStamp(systemTimestamp());
        result.setMdsSecurityType(mdsMktDataSnapshotHead.getSecurityType());
        String exchange;
        if (mdsMktDataSnapshotHead.getExchId() == MdsExchangeId.MDS_EXCH_SZSE) {
            String instrId = mdsMktDataSnapshotHead.getInstrId()+"";
            if(instrId.startsWith("3")){
                exchange = "CHINEXT";
            }else{
                exchange = "SEHKSZSE";
            }
        } else {
            exchange = "SEHKNTL";
        }
        result.setInstrumentId(kuanruiUtils.getInstrumentIdBySecurityId(exchange, result.getMdsSecurityType(), securityId));
        result.setLast(BigDecimal.valueOf(data.getTradePx(), KuanruiConstants.PRICE_PRECISION));
        result.setVol(BigDecimal.valueOf(data.getTotalVolumeTraded()).multiply(BigDecimal.valueOf(100)));
        result.setOpen(BigDecimal.valueOf(data.getOpenPx(), KuanruiConstants.PRICE_PRECISION));
        result.setHigh(BigDecimal.valueOf(data.getHighPx(), KuanruiConstants.PRICE_PRECISION));
        result.setLow(BigDecimal.valueOf(data.getLowPx(), KuanruiConstants.PRICE_PRECISION));
        result.setClose(BigDecimal.valueOf(data.getPrevClosePx(), KuanruiConstants.PRICE_PRECISION));
        result.setA1(BigDecimal.valueOf(data.getOfferLevels().get(0).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setA2(BigDecimal.valueOf(data.getOfferLevels().get(1).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setA3(BigDecimal.valueOf(data.getOfferLevels().get(2).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setA4(BigDecimal.valueOf(data.getOfferLevels().get(3).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setA5(BigDecimal.valueOf(data.getOfferLevels().get(4).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setB1(BigDecimal.valueOf(data.getBidLevels().get(0).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setB2(BigDecimal.valueOf(data.getBidLevels().get(1).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setB3(BigDecimal.valueOf(data.getBidLevels().get(2).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setB4(BigDecimal.valueOf(data.getBidLevels().get(3).getPrice(), KuanruiConstants.PRICE_PRECISION));
        result.setB5(BigDecimal.valueOf(data.getBidLevels().get(4).getPrice(), KuanruiConstants.PRICE_PRECISION));

        result.setAv1(BigDecimal.valueOf(data.getOfferLevels().get(0).getOrderQty()));
        result.setAv2(BigDecimal.valueOf(data.getOfferLevels().get(1).getOrderQty()));
        result.setAv3(BigDecimal.valueOf(data.getOfferLevels().get(2).getOrderQty()));
        result.setAv4(BigDecimal.valueOf(data.getOfferLevels().get(3).getOrderQty()));
        result.setAv5(BigDecimal.valueOf(data.getOfferLevels().get(4).getOrderQty()));
        result.setBv1(BigDecimal.valueOf(data.getBidLevels().get(0).getOrderQty()));
        result.setBv2(BigDecimal.valueOf(data.getBidLevels().get(1).getOrderQty()));
        result.setBv3(BigDecimal.valueOf(data.getBidLevels().get(2).getOrderQty()));
        result.setBv4(BigDecimal.valueOf(data.getBidLevels().get(3).getOrderQty()));
        result.setBv5(BigDecimal.valueOf(data.getBidLevels().get(4).getOrderQty()));

        result.setMsgType("snapshot");

        return result;
    }

    //TODO: KuanruiUtils.getInstrumentId should be changed
    public MarketDataL2Quote buildMarketDataL2QuoteFromMdsStockL2TradeBody(MdsL2Trade data) throws CommonException {
        MarketDataL2Quote result = new MarketDataL2Quote();
        String tradeDate = String.valueOf(data.getTradeDate());
        String transactTime = String.valueOf(data.getTransactTime());
        long timestamp = convertTimestamp(tradeDate, transactTime);
        String securityID = data.getSecurityID();
        result.setTimeStamp(timestamp);
        result.setSysTimeStamp(systemTimestamp());
        result.setMdsSecurityType(data.getSecurityType());
        String exchange;
        if (data.getExchId() == MdsExchangeId.MDS_EXCH_SZSE) {
            exchange = "SEHKSZSE";
        } else {
            exchange = "SEHKNTL";
        }
        result.setInstrumentId(kuanruiUtils.getInstrumentIdBySecurityId(exchange, result.getMdsSecurityType(), securityID));
//        result.setInstrumentId(kuanruiUtils.getInstrumentIdBySecurityId(securityID));
        result.setLast(BigDecimal.valueOf(data.getTradePrice(), KuanruiConstants.PRICE_PRECISION));
        //todo(joseph): volume is not trade value
        result.setVol(BigDecimal.valueOf(data.getTradeMoney(), KuanruiConstants.PRICE_PRECISION));
        return result;
    }

    //TODO: KuanruiUtils.getInstrumentId should be changed
    public MarketDataL2Quote buildMarketDataL2QuoteFromMdsStockL2OrderBody(MdsL2Order data) throws CommonException {
        MarketDataL2Quote result = new MarketDataL2Quote();
        String tradeDate = String.valueOf(data.getTradeDate());
        String transactTime = String.valueOf(data.getTransactTime());
        long timestamp = convertTimestamp(tradeDate, transactTime);
        String securityID = data.getSecurityID();
        result.setTimeStamp(timestamp);
        result.setSysTimeStamp(systemTimestamp());
        result.setMdsSecurityType(data.getSecurityType());
        String exchange;
        if (data.getExchId() == MdsExchangeId.MDS_EXCH_SZSE) {
            exchange = "SEHKSZSE";
        } else {
            exchange = "SEHKNTL";
        }
        result.setInstrumentId(kuanruiUtils.getInstrumentIdBySecurityId(exchange, result.getMdsSecurityType(), securityID));
//        result.setInstrumentId(kuanruiUtils.getInstrumentIdBySecurityId(securityID));

        if (data.getSide()==MDS_L2_ORDER_SIDE_SELL){
            result.setA1(BigDecimal.valueOf(data.getPrice(), KuanruiConstants.PRICE_PRECISION));
            result.setAv1(BigDecimal.valueOf(data.getOrderQty()));
        } else if (data.getSide()==MDS_L2_ORDER_SIDE_BUY){
            result.setB1(BigDecimal.valueOf(data.getPrice(), KuanruiConstants.PRICE_PRECISION));
            result.setBv1(BigDecimal.valueOf(data.getOrderQty()));
        }
        return result;
    }

    public String getKRCashAccount(String brokerAccount)
    {
        if(brokerAccount.equals("0188800385") || brokerAccount.equals("A188800385")) {
            return "1888000385";
        } else if(brokerAccount.equals("0188800482") || brokerAccount.equals("A188800482")) {
            return "1888000482";
        } else if(brokerAccount.equals("0188800489") || brokerAccount.equals("A188800489")) {
            return "1888000489";
        } else if(brokerAccount.equals("0188800491") || brokerAccount.equals("A188800491")) {
            return "1888000491";
        } else if(brokerAccount.equals("0157563099") || brokerAccount.equals("A454543295")) {
            return "20050178";
        }else {
            return brokerAccount;
        }

    }
}
