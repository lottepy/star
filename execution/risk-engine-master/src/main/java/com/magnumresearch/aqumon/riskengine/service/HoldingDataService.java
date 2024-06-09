package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.common.feign.LarkRobotClient;
import com.magnumresearch.aqumon.riskengine.adapter.BaseBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.DataSourceType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.dao.ExecutionDao;
import com.magnumresearch.aqumon.riskengine.dao.HoldingDao;
import com.magnumresearch.aqumon.riskengine.dao.HoldingHistoryDao;
import com.magnumresearch.aqumon.riskengine.dao.SubAccountDao;
import com.magnumresearch.aqumon.riskengine.pojo.ComparisonResult;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationData;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationKey;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.constants.OrderDirectionType;
import com.magnumresearch.aqumon.trading.constants.SubAccountType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.Execution;
import com.magnumresearch.aqumon.trading.model.Holding;
import com.magnumresearch.aqumon.trading.model.HoldingHistory;
import com.magnumresearch.aqumon.trading.model.SubAccount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class HoldingDataService {

    @Autowired
    ExecutionDao executionDao;
    @Autowired
    HoldingDao holdingDao;
    @Autowired
    SubAccountDao subAccountDao;
    @Autowired
    HoldingHistoryDao holdingHistoryDao;
    @Autowired
    BrokerAccountService brokerAccountService;
    @Autowired
    BrokerAdapterService brokerAdapterService;
    @Autowired
    HoldingValidationService holdingValidationService;
    @Autowired
    BaseConfigService baseConfigService;
    @Autowired
    LarkRobotClient larkRobotClient;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Get historical holdings from DB
     * @param brokerType broker type
     * @param date date of historical holdings
     * @param brokerAccounts broker accounts
     * @param symbols symbols
     * @return a list of holding history
     */
    public List<HoldingHistory> getHistoricalHoldingsFromDB(BrokerType brokerType, Date date, Set<String> brokerAccounts, Set<String> symbols) {
        List<HoldingHistory> holdings = new LinkedList<>();
        for (String brokerAccount: brokerAccounts)
            if (Objects.isNull(symbols) || symbols.size() <= 0)
                holdings.addAll(getHoldingHistoryDaos(brokerType, date, brokerAccount, null));
            else
                for (String symbol: symbols)
                    holdings.addAll(getHoldingHistoryDaos(brokerType, date, brokerAccount, symbol));
        return holdings;
    }

    /**
     * Get current holdings from DB
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param symbols symbol
     * @return a list of holding
     */
    public List<Holding> getCurrentHoldingsFromDB(BrokerType brokerType, Set<String> brokerAccounts, Set<String> symbols) {
        List<Holding> holdings = new LinkedList<>();
        for (String brokerAccount: brokerAccounts)
            if (Objects.isNull(symbols) || symbols.size() <= 0)
                holdings.addAll(getHoldingDaos(brokerType, brokerAccount, null));
            else
                for (String symbol: symbols)
                    holdings.addAll(getHoldingDaos(brokerType, brokerAccount, symbol));
        return holdings;
    }

    /**
     * Get current holdings from DB
     * @param brokerType broker type
     * @param brokerAccount broker account
     * @param symbol symbol
     * @return a list of holding current
     */
    public List<Holding> getHoldingDaos(BrokerType brokerType, String brokerAccount, String symbol) {
        if (Objects.nonNull(brokerAccount) && Objects.nonNull(symbol))
            return holdingDao.findByBrokerTypeAndBrokerAccountAndInstrumentId(brokerType, brokerAccount, symbol);
        else if (Objects.nonNull(brokerAccount))
            return holdingDao.findByBrokerTypeAndBrokerAccount(brokerType, brokerAccount);
        else if (Objects.nonNull(symbol))
            return holdingDao.findByBrokerTypeAndInstrumentId(brokerType, symbol);
        else
            return holdingDao.findByBrokerType(brokerType);
    }

    /**
     * Get historical holdings from DB
     * @param brokerType broker type
     * @param date date of history holdings
     * @param brokerAccount broker account
     * @param symbol symbol
     * @return a list of holding history
     */
    public List<HoldingHistory> getHoldingHistoryDaos(BrokerType brokerType, Date date, String brokerAccount, String symbol) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = dateFormat.format(date);
        if (Objects.nonNull(brokerAccount) && Objects.nonNull(symbol))
            return holdingHistoryDao.findByDateAndBrokerTypeAndBrokerAccountAndInstrumentId(dateStr, brokerType, brokerAccount, symbol);
        else if (Objects.nonNull(brokerAccount))
            return holdingHistoryDao.findByDateAndBrokerTypeAndBrokerAccount(dateStr, brokerType, brokerAccount);
        else if (Objects.nonNull(symbol))
            return holdingHistoryDao.findByDateAndBrokerTypeAndInstrumentId(dateStr, brokerType, symbol);
        else
            return holdingHistoryDao.findByDateAndBrokerType(dateStr, brokerType);
    }

    /**
     * Get holdings from broker
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param symbols symbols
     * @return a list of holding objects
     */
    public List<Holding> getHoldingsFromBroker(BrokerType brokerType, Set<String> brokerAccounts, Set<String> symbols) {
        List<Holding> holdingResults = new LinkedList<>();
        BaseBrokerAdapter adapter;

        try {
            adapter = brokerAdapterService.getAdapter(brokerType);
        } catch (TradingException tradingException) {
            log.error("[RiskEngine][Holding] adapter failed to retrieved: Exception=", tradingException);
            return null;
        }

        if (Objects.isNull(brokerAccounts))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);

        for (String account: brokerAccounts) {
            List<Holding> holdings;
            try {
                holdings = adapter.queryHolding(account);
                holdings.removeIf(holding -> Objects.isNull(holding.getInstrumentId()));
                if (Objects.nonNull(symbols))
                    holdings.removeIf(holding -> !symbols.contains(holding.getInstrumentId()));
                holdingResults.addAll(holdings);
            }
            catch (TradingException tradingException) {
                log.error("[RiskEngine][Holding] holdings failed to retrieved: Exception=", tradingException);
            } 
        }
        return holdingResults;
    }

    /**
     * calculate today's trading volumes based on execution records in DB
     * @param brokerType broker type
     * @param brokerAccount broker account
     * @param symbols symbols
     * @return an array of long and short volume for today
     */
    public BigDecimal[] calculateTodayVolumeFromExecs(BrokerType brokerType, String brokerAccount, String symbols, BigDecimal holdingPosition) {
        BigDecimal longVolumeToday = BigDecimal.ZERO;
        BigDecimal shortVolumeToday = BigDecimal.ZERO;
        List<Execution> executionList = executionDao.findByBrokerTypeAndBrokerAccountAndInstrumentId(brokerType, brokerAccount, symbols);
        for (Execution execution : executionList) {
            if (execution.getDirection() == OrderDirectionType.BUY) {
                longVolumeToday = longVolumeToday.add(execution.getQuantityFilled());
            } else if (execution.getDirection() == OrderDirectionType.SELL) {
                shortVolumeToday = shortVolumeToday.add(execution.getQuantityFilled());
            }
        }
        if (executionList.size() <= 0)
            longVolumeToday = holdingPosition;
        return new BigDecimal[] {longVolumeToday, shortVolumeToday};
    }

    /**
     * query historical trading volumes based on holding historical holding records in DB
     * @param brokerType broker type
     * @param date date of historical holding record
     * @param brokerAccount broker accounts
     * @param symbol symbol
     * @return an array of historical long, short, and frozen long volume
     */
    public BigDecimal[] queryHistoricalVolume(BrokerType brokerType, Date date, String brokerAccount, String symbol) {
        BigDecimal longVolumeHistory = BigDecimal.ZERO;
        BigDecimal shortVolumeHistory = BigDecimal.ZERO;
        BigDecimal frozenLongVolumeHistory = BigDecimal.ZERO;
        List<HoldingHistory> holdings = getHoldingHistoryDaos(brokerType, date, brokerAccount, symbol);
        for (HoldingHistory holding: holdings) {
            longVolumeHistory = longVolumeHistory.add(holding.getLongVolumeToday()).add(holding.getLongVolumeHistory());
            shortVolumeHistory = shortVolumeHistory.add(holding.getShortVolumeToday()).add(holding.getShortVolumeHistory());
            frozenLongVolumeHistory = frozenLongVolumeHistory.add(holding.getFreezingLongVolumeHistory());
        }
        return new BigDecimal[] {longVolumeHistory, shortVolumeHistory, frozenLongVolumeHistory};
    }

    /**
     * Update holding with volume information
     * @param source source holding
     * @param position target position
     * @param price target price
     * @param amount target amount
     * @param volumeToday target longVolumeToday and shortVolumeToday
     * @param volumeHistory target longVolumeHistory, shortVolumeHistory, and frozenLongVolumeHistory
     * @return updated holding object
     */
    public Holding updateHoldingsWithVolume(Holding source, BigDecimal position, BigDecimal price, BigDecimal amount, BigDecimal[] volumeToday, BigDecimal[] volumeHistory) {
        source.setHoldingPosition(position);
        source.setHoldingPrice(price);
        source.setHoldingAmount(amount);
        source.setLongVolumeToday(volumeToday[0]);
        source.setShortVolumeToday(volumeToday[1]);
        source.setLongVolumeHistory(volumeHistory[0]);
        source.setShortVolumeHistory(volumeHistory[1]);
        source.setFrozenLongVolumeHistory(volumeHistory[2]);
        return source;
    }

    /**
     * Convert a list of holding to a map of holding Key to holding object
     * @param currentHoldings a list of holdings
     * @return a map of holding Key to holding object
     */
    public Map<String, Holding> convertHoldingListToMap(List<Holding> currentHoldings) {
        Map<String, Holding> currentHoldingsMap = new HashMap<>();
        for (Holding holding: currentHoldings) {
            ValidationKey validationKey = new ValidationKey(RiskGroupType.HOLDING, holding.getBrokerType(), holding.getBrokerAccount(), null, holding.getInstrumentId());
            String key = validationKey.toString();
            if (currentHoldingsMap.containsKey(key)) {
                combineHoldings(currentHoldingsMap.get(key), holding);
            } else {
                currentHoldingsMap.put(key, holding);
            }
        }
        return currentHoldingsMap;
    }

    /**
     * Convert a list of holdingHistory to a map of holding Key to holdingHistory object
     * @param historicalHoldings a list of holdingHistory
     * @return a map of holding Key to holdingHistory object
     */
    public Map<String, Holding> convertHoldingHistoryListToMap(List<HoldingHistory> historicalHoldings) {
        Map<String, Holding> historicalHoldingsMap = new HashMap<>();
        for (HoldingHistory holdingHistory: historicalHoldings) {
            ValidationKey validationKey = new ValidationKey(RiskGroupType.HOLDING, holdingHistory.getBrokerType(), holdingHistory.getBrokerAccount(), null, holdingHistory.getInstrumentId());
            String key = validationKey.toString();
            Holding holding = new Holding(holdingHistory);
            if (historicalHoldingsMap.containsKey(key)) {
                combineHoldings(historicalHoldingsMap.get(key), holding);
            } else {
                historicalHoldingsMap.put(key, holding);
            }
        }
        return historicalHoldingsMap;
    }

    /**
     * combine two holding and recalculate the numeric values with holding object
     * @param source source holding object
     * @param target target holding object
     */
    public void combineHoldings(Holding source, Holding target) {
        if (Objects.nonNull(source.getHoldingPosition()) && Objects.nonNull(target.getHoldingPosition()))
            source.setHoldingPosition(source.getHoldingPosition().add(target.getHoldingPosition()));
        if (Objects.nonNull(source.getHoldingAmount()) && Objects.nonNull(target.getHoldingAmount()))
            source.setHoldingAmount(source.getHoldingAmount().add(target.getHoldingAmount()));
        if (Objects.nonNull(source.getHoldingPrice()) && Objects.nonNull(target.getHoldingPrice()))
            source.setHoldingPrice(source.getHoldingAmount().add(target.getHoldingAmount()).divide(source.getHoldingPosition().add(target.getHoldingPosition()), RoundingMode.HALF_UP));
        if (Objects.nonNull(source.getLongVolumeHistory()) && Objects.nonNull(target.getLongVolumeHistory()))
            source.setLongVolumeHistory(source.getLongVolumeHistory().add(target.getLongVolumeHistory()));
        if (Objects.nonNull(source.getLongVolumeToday()) && Objects.nonNull(target.getLongVolumeToday()))
            source.setLongVolumeToday(source.getLongVolumeToday().add(target.getLongVolumeToday()));
        if (Objects.nonNull(source.getShortVolumeHistory()) && Objects.nonNull(target.getShortVolumeHistory()))
            source.setShortVolumeHistory(source.getShortVolumeHistory().add(target.getShortVolumeHistory()));
        if (Objects.nonNull(source.getShortVolumeToday()) && Objects.nonNull(target.getShortVolumeToday()))
            source.setShortVolumeToday(source.getShortVolumeToday().add(target.getShortVolumeToday()));
        if (Objects.nonNull(source.getFrozenLongVolumeHistory()) && Objects.nonNull(target.getFrozenLongVolumeHistory()))
            source.setFrozenLongVolumeHistory(source.getFrozenLongVolumeHistory().add(target.getFrozenLongVolumeHistory()));
    }

    /**
     * find duplicate holdings from current database
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param symbols symbols
     * @return a map between key and number of duplications
     */
    public Map<String, Integer> findDuplicatedHoldings(BrokerType brokerType, Set<String> brokerAccounts, Set<String> symbols) {
        List<Holding> holdings = getCurrentHoldingsFromDB(brokerType, brokerAccounts, symbols);
        Map<String, Integer> holdingMap = new HashMap<>();
        for (Holding holding: holdings) {
            String key = holding.getBrokerType() + "_" + holding.getBrokerAccount() + "_" + holding.getSub_account_id() + "_" + holding.getInstrumentId();
            if (holdingMap.containsKey(key))
                holdingMap.put(key, holdingMap.get(key) + 1);
            else
                holdingMap.put(key, 1);
        }
        holdingMap.entrySet().removeIf(entry -> entry.getValue() <= 1);
        return holdingMap;
    }

    /**
     * Synchronize holding in datqbase based on Broker holdings.
     * @param brokerType Broker Type
     * @param brokerAccounts Broker Accounts
     * @param symbols symbol
     * @param ruleName name of rule
     * @return list of holdings that being updated in database
     */
    @Transactional
    public synchronized List<Holding> syncHoldings(BrokerType brokerType, Set<String> brokerAccounts, Set<String> symbols, String ruleName) {
        if (!(brokerType == BrokerType.AYERS || brokerType == BrokerType.IB || brokerType == BrokerType.KR)) {
            log.error("[RiskEngine][Sync][Holding] sync Holding is not supported for BrokerType=" + brokerType.name());
            return new ArrayList<>();
        }

        log.info("[RiskEngine][Sync][Holding] sync Holding from from Broker to current holding table starts: BrokerType=" + brokerType.name() + ", Accounts=" + brokerAccounts + ", Symbols=" + symbols + ", RuleName=" + ruleName);
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.HOLDING);
        if (Objects.isNull(brokerAccounts) || brokerAccounts.contains("All") || brokerAccounts.contains("all") || brokerAccounts.contains("ALL"))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);

        Set<String> validAccounts = brokerAccountService.queryCheckableBrokerAccounts(brokerType, brokerAccounts, true).get(BrokerAccountService.CHECKABLE);
        if (validAccounts.size() == 0)
            return null;

        List<Holding> currentHoldings = getCurrentHoldingsFromDB(brokerType, validAccounts, symbols);
        List<Holding> brokerHoldings = getHoldingsFromBroker(brokerType, validAccounts, symbols);
        Map<String, Holding> sourceHoldings = convertHoldingListToMap(currentHoldings);
        Map<String, Holding> targetHoldings = convertHoldingListToMap(brokerHoldings);
        Map<String, Object> sources = new HashMap<>(sourceHoldings);
        Map<String, Object> targets = new HashMap<>(targetHoldings);
        List<ValidationData> diffHoldings = holdingValidationService.compareObjectMap(sources, targets, ruleSet, RiskGroupType.HOLDING, DataSourceType.DATABASE, DataSourceType.BROKER, null, true);

        List<Holding> newHoldings = new ArrayList<>();
        List<Holding> updateHoldings = new ArrayList<>();
        List<Holding> removeHoldings = new ArrayList<>();
        for (ValidationData validationData: diffHoldings) {
            for (Map.Entry<String, ComparisonResult> entry: validationData.getFieldData().entrySet()) {
                Holding currentHolding = (Holding) entry.getValue().getSource();
                Holding brokerHolding = (Holding) entry.getValue().getTarget();

                if (Objects.isNull(brokerHolding) && Objects.nonNull(currentHolding)) {
                    removeHoldings.add(currentHolding);
                    log.warn("[RiskEngine][Sync][Holding] holding to be removed: brokerType=" + brokerType + ", brokerAccount=" + currentHolding.getBrokerAccount() + ", instrumentId=" + currentHolding.getInstrumentId(), ", holdingPosition=" + currentHolding.getHoldingPosition());
                }
                else {
                    Date date = new Date();
                    BigDecimal[] volumeToday = calculateTodayVolumeFromExecs(brokerHolding.getBrokerType(), brokerHolding.getBrokerAccount(), brokerHolding.getInstrumentId(), brokerHolding.getHoldingPosition());
                    BigDecimal[] volumeHistory = queryHistoricalVolume(brokerHolding.getBrokerType(), date, brokerHolding.getBrokerAccount(), brokerHolding.getInstrumentId());
                    SubAccount subAccount = brokerAccountService.queryNormalSubAccount(brokerHolding.getBrokerType(), brokerHolding.getBrokerAccount());

                    /*
                        Temp solution for missing holdings in Database for Kuanrui
                        1. For comparison, aggregated values (by subaccountIds) are used;
                        2. If more than 1 subaccount exists per broker account, then
                            For removal, only remove one subaccount per sync run
                            For insert, the default account id -6 is used
                            For update, no action will be triggered as we can't differentiate by subaccount id.
                     */
                    if (Objects.isNull(subAccount) && brokerType == BrokerType.KR) {
                        subAccount = subAccountDao.findFirstByAccountIdAndSubAccountType((long)6, SubAccountType.NORMAL);
                    }

                    if (Objects.isNull(currentHolding)) {
                        currentHolding = new Holding(brokerHolding.getBrokerType(), brokerHolding.getBrokerAccount(), subAccount, brokerHolding.getInstrumentId());
                        currentHolding = updateHoldingsWithVolume(currentHolding, brokerHolding.getHoldingPosition(), brokerHolding.getHoldingPrice(), brokerHolding.getHoldingAmount(), volumeToday, volumeHistory);
                        newHoldings.add(currentHolding);
                    }
                    else {
                        currentHolding = updateHoldingsWithVolume(currentHolding, brokerHolding.getHoldingPosition(), brokerHolding.getHoldingPrice(), brokerHolding.getHoldingAmount(), volumeToday, volumeHistory);
                        updateHoldings.add(currentHolding);
                    }
                    log.warn("[RiskEngine][Sync][Holding] holding to be updated: brokerType=" + brokerType + ", brokerAccount=" + currentHolding.getBrokerAccount() + ", instrumentId=" + currentHolding.getInstrumentId(), ", holdingPosition=" + currentHolding.getHoldingPosition());
                }
            }
        }

        List<Holding> results = new ArrayList<>(holdingDao.saveAll(newHoldings));
        if (brokerType == BrokerType.AYERS || brokerType == BrokerType.IB)
            results.addAll(holdingDao.saveAll(updateHoldings));
        for (Holding removeHolding: removeHoldings)
            holdingDao.deleteById(removeHolding.getId());
        log.info("[RiskEngine][Sync][Holding] sync Holding completed: UpdatedRecordsSize=" + updateHoldings.size() + ", newRecordsSize=" + newHoldings.size() + ", RemovedRecordsSize=" + removeHoldings.size() + ", BrokerType=" + brokerType.name() + ", Accounts=" + validAccounts + ", Symbols=" + symbols + ", RuleName=" + ruleName);

        String newMessage = buildDingDingMessage(newHoldings, "Add");
        String updateMessage = buildDingDingMessage(updateHoldings, "Update");
        String removeMessage = buildDingDingMessage(removeHoldings, "Remove");
        larkRobotClient.sendMessage("[RiskEngine] Holding sync complete: broker type=" + brokerType + ", total updated records=" + (newHoldings.size() + updateHoldings.size() + removeHoldings.size()) + "\n" + newMessage + updateMessage + removeMessage);
        return results;
    }

    /**
     * combine a list of sync data into a single dingding message
     * @param holdings a list of updated holdings for dingding alert
     * @return a string of dingding message
     */
    public String buildDingDingMessage(List<Holding> holdings, String action) {
        StringBuilder message = new StringBuilder();
        int index = 0;
        if (holdings.size() > 0) {
            message.append(action).append(" records: size=").append(holdings.size()).append("\n");
            for (Holding holding : holdings) {
                if (index < 10) {
                    message.append("#").append(index + 1).append(": broker=").append(holding.getBrokerType()).append(", account=").append(holding.getBrokerAccount()).
                            append(", instrument=").append(holding.getInstrumentId()).append(", position=").append(holding.getHoldingPosition()).append("\n");
                    index = index + 1;
                }
                else
                    break;
            }
            message.append("...\n");
        }
        return message.toString();
    }
}
