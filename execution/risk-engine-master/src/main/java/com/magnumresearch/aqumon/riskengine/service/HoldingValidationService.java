package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.DataSourceType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationData;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.Holding;
import com.magnumresearch.aqumon.trading.model.HoldingHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class HoldingValidationService extends BaseValidationService{

    @Autowired
    BaseConfigService baseConfigService;
    @Autowired
    HoldingDataService holdingDataService;
    @Autowired
    BrokerAccountService brokerAccountService;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * Compare current holdings to historical holdings
     * @param brokerType Broker Type
     * @param date Date for historical holdings
     * @param brokerAccounts Broker Accounts
     * @param symbols symbols
     * @param ruleName names of validation rules
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @param useCache get result from cache if any
     * @return validation result that breach the validation rules
     */
    public ValidationResult compareCurrentToHistoricalHoldings(BrokerType brokerType, Date date, Set<String> brokerAccounts, Set<String> symbols, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][Holding] compare Holding from current holding table to historical holding table : [BrokerType] " + brokerType.name() + ", [Accounts] " + brokerAccounts + ", [Symbols] " + symbols + ", [RuleName] " + ruleName + ", [Date] " + date);
        String functionName = "compareCurrentToHistoricalHoldings";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.HOLDING);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, symbols, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        List<Holding> currentHoldings = holdingDataService.getCurrentHoldingsFromDB(brokerType, brokerAccounts, symbols);
        List<HoldingHistory> historicalHoldings = holdingDataService.getHistoricalHoldingsFromDB(brokerType, date, brokerAccounts, symbols);
        Map<String, Holding> sourceHoldings = holdingDataService.convertHoldingListToMap(currentHoldings);
        Map<String, Holding> targetHoldings = holdingDataService.convertHoldingHistoryListToMap(historicalHoldings);
        Map<String, Object> sources = new HashMap<>(sourceHoldings);
        Map<String, Object> targets = new HashMap<>(targetHoldings);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.HOLDING, DataSourceType.DATABASE, DataSourceType.HISTORYDATABASE, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * Compare historical holdings to historical holdings
     * @param brokerType Broker Type
     * @param date1 Date1 for historical holdings
     * @param date2 Date2 for historical holdings
     * @param brokerAccounts Broker Accounts
     * @param symbols symbols
     * @param ruleName name of validation rules
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @param useCache get result from cache if any
     * @return validation result that breach the validation rules
     */
    public ValidationResult compareHistoricalHoldings(BrokerType brokerType, Date date1, Date date2, Set<String> brokerAccounts, Set<String> symbols, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][Holding] compare Holding between historical holding table of two dates: [BrokerType] " + brokerType.name() + ", [Accounts] " + brokerAccounts + ", [Symbols] " + symbols + ", [RuleName] " + ruleName + ", [Date1] " + date1 + ", [Date2] " + date2);
        String functionName = "compareHistoricalHoldings";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.HOLDING);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, symbols, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        List<HoldingHistory> historicalHoldings1 = holdingDataService.getHistoricalHoldingsFromDB(brokerType, date1, brokerAccounts, symbols);
        List<HoldingHistory> historicalHoldings2 = holdingDataService.getHistoricalHoldingsFromDB(brokerType, date2, brokerAccounts, symbols);
        Map<String, Holding> sourceHoldings = holdingDataService.convertHoldingHistoryListToMap(historicalHoldings1);
        Map<String, Holding> targetHoldings = holdingDataService.convertHoldingHistoryListToMap(historicalHoldings2);
        Map<String, Object> sources = new HashMap<>(sourceHoldings);
        Map<String, Object> targets = new HashMap<>(targetHoldings);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.HOLDING, DataSourceType.HISTORYDATABASE, DataSourceType.HISTORYDATABASE, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * Compare holdings between current holdings in database and broker
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param symbols symbols
     * @param ruleName name of validation rules
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @param useCache get result from cache if any
     * @return validation result that breach the validation rules
     */
    public ValidationResult compareCurrentToBrokerHoldings(BrokerType brokerType, Set<String> brokerAccounts, Set<String> symbols, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][Holding] compare Holding from current holding table to Broker holding: [BrokerType] " + brokerType.name() + ", [Accounts] " + brokerAccounts + ", [Symbols] " + symbols + ", [RuleName] " + ruleName);
        String functionName = "compareCurrentToBrokerHoldings";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.HOLDING);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, symbols, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        if (Objects.isNull(brokerAccounts) || brokerAccounts.contains("All") || brokerAccounts.contains("all") || brokerAccounts.contains("ALL"))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);

        Set<String> validAccounts = brokerAccountService.queryCheckableBrokerAccounts(brokerType, brokerAccounts, true).get(BrokerAccountService.CHECKABLE);
        if (validAccounts.size() == 0)
            return null;

        List<Holding> currentHoldings = holdingDataService.getCurrentHoldingsFromDB(brokerType, validAccounts, symbols);
        List<Holding> brokerHoldings = holdingDataService.getHoldingsFromBroker(brokerType, validAccounts, symbols);
        Map<String, Holding> sourceHoldings = holdingDataService.convertHoldingListToMap(currentHoldings);
        Map<String, Holding> targetHoldings = holdingDataService.convertHoldingListToMap(brokerHoldings);
        Map<String, Object> sources = new HashMap<>(sourceHoldings);
        Map<String, Object> targets = new HashMap<>(targetHoldings);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.HOLDING, DataSourceType.DATABASE, DataSourceType.BROKER, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * Compare holdings between historical holdings in database and broker
     * @param date date of historical holdings
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param symbols symbols
     * @param ruleName name of validation rules
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @param useCache get result from cache if any
     * @return validation result that breach the validation rules
     */
    public ValidationResult compareHistoryToBrokerHoldings(BrokerType brokerType, Date date, Set<String> brokerAccounts, Set<String> symbols, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][Holding] compare Holding from history holding table to Broker holding: [BrokerType] " + brokerType.name() + ", [Accounts] " + brokerAccounts + ", [Symbols] " + symbols + ", [RuleName] " + ruleName + ", [Date] " + date);
        String functionName = "compareHistoryToBrokerHoldings";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.HOLDING);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, symbols, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        if (Objects.isNull(brokerAccounts) || brokerAccounts.contains("All") || brokerAccounts.contains("all") || brokerAccounts.contains("ALL"))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);

        Set<String> validAccounts = brokerAccountService.queryCheckableBrokerAccounts(brokerType, brokerAccounts, true).get(BrokerAccountService.CHECKABLE);
        if (validAccounts.size() == 0)
            return null;

        List<HoldingHistory> historicalHoldings = holdingDataService.getHistoricalHoldingsFromDB(brokerType, date, validAccounts, symbols);
        List<Holding> brokerHoldings = holdingDataService.getHoldingsFromBroker(brokerType, validAccounts, symbols);
        Map<String, Holding> sourceHoldings = holdingDataService.convertHoldingHistoryListToMap(historicalHoldings);
        Map<String, Holding> targetHoldings = holdingDataService.convertHoldingListToMap(brokerHoldings);
        Map<String, Object> sources = new HashMap<>(sourceHoldings);
        Map<String, Object> targets = new HashMap<>(targetHoldings);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.HOLDING, DataSourceType.HISTORYDATABASE, DataSourceType.BROKER, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * Compare holdings between broker and default values
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param symbols symbols
     * @param ruleName names of validation rules
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @param useCache get result from cache if any
     * @return validation result that breach the validation rules
     */
    public ValidationResult compareBrokerToDefaultHoldings(BrokerType brokerType, Set<String> brokerAccounts, Set<String> symbols, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][Holding] compare Holding from from Broker holding to Default holding values: [BrokerType] " + brokerType.name() + ", [Accounts] " + brokerAccounts + ", [Symbols] " + symbols + ", [RuleName] " + ruleName);
        String functionName = "compareBrokerToDefaultHoldings";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.HOLDING);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, symbols, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        if (Objects.isNull(brokerAccounts) || brokerAccounts.contains("All") || brokerAccounts.contains("all") || brokerAccounts.contains("ALL"))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);

        Set<String> validAccounts = brokerAccountService.queryCheckableBrokerAccounts(brokerType, brokerAccounts, true).get(BrokerAccountService.CHECKABLE);
        if (validAccounts.size() == 0)
            return null;

        List<Holding> brokerHoldings = holdingDataService.getHoldingsFromBroker(brokerType, validAccounts, symbols);
        Map<String, Holding> sourceHoldings = holdingDataService.convertHoldingListToMap(brokerHoldings);
        Map<String, Object> targets = constructDefaultObjects(new Holding(), sourceHoldings.keySet(), ruleSet, RiskGroupType.HOLDING);
        Map<String, Object> sources = new HashMap<>(sourceHoldings);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.HOLDING, DataSourceType.BROKER, DataSourceType.DEFAULT, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * Compare holdings between current DB and default values
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param symbols symbols
     * @param ruleName name of validation rules
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @param useCache get result from cache if any
     * @return validation result that breach the validation rules
     */
    public ValidationResult compareCurrentToDefaultHoldings(BrokerType brokerType, Set<String> brokerAccounts, Set<String> symbols, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][Holding] compare Holding from from current holding table to Default holding values: [BrokerType] " + brokerType.name() + ", [Accounts] " + brokerAccounts + ", [Symbols] " + symbols + ", [RuleName] " + ruleName);
        String functionName = "compareCurrentToDefaultHoldings";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.HOLDING);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, symbols, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }
        List<Holding> currentHoldings = holdingDataService.getCurrentHoldingsFromDB(brokerType, brokerAccounts, symbols);
        Map<String, Holding> sourceHoldings = holdingDataService.convertHoldingListToMap(currentHoldings);
        Map<String, Object> targets = constructDefaultObjects(new Holding(), sourceHoldings.keySet(), ruleSet, RiskGroupType.HOLDING);
        Map<String, Object> sources = new HashMap<>(sourceHoldings);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.HOLDING, DataSourceType.DATABASE, DataSourceType.DEFAULT, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * Simple validation on Holding object
     * @param key holding key (BrokerType + BrokerAccount + symbol)
     * @param holding holding object
     * @return true if holding passes validation, otherwise false
     */
    public boolean validateHolding(String key, Holding holding) {
        boolean validationResult = true;
        if (holding.getInstrumentId() == null) {
            log.error("[RiskEngine][Holding] holding validation failed: Key=" + key + ", Exception='field instrument_Id value is null'");
            validationResult = false;
        }
        if (Objects.nonNull(holding.getId()) && holding.getHoldingPosition().compareTo(holding.getLongVolume().add(holding.getShortVolume().negate())) != 0) {
            log.error("[RiskEngine][Holding] holding validation failed: Key=" + key + ", Exception='Long/Short volume not match with aggregated volume'");
            validationResult = false;
        }
        return validationResult;
    }
}
