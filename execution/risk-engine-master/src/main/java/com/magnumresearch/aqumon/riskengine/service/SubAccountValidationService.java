package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.DataSourceType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationData;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.SubAccountSummary;
import com.magnumresearch.aqumon.trading.pojo.CashInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class SubAccountValidationService extends BaseValidationService{

    @Autowired
    BaseConfigService baseConfigService;
    @Autowired
    BrokerAccountService brokerAccountService;
    @Autowired
    SubAccountDataService subAccountDataService;

    Logger log = LoggerFactory.getLogger(this.getClass());
    
    /**
     * compare sub-account objects between database and broker
     * @param brokerType broker type
     * @param brokerAccounts broker account
     * @param ruleName name of validation rules
     * @param includeObjects true to include objects in return data, otherwise return diff only
     * @param useCache get result from cache if any
     * @return a map of key (brokerType + brokerAccount + SubAccountId) to two sub-account objects if they are different
     */
    public ValidationResult compareDBToBrokerSubAccountSummary(BrokerType brokerType, Set<String> brokerAccounts, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][SubAccountSummary] compare SubAccountSummary from Database to Broker: BrokerType=" + brokerType.name() + ", Accounts=" + brokerAccounts + ", RuleName=" + ruleName);

        String functionName = "compareDBToBrokerSubAccountSummary";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.SUBACCOUNTSUMMARY);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, null, includeObjects);
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

        Map<String, SubAccountSummary> sourceSubAccountSummaries = subAccountDataService.getSubAccountSummariesFromDB(brokerType, validAccounts);
        Map<String, SubAccountSummary> targetSubAccountSummaries = subAccountDataService.getSubAccountSummariesFromBroker(brokerType, validAccounts);
        Map<String, Object> sources = new HashMap<>(sourceSubAccountSummaries);
        Map<String, Object> targets = new HashMap<>(targetSubAccountSummaries);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.SUBACCOUNTSUMMARY, DataSourceType.DATABASE, DataSourceType.BROKER, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * compare sub-account objects between current and default values
     * @param brokerType broker type
     * @param brokerAccounts broker account
     * @param ruleName name of validation rules
     * @param includeObjects true to include objects in return data, otherwise return diff only
     * @param useCache get result from cache if any
     * @return a map of key (brokerType + brokerAccount + SubAccountId) to two sub-account objects if they are different
     */
    public ValidationResult compareDBToDefaultSubAccountSummary(BrokerType brokerType, Set<String> brokerAccounts, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][SubAccountSummary] compare SubAccountSummary from Database to Default: BrokerType=" + brokerType.name() + ", Accounts=" + brokerAccounts + ", RuleName=" + ruleName);

        String functionName = "compareDBToDefaultSubAccountSummary";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.SUBACCOUNTSUMMARY);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, null, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        if (Objects.isNull(brokerAccounts) || brokerAccounts.contains("All") || brokerAccounts.contains("all") || brokerAccounts.contains("ALL"))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);
        Map<String, SubAccountSummary> sourceSubAccountSummaries = subAccountDataService.getSubAccountSummariesFromDB(brokerType, brokerAccounts);
        Map<String, Object> sources = new HashMap<>(sourceSubAccountSummaries);
        Map<String, Object> targets = constructDefaultObjects(new SubAccountSummary(), sourceSubAccountSummaries.keySet(), ruleSet, RiskGroupType.SUBACCOUNTSUMMARY);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.SUBACCOUNTSUMMARY, DataSourceType.DATABASE, DataSourceType.DEFAULT, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * compare sub-account objects between broker and default values
     * @param brokerType broker type
     * @param brokerAccounts broker account
     * @param ruleName names of validation rules
     * @param includeObjects true to include objects in return data, otherwise return diff only
     * @param useCache get result from cache if any
     * @return a map of key (brokerType + brokerAccount + SubAccountId) to two sub-account objects if they are different
     */
    public ValidationResult compareBrokerToDefaultSubAccountSummary(BrokerType brokerType, Set<String> brokerAccounts, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][SubAccountSummary] compare SubAccountSummary from Broker to Default: BrokerType=" + brokerType.name() + ", Accounts=" + brokerAccounts + ", RuleName=" + ruleName);

        String functionName = "compareBrokerToDefaultSubAccountSummary";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.SUBACCOUNTSUMMARY);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, null, includeObjects);
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

        Map<String, SubAccountSummary> sourceSubAccountSummaries = subAccountDataService.getSubAccountSummariesFromBroker(brokerType, validAccounts);
        Map<String, Object> sources = new HashMap<>(sourceSubAccountSummaries);
        Map<String, Object> targets = constructDefaultObjects(new SubAccountSummary(), sourceSubAccountSummaries.keySet(), ruleSet, RiskGroupType.SUBACCOUNTSUMMARY);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.SUBACCOUNTSUMMARY, DataSourceType.BROKER, DataSourceType.DEFAULT, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * compare sub-account objects between broker and default values
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param ruleName name of validation rules
     * @param includeObjects true to include objects in return data, otherwise return diff only
     * @param useCache get result from cache if any
     * @return a map of key (brokerType + brokerAccount + SubAccountId) to two sub-account objects if they are different
     */
    public ValidationResult compareBrokerToDefaultCashInfoMap(BrokerType brokerType, Set<String> brokerAccounts, String ruleName, boolean includeObjects, boolean useCache) {
        log.info("[RiskEngine][CashInfo] compare CashInfo from Broker to Default: BrokerType=" + brokerType.name() + ", Accounts=" + brokerAccounts + ", RuleName=" + ruleName + ", useCache=" + useCache);

        String functionName = "compareBrokerToDefaultCashInfoMap";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.SUBACCOUNTSUMMARY);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, brokerAccounts, null, includeObjects);
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

        Map<String, CashInfo> sourceCashInfos = subAccountDataService.getCashInfoMapFromBroker(brokerType, validAccounts);
        Map<String, Object> sources = new HashMap<>(sourceCashInfos);
        Map<String, Object> targets = constructDefaultObjects(new CashInfo(), sourceCashInfos.keySet(), ruleSet, RiskGroupType.SUBACCOUNTSUMMARY);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.SUBACCOUNTSUMMARY, DataSourceType.BROKER, DataSourceType.DEFAULT, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }
}

