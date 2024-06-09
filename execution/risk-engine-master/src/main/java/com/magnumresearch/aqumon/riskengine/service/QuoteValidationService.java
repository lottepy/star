package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.quoteengine.pojo.SnapshotResponse;
import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.DataSourceType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationData;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.Holding;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.*;

@Service
public class QuoteValidationService extends BaseValidationService {

    @Autowired
    BaseConfigService baseConfigService;
    @Autowired
    QuoteDataService quoteDataService;

    /**
     * Check quotes from Quote Engine against Market Data Snapshot
     * @param brokerType source of quotes, currently supporting Neutral (from data master), KUAIRUI or BOCI
     * @param symbols a list of symbols to be check
     * @param ruleName name of validation rules
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @param useCache get result from cache if any
     * @return a map of symbols with different quotes if quotes diff from QuoteEngine and Market Data Snapshot are beyond the threshold
     * @throws IllegalStateException return state from datamaster or snapshot is incorrect
     */
    public ValidationResult compareDMToSnapshotQuotes(BrokerType brokerType, Set<String> symbols, String ruleName, boolean includeObjects, boolean useCache) throws IllegalStateException {
        log.info("[RiskEngine][Quote] compare SnapshotResponse from DataMaster to Snapshot: BrokerType=" + brokerType.name() + ", Symbols=" + symbols + ", RuleName=" + ruleName);

        String functionName = "compareDMToSnapshotQuotes";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.QUOTE);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, null, symbols, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        Map<String, SnapshotResponse> sourceQuotes = quoteDataService.getQuotesFromDM(brokerType, symbols, true);
        Map<String, SnapshotResponse> targetQuotes = quoteDataService.getQuotesFromDM(brokerType, symbols, false);
        Map<String, Object> sources = new HashMap<>(sourceQuotes);
        Map<String, Object> targets = new HashMap<>(targetQuotes);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.QUOTE, DataSourceType.DATAMASTER, DataSourceType.SNAPSHOT, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * Check quotes from Quote Engine against default values
     * @param brokerType source of quotes, currently supporting Neutral (from data master), KUAIRUI or BOCI
     * @param symbols a list of symbols to be check
     * @param ruleName name of validation rules
     * @param includeObjects true if the whole object to be stored, otherwise just the checked difference parts
     * @param enableStreaming true  if quote source is from streaming, false from snapshot
     * @param useCache get result from cache if any
     * @return a map of symbols with different quotes if quotes diff from QuoteEngine and Market Data Snapshot are beyond the threshold
     * @throws IllegalStateException return state from datamaster or snapshot is incorrect
     */
    public ValidationResult compareDMToDefaultQuotes(BrokerType brokerType, Set<String> symbols, String ruleName, boolean includeObjects, boolean enableStreaming, boolean useCache) throws IllegalStateException {
        log.info("[RiskEngine][Quote] compare SnapshotResponse from DataMaster to Default: BrokerType=" + brokerType.name() + ", Symbols=" + symbols + ", RuleName=" + ruleName + ", EnableStreaming=" + enableStreaming);

        String functionName = "compareDMToDefaultQuotes";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.QUOTE);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, null, symbols, includeObjects);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        Map<String, SnapshotResponse> sourceQuotes = quoteDataService.getQuotesFromDM(brokerType, symbols, enableStreaming);
        Map<String, Object> sources = new HashMap<>(sourceQuotes);
        Map<String, Object> targets = constructDefaultObjects(new Holding(), sourceQuotes.keySet(), ruleSet, RiskGroupType.QUOTE);
        List<ValidationData> result = compareObjectMap(sources, targets, ruleSet, RiskGroupType.QUOTE, DataSourceType.DATAMASTER, DataSourceType.DEFAULT, null, includeObjects);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }

    /**
     * Check spread from Quote Engine against default values
     * @param brokerType source of quotes, currently supporting Neutral (from data master), KUAIRUI or BOCI
     * @param symbols a list of symbols to be check
     * @param enableStreaming true if quote source is from streaming, false from snapshot
     * @param useCache get result from cache if any
     * @return a map of symbols to spread if spread diff from QuoteEngine and Market Data Snapshot/default data are beyond the threshold
     * @throws IllegalStateException return state from datamaster or snapshot is incorrect
     */
    public ValidationResult compareDMToDefaultSpread(BrokerType brokerType, Set<String> symbols, boolean enableStreaming, boolean useCache) throws IllegalStateException {
        log.info("[RiskEngine][Spread] compare SnapshotResponse Spread from DataMaster to Default: BrokerType=" + brokerType.name() + ", Symbols=" + symbols + ", RuleType=" + RiskGroupType.SPREAD);

        String functionName = "compareDMToDefaultSpread";
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(null, RiskGroupType.SPREAD);
        String key = BaseCacheService.constructKey(functionName, ruleSet, brokerType, null, symbols, false) + "|" + enableStreaming;
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        Map<String, SnapshotResponse> sourceQuotes = quoteDataService.getQuotesFromDM(brokerType, symbols, enableStreaming);
        Map<String, BigDecimal> sources = quoteDataService.calculateSpreads(sourceQuotes);

        if (ruleSet.size() != 1)
            log.info("[RiskEngine][Spread] riskGroupType has more than 1 rules, 1 of the rules will be used randomly");

        BigDecimal defaultValue = null;
        for (BaseRulesConfig.ValidationRule rule: ruleSet)
            defaultValue = BigDecimal.valueOf(rule.getDefaultValue());
        if (Objects.isNull(defaultValue))
            return null;

        Map<String, BigDecimal> targets = new HashMap<>();
        for (Map.Entry<String, BigDecimal> entry: sources.entrySet())
            targets.put(entry.getKey(), defaultValue);

        List<ValidationData> result = compareObjectMap(new HashMap<>(sources), new HashMap<>(targets), ruleSet, RiskGroupType.SPREAD, DataSourceType.DATAMASTER, DataSourceType.DEFAULT, "Spread", false);
        BaseCacheService.addResultToCache(key, result);
        return new ValidationResult(key, result);
    }
}