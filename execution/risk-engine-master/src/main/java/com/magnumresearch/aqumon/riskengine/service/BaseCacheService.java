package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationData;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class BaseCacheService {

    public static Logger log = LoggerFactory.getLogger(BaseCacheService.class);
    public static final Map<String, List<ValidationResult>> CACHEMAP = new ConcurrentHashMap<>();
    public static final int QUEUESIZE = 3;

    /**
     * add validation results into cache
     * @param key key of cache
     * @param results list of validation data
     */
    public static void addResultToCache(String key, List<ValidationData> results) {
        ValidationResult validationResult = new ValidationResult(key, results);
        if (CACHEMAP.containsKey(key)) {
            List<ValidationResult> list = CACHEMAP.get(key);
            if (list.size() >= QUEUESIZE)
                list.remove(list.size()-1);
            list.add(0, validationResult);
        }
        else {
            List<ValidationResult> list = new CopyOnWriteArrayList<>();
            list.add(0, validationResult);
            CACHEMAP.put(key, list);
        }
        log.info("[RiskEngine][Cache] validation results added into cache: key=" + key + ", resultSize=" + results.size());
    }

    /**
     * get validation result from cache
     * @param key key of cache
     * @return validation result from cache
     */
    public static ValidationResult getResultFromCache(String key) {
        if (CACHEMAP.containsKey(key) && CACHEMAP.get(key).size() > 0) {
            log.info("[RiskEngine][Cache] get validation results from cache: key=" + key);
            return CACHEMAP.get(key).get(0);
        }
        return null;
    }

    /**
     * list all keys within cache
     * @return a list of keys from cache
     */
    public static Map<String, Integer> listCacheKey() {
        Map<String, Integer> results = new HashMap<>();
        for (Map.Entry<String, List<ValidationResult>> entry: CACHEMAP.entrySet()) {
            results.put(entry.getKey(), entry.getValue().size());
        }
        return results;
    }

    /**
     * build a key to cache
     * @param functionName function name
     * @param ruleSet a set of validation rule
     * @param brokerType broker type
     * @param accounts a set of accounts
     * @param symbols a set of symbols
     * @param includeObjects an indicator to include objects or include differences only
     * @return a key to cache
     */
    public static String constructKey(String functionName, Set<BaseRulesConfig.ValidationRule> ruleSet, BrokerType brokerType, Set<String> accounts, Set<String> symbols, boolean includeObjects) {
        StringBuilder key = new StringBuilder();
        key.append(functionName).append("|");

        if (Objects.nonNull(ruleSet)) {
            for (BaseRulesConfig.ValidationRule ruleObj : ruleSet)
                key.append(ruleObj.getName()).append(",");
            key.deleteCharAt(key.lastIndexOf(","));
            key.append("|");
        }

        if (Objects.nonNull(brokerType))
            key.append(brokerType.name()).append("|");

        if (Objects.nonNull(accounts) && accounts.size() > 0)
            key.append(String.join(",", accounts)).append("|");

        if (Objects.nonNull(symbols) && symbols.size() > 0)
            key.append(String.join(",", symbols)).append("|");

        key.append(includeObjects);

        return key.toString();
    }
}
