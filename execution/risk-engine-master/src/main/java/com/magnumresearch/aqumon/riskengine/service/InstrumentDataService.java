package com.magnumresearch.aqumon.riskengine.service;

import com.google.common.collect.Iterables;
import com.magnumresearch.aqumon.common.feign.DataMasterFeignClient;
import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationKey;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class InstrumentDataService {
    @Autowired
    DataMasterFeignClient dataMasterFeignClient;
    @Autowired
    BaseConfigService baseConfigService;

    public static final int SIZE_EACH_PARTITION = 10;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * get a map of symbol and instrument object
     * @param symbols input symbols
     * @return a map of symbol and instrument object
     */
    public Map<String, HashMap<String, Object>> getInstrumentInfo(Set<String> symbols) {
        Iterable<List<String>> batchSymbols = Iterables.partition(symbols, SIZE_EACH_PARTITION);
        HashMap<String, HashMap<String, Object>> returnData = new HashMap<>();
        for (List<String> batch: batchSymbols) {
            Map<String, HashMap<String, Object>> singleData;
            try {
                singleData = dataMasterFeignClient.getInstrumentInfo(new ArrayList<>(batch)).getSymbolToFieldsToValuesMapping();
                for (Map.Entry<String, HashMap<String, Object>> entry: singleData.entrySet()) {
                    ValidationKey validationKey = new ValidationKey(RiskGroupType.INSTRUMENTFIELD, BrokerType.NEUTRAL, null, null, entry.getKey());
                    String key = validationKey.toString();
                    returnData.put(key, entry.getValue());
                }
            } catch (Exception exception) {
                log.error("[RiskEngine][InstrumentField] Failed to get instrument info from DataMaster: Symbols=" + Arrays.toString(symbols.toArray()) + ", Exception=", exception);
            }
        }
        return returnData;
    }

    /**
     * get a list of fields from field filter rules if input fields are not specified
     * @param fieldNames a list of fields
     * @param ruleName field rule name
     * @return a list of fields
     */
    public Set<String> getFieldsFromRules(Set<String> fieldNames, String ruleName) {
        if (Objects.isNull(fieldNames) || fieldNames.size() == 0) {
            Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.INSTRUMENTFIELD);
            fieldNames = new HashSet<>();
            for (BaseRulesConfig.ValidationRule rule : ruleSet)
                fieldNames.addAll(rule.getFieldNames());
        }
        return fieldNames;
    }
}
