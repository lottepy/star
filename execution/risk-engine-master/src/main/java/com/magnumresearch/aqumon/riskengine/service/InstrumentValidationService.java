package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.common.feign.LarkRobotClient;
import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.CompareType;
import com.magnumresearch.aqumon.riskengine.constants.DataSourceType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.pojo.ComparisonResult;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationData;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationKey;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class InstrumentValidationService extends BaseValidationService {

    @Autowired
    BaseConfigService baseConfigService;
    @Autowired
    InstrumentDataService instrumentDataService;
    @Autowired
    LarkRobotClient larkRobotClient;

    /**
     * Given a set of fields, to validate if the instrument object from DM contains valid data of these fields.
     * @param symbols symbols
     * @param fieldNames field names
     * @param ruleName valid rule names
     * @param useCache get result from cache if any
     * @return a map of symbols and invalid fields
     */
    public ValidationResult validateInstrumentFieldsFromDM(Set<String> symbols, Set<String> fieldNames, String ruleName, boolean useCache) {
        log.info("[RiskEngine][InstrumentField] compare Instrument fields from DataMaster to Default: Symbols=" + symbols + ", FieldNames=" + fieldNames + ", RuleName=" + ruleName);

        String functionName = "validateInstrumentFieldsFromDM";
        fieldNames = instrumentDataService.getFieldsFromRules(fieldNames, ruleName);
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.INSTRUMENTFIELD);
        String key = BaseCacheService.constructKey(functionName, ruleSet, null, fieldNames, symbols, false);
        if (useCache) {
            ValidationResult validationResult = BaseCacheService.getResultFromCache(key);
            if (Objects.nonNull(validationResult))
                return validationResult;
        }

        List<ValidationData> results = new ArrayList<>();
        Map<String, HashMap<String, Object>> instInfoMap = instrumentDataService.getInstrumentInfo(symbols);
        for (Map.Entry<String, HashMap<String, Object>> instInfoEntry: instInfoMap.entrySet()) {
            Map<String, ComparisonResult> compareResult = validateFields(instInfoEntry.getKey(), instInfoEntry.getValue(), fieldNames, DataSourceType.DATAMASTER, DataSourceType.DEFAULT);
            if (compareResult.size() > 0){
                ValidationData validationData = new ValidationData(ValidationKey.parseKey(instInfoEntry.getKey()), compareResult);
                results.add(validationData);
            }
        }
        BaseCacheService.addResultToCache(key, results);

        if (results.size() > 0) {
            String message = buildDingDingMessage(RiskGroupType.INSTRUMENTFIELD, results);
            larkRobotClient.sendMessage(message);
        }
        return new ValidationResult(key, results);
    }

    /**
     * validate instrument fields
     * @param symbol a list symbols
     * @param instInfo a map of instrument info
     * @param fieldNames a list of field names
     * @return a map of key and comparison results if rule is breached
     */
    public Map<String, ComparisonResult> validateFields(String symbol, HashMap<String, Object> instInfo, Set<String> fieldNames, DataSourceType origin, DataSourceType target) {
        Map<String, ComparisonResult> resultMap = new HashMap<>();
        for (String fieldName : fieldNames) {
            if (Objects.isNull(instInfo) || !instInfo.containsKey(fieldName) || (Objects.isNull(instInfo.get(fieldName)))) {
                ComparisonResult comparisonResult = new ComparisonResult(null, null);
                comparisonResult.setCompareType(CompareType.NONNULL);
                resultMap.put(fieldName, comparisonResult);
                log.info("[RiskEngine][Validation][" + RiskGroupType.INSTRUMENTFIELD.name() + "] validation failed: Symbol=" + symbol + ", FieldName=" + fieldName + ", Origin=" + origin.name() + ", Target=" + target.name() + ", CompareType=ValueExists");
            } else
                log.info("[RiskEngine][Validation][" + RiskGroupType.INSTRUMENTFIELD.name() + "] validation passed: Symbol=" + symbol + ", FieldNames=" + fieldNames + ", Origin=" + origin.name() + ", Target=" + target.name() + ", CompareType=ValueExists");
        }
        return resultMap;
    }

    /**
     * combine a list of validation results into a single dingding message
     * @param validationResults a list of validation results for dingding alert
     * @return a string of dingding message
     */
    public String buildDingDingMessage(RiskGroupType riskGroupType, List<ValidationData> validationResults) {
        StringBuilder message = new StringBuilder();
        message.append("[RiskEngine] Validation failed: riskGroup=").append(riskGroupType).append("\n");
        int index = 0;
        for (ValidationData validationResult: validationResults) {
            for (String field: validationResult.getFieldData().keySet()) {
                if (index < 10)
                    message.append("#").append(index + 1).append(": Key=").append(validationResult.getValidationKey().toString()).append(", FieldName=").append(field).append(", Source=DataMaster, Exception=FieldNonExists\n");
                index = index + 1;
            }
        }
        message.append("...\n").append("Total ").append(index).append(" breaches found");
        return message.toString();
    }
}