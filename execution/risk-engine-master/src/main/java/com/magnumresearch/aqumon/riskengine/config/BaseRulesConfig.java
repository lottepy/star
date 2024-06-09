package com.magnumresearch.aqumon.riskengine.config;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.magnumresearch.aqumon.riskengine.constants.CalculationType;
import com.magnumresearch.aqumon.riskengine.constants.CompareType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import java.util.*;

@Data
@RefreshScope
@ConfigurationProperties(prefix = "risk.health")
public class BaseRulesConfig {
    public String rules;
    static Logger log = LoggerFactory.getLogger(BaseRulesConfig.class);

    @Autowired
    BaseFilterConfig baseFilterConfig;

    @Data
    public static class ValidationRule {
        String key;
        String name;
        RiskGroupType riskGroup;
        Set<String> fieldFilters;
        Set<String> fieldNames;
        CompareType compareType;
        CalculationType calculationType;
        Double threshold;
        Double defaultValue;
    }

    /**
     * parse json string from config into a map of config name and validationRule objects
     * @param jsonStr json string from config
     * @return a map of config name and validationRule objects
     */
    public Map<String, Set<ValidationRule>> parseValidationRuleJson(String jsonStr) {
        Map<String, Set<ValidationRule>> validationRuleMap = new HashMap<>();
        if (Objects.isNull(jsonStr) || jsonStr.trim().length() == 0)
            return validationRuleMap;

        List<ValidationRule> validationRules;
        try {
            validationRules = new Gson().fromJson(jsonStr, new TypeToken<List<ValidationRule>>() {}.getType());
        } catch (JsonSyntaxException jsonSyntaxException) {
            log.error("[RiskEngine][FilterConfig] validation rules configuration failed to parse: validationRules=" + jsonStr, jsonSyntaxException);
            return validationRuleMap;
        }

        for (ValidationRule validationRule: validationRules) {
            if (Objects.nonNull(validationRule)) {
                Set<String> fieldNames = enrichRuleWithFields(validationRule);
                validationRule.setKey(buildValidateRuleKey(validationRule));
                validationRule.setFieldNames(fieldNames);
                if (validationRuleMap.containsKey(validationRule.getName()))
                    validationRuleMap.get(validationRule.getName()).add(validationRule);
                else {
                    Set<ValidationRule> ruleList = new HashSet<>();
                    ruleList.add(validationRule);
                    validationRuleMap.put(validationRule.getName(), ruleList);
                }
            }
        }
        return  validationRuleMap;
    }

    /**
     * generate key for validation rule object
     * @param validationRule validation rule object
     * @return key for validation rule object
     */
    public String buildValidateRuleKey(ValidationRule validationRule) {
        String riskGroup = Objects.nonNull(validationRule.getRiskGroup()) ? validationRule.getRiskGroup().name() : "NULL";
        String compareType = Objects.nonNull(validationRule.getCompareType()) ? validationRule.getCompareType().name() : "NULL";
        String calcType = Objects.nonNull(validationRule.getCalculationType()) ? validationRule.getCalculationType().name() : "NULL";
        String threshold = Objects.nonNull(validationRule.getThreshold()) ? validationRule.getThreshold().toString() : "NULL";
        String defaultValue = Objects.nonNull(validationRule.getDefaultValue()) ? validationRule.getThreshold().toString() : "NULL";
        String fields = Objects.nonNull(validationRule.getFieldNames()) && validationRule.getFieldNames().size() > 0 ? String.join(",", validationRule.getFieldNames()) : "NULL";
        return validationRule.getName() + "|" + riskGroup + "|" + compareType + "|" + calcType + "|" + threshold + "|" + defaultValue + "|" + fields;
    }

    public Set<String> enrichRuleWithFields(ValidationRule validationRule) {
        Set<String> fieldNames = new HashSet<>();
        if (Objects.isNull(validationRule.getFieldFilters()))
            return fieldNames;

        Map<String, Set<BaseFilterConfig.FieldFilter>> fieldFilters = baseFilterConfig.parseFieldJson(baseFilterConfig.getFields());
        for (String fieldFilterName: validationRule.getFieldFilters()) {
            if (fieldFilters.containsKey(fieldFilterName))
                for (BaseFilterConfig.FieldFilter fieldFilter: fieldFilters.get(fieldFilterName))
                    fieldNames.addAll(fieldFilter.getFields());
            else
                log.error("[RiskEngine][RulesConfig] validation rule configuration failed to enrich fieldFilters: validationRule=" + validationRule.getKey() + ", fieldFilter=" + fieldFilterName);
        }
        return fieldNames;
    }

    /**
     * validate rule object
     * @param validationRule rule object
     * @return error message for rule object validation
     */
    public String validateRules(ValidationRule validationRule) {
        String msg;
        if (Objects.isNull(validationRule.getName())) {
            msg = "[RiskEngine][FilterConfig] validation rule configuration failed to get rule name";
            return msg;
        }
        if (Objects.isNull(validationRule.getRiskGroup())) {
            msg = "[RiskEngine][FilterConfig] validation rule configuration failed to get risk group: validationRule=" + validationRule.getKey();
            return msg;
        }
        if (Objects.isNull(validationRule.getFieldFilters()) || validationRule.getFieldFilters().size() <= 0) {
            if (validationRule.getRiskGroup() != RiskGroupType.SPREAD) {
                msg = "[RiskEngine][FilterConfig] validation rule configuration failed to get field filters: validationRule=" + validationRule.getKey();
                return msg;
            }
        }
        Map<String, Set<BaseFilterConfig.FieldFilter>> fieldFilters = baseFilterConfig.parseFieldJson(baseFilterConfig.getFields());
        for (String fieldFilterName: validationRule.getFieldFilters()) {
            if (!fieldFilters.containsKey(fieldFilterName)) {
                msg = "[RiskEngine][RulesConfig] validation rule configuration failed to match fieldFilters: validationRule=" + validationRule.getKey() + ", fieldFilter=" + fieldFilterName;
                return msg;
            }
        }
        if (Objects.isNull(validationRule.getCompareType())) {
            msg = "[RiskEngine][FilterConfig] validation rule configuration failed to get compare type: validationRule=" + validationRule.getKey();
            return msg;
        }
        if (validationRule.getCompareType() == CompareType.ABSOLUTE || validationRule.getCompareType() == CompareType.PERCENTAGE) {
            if (Objects.isNull(validationRule.getCalculationType())) {
                msg = "[RiskEngine][FilterConfig] validation rule configuration failed to get calculation type: validationRule=" + validationRule.getKey();
                return msg;
            }
            if (Objects.isNull(validationRule.getThreshold())) {
                msg = "[RiskEngine][FilterConfig] validation rule configuration failed to get threshold: validationRule=" + validationRule.getKey();
                return msg;
            }
            if (Objects.isNull(validationRule.getDefaultValue())) {
                msg = "[RiskEngine][FilterConfig] validation rule configuration failed to get default value: validationRule=" + validationRule.getKey();
                return msg;
            }
        }
        return null;
    }
}
