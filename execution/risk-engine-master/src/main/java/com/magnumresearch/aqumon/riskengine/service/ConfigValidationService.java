package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.riskengine.config.BaseFilterConfig;
import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.config.BaseSchedulesConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Service
public class ConfigValidationService {

    @Autowired
    BaseFilterConfig baseFilterConfig;
    @Autowired
    BaseRulesConfig baseRulesConfig;
    @Autowired
    BaseSchedulesConfig baseSchedulesConfig;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * validate configuration
     * @return a map of rules and its error message
     */
    public Map<String, String> validateConfigs() {
        Map<String, String> resultMap = new HashMap<>();
        Map<String, Set<BaseFilterConfig.AccountFilter>> accountFilters = baseFilterConfig.parseAccountJson(baseFilterConfig.getAccounts());
        Map<String, Set<BaseFilterConfig.SymbolFilter>> symbolFilters = baseFilterConfig.parseSymbolJson(baseFilterConfig.getSymbols());
        Map<String, Set<BaseFilterConfig.FieldFilter>> fieldFilters = baseFilterConfig.parseFieldJson(baseFilterConfig.getFields());
        Map<String, Set<BaseRulesConfig.ValidationRule>> rules = baseRulesConfig.parseValidationRuleJson(baseRulesConfig.getRules());
        Map<String, BaseSchedulesConfig.ScheduleRule> schedules = baseSchedulesConfig.parseScheduleRuleJson(baseSchedulesConfig.getSchedules());

        String msg;
        for (Map.Entry<String, Set<BaseFilterConfig.AccountFilter>> accountEntry: accountFilters.entrySet()) {
            for (BaseFilterConfig.AccountFilter accountFilter: accountEntry.getValue()) {
                msg = baseFilterConfig.validateAccountFilter(accountFilter);
                if (Objects.nonNull(msg)) {
                    resultMap.put(accountEntry.getKey(), msg);
                    log.info("[RiskEngine][Configuration] account filter configuration validation failed: accountFilter=" + accountFilter.getKey());
                } else {
                    log.info("[RiskEngine][Configuration] account filter configuration validation passed: accountFilter=" + accountFilter.getKey());
                }
            }
        }

        for (Map.Entry<String, Set<BaseFilterConfig.SymbolFilter>> symbolEntry: symbolFilters.entrySet()) {
            for (BaseFilterConfig.SymbolFilter symbolFilter: symbolEntry.getValue()) {
                msg = baseFilterConfig.validateSymbolFilter(symbolFilter);
                if (Objects.nonNull(msg)) {
                    resultMap.put(symbolEntry.getKey(), msg);
                    log.info("[RiskEngine][Configuration] symbol filter configuration validation failed: symbolFilter=" + symbolFilter.getKey());
                } else {
                    log.info("[RiskEngine][Configuration] symbol filter configuration validation passed: symbolFilter=" + symbolFilter.getKey());
                }
            }
        }

        for (Map.Entry<String, Set<BaseFilterConfig.FieldFilter>> fieldEntry: fieldFilters.entrySet()) {
            for (BaseFilterConfig.FieldFilter fieldFilter: fieldEntry.getValue()) {
                msg = baseFilterConfig.validateFieldFilter(fieldFilter);
                if (Objects.nonNull(msg)) {
                    resultMap.put(fieldEntry.getKey(), msg);
                    log.info("[RiskEngine][Configuration] field filter configuration validation failed: fieldFilter=" + fieldFilter.getKey());
                } else {
                    log.info("[RiskEngine][Configuration] field filter configuration validation passed: fieldFilter=" + fieldFilter.getKey());
                }
            }
        }

        for (Map.Entry<String, Set<BaseRulesConfig.ValidationRule>> ruleEntry: rules.entrySet()) {
            for (BaseRulesConfig.ValidationRule rule: ruleEntry.getValue()) {
                msg = baseRulesConfig.validateRules(rule);
                if (Objects.nonNull(msg)) {
                    resultMap.put(ruleEntry.getKey(), msg);
                    log.info("[RiskEngine][Configuration] validation rule configuration validation failed: rule=" + rule.getKey());
                } else {
                    log.info("[RiskEngine][Configuration] validation rule configuration validation passed: rule=" + rule.getKey());
                }
            }
        }

        for (Map.Entry<String, BaseSchedulesConfig.ScheduleRule> scheduleEntry: schedules.entrySet()) {
            msg = baseSchedulesConfig.validateSchedules(scheduleEntry.getValue());
            if (Objects.nonNull(msg)) {
                resultMap.put(scheduleEntry.getKey(), msg);
                log.info("[RiskEngine][Configuration] schedule configuration validation failed: schedule=" + scheduleEntry.getValue().getKey());
            } else {
                log.info("[RiskEngine][Configuration] schedule configuration validation passed: schedule=" + scheduleEntry.getValue().getKey());
            }
        }

        return resultMap;
    }
}