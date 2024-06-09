package com.magnumresearch.aqumon.riskengine.config;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.magnumresearch.aqumon.riskengine.constants.DataSourceType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import lombok.Data;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import java.util.*;

@Data
@RefreshScope
@ConfigurationProperties(prefix = "risk.health")
public class BaseSchedulesConfig {
    public String schedules;
    static Logger log = LoggerFactory.getLogger(BaseSchedulesConfig.class);

    @Autowired
    BaseFilterConfig baseFilterConfig;
    @Autowired
    BaseRulesConfig baseRulesConfig;

    @Data
    public static class ScheduleRule {
        String key;
        String name;
        Set<String> rules;
        Set<BaseRulesConfig.ValidationRule> ruleList;
        Set<String> symbolFilters;
        Set<String> symbolNames;
        Set<String> accountFilters;
        Map<BrokerType, Set<String>> accountMap;
        DataSourceType source;
        DataSourceType target;
        Set<String> cronSchedules;
        Boolean enableSync;
    }

    /**
     * parse json string from config into a map of config name and scheduleRule objects
     * @param jsonStr json string from config
     * @return a map of config name and scheduleRule objects
     */
    public Map<String, ScheduleRule> parseScheduleRuleJson(String jsonStr) {
        Map<String, ScheduleRule> ScheduleRuleMap = new HashMap<>();
        if (Objects.isNull(jsonStr) || jsonStr.trim().length() == 0)
            return ScheduleRuleMap;

        List<ScheduleRule> scheduleRules;
        try {
            scheduleRules = new Gson().fromJson(jsonStr, new TypeToken<List<ScheduleRule>>() {}.getType());
        } catch (JsonSyntaxException jsonSyntaxException) {
            log.error("[RiskEngine][FilterConfig] schedule configuration failed to parse: ScheduleRules=" + jsonStr, jsonSyntaxException);
            return ScheduleRuleMap;
        }

        for (ScheduleRule scheduleRule: scheduleRules) {
            if (Objects.nonNull(scheduleRule)) {
                Set<String> symbolNames = enrichScheduleWithSymbols(scheduleRule);
                scheduleRule.setSymbolNames(symbolNames);
                Map<BrokerType, Set<String>> accountMap = enrichScheduleWithAccounts(scheduleRule);
                scheduleRule.setAccountMap(accountMap);
                Set<BaseRulesConfig.ValidationRule> ruleList = enrichScheduleWithRules(scheduleRule);
                scheduleRule.setRuleList(ruleList);
                scheduleRule.setKey(buildScheduleRuleKey(scheduleRule));
                ScheduleRuleMap.put(scheduleRule.getName(), scheduleRule);
            }
        }
        return  ScheduleRuleMap;
    }

    /**
     * generate key for schedule rule object
     * @param scheduleRule schedule rule object
     * @return key for schedule rule object
     */
    public String buildScheduleRuleKey(ScheduleRule scheduleRule) {
        String name = scheduleRule.getName();
        String rules = Objects.nonNull(scheduleRule.getRules()) && scheduleRule.getRules().size() > 0 ? String.join(",", scheduleRule.getRules()) : "NULL";
        String symbols = Objects.nonNull(scheduleRule.getSymbolNames()) && scheduleRule.getSymbolNames().size() > 0 ? String.join(",", scheduleRule.getSymbolNames()) : "NULL";
        String source = Objects.nonNull(scheduleRule.getSource()) ? scheduleRule.getSource().name() : "NULL";
        String target = Objects.nonNull(scheduleRule.getTarget()) ? scheduleRule.getTarget().name() : "NULL";
        StringBuilder accounts = new StringBuilder();
        if (Objects.nonNull(scheduleRule.getAccountMap()) && scheduleRule.getAccountMap().size() > 0)
            for (Map.Entry<BrokerType, Set<String>> entry: scheduleRule.getAccountMap().entrySet())
                accounts.append(entry.getKey().name()).append(":").append(String.join(",", entry.getValue())).append(" ");
        else
            accounts = new StringBuilder("NULL");
        return name + "|" + rules + "|" + accounts.toString().trim() + "|" + symbols + "|" + source + "|" + target;

    }

    /**
     * gets symbols from filter rules in schedule
     * @param scheduleRule schedule rule object
     * @return symbols from filter rules defined in schedule
     */
    public Set<String> enrichScheduleWithSymbols(ScheduleRule scheduleRule) {
        Set<String> symbolNames = new HashSet<>();
        Map<String, Set<BaseFilterConfig.SymbolFilter>> symbolFilters = baseFilterConfig.parseSymbolJson(baseFilterConfig.getSymbols());
        for (String symbolFilterName: scheduleRule.getSymbolFilters()) {
            if (symbolFilters.containsKey(symbolFilterName))
                for (BaseFilterConfig.SymbolFilter symbolFilter: symbolFilters.get(symbolFilterName))
                    symbolNames.addAll(symbolFilter.getSymbols());
            else
                log.error("[RiskEngine][RulesConfig] schedule configuration failed to enrich symbolFilters: scheduleRule=" + scheduleRule.getKey() + ", symbolFilter=", symbolFilterName);
        }
        return symbolNames;
    }

    /**
     * gets accounts from filter rules in schedule
     * @param scheduleRule schedule rule object
     * @return accounts from filter rules defined in schedule
     */
    public Map<BrokerType, Set<String>> enrichScheduleWithAccounts(ScheduleRule scheduleRule) {
        Map<BrokerType, Set<String>> accountMap = new HashMap<>();
        Map<String, Set<BaseFilterConfig.AccountFilter>> accountFilters = baseFilterConfig.parseAccountJson(baseFilterConfig.getAccounts());
        for (String accountFilterName: scheduleRule.getAccountFilters()) {
            if (accountFilters.containsKey(accountFilterName)) {
                for (BaseFilterConfig.AccountFilter accountFilter: accountFilters.get(accountFilterName)) {
                    BrokerType broker = accountFilter.getBroker();
                    if (accountMap.containsKey(broker))
                        accountMap.get(broker).addAll(accountFilter.getAccounts());
                    else
                        accountMap.put(broker, new HashSet<>(accountFilter.getAccounts()));
                }
            }
            else
                log.error("[RiskEngine][RulesConfig] schedule configuration failed to enrich accountFilters: scheduleRule=" + scheduleRule.getKey() + ", accountFilter=" + accountFilterName);
        }
        return accountMap;
    }

    /**
     * gets validation rule objects from filter rules in schedule
     * @param scheduleRule schedule rule object
     * @return validation rule objects from rules defined in schedule
     */
    public Set<BaseRulesConfig.ValidationRule> enrichScheduleWithRules(ScheduleRule scheduleRule) {
        Set<BaseRulesConfig.ValidationRule> ruleList = new HashSet<>();
        Map<String, Set<BaseRulesConfig.ValidationRule>> validationRules = baseRulesConfig.parseValidationRuleJson(baseRulesConfig.getRules());
        for (String ruleName: scheduleRule.getRules()) {
            if (validationRules.containsKey(ruleName))
                ruleList.addAll(validationRules.get(ruleName));
            else
                log.error("[RiskEngine][RulesConfig] schedule configuration failed to enrich validation rules: scheduleRule=" + scheduleRule.getKey() + ", validationRule=", ruleName);
        }
        return ruleList;
    }

    /**
     * validate schedule rule object
     * @param scheduleRule schedule rule object
     * @return error message for schedule rule object validation
     */
    public String validateSchedules(ScheduleRule scheduleRule) {
        String msg;
        if (Objects.isNull(scheduleRule.getName())) {
            msg = "[RiskEngine][FilterConfig] schedule configuration failed to get schedule name: scheduleRule=" + scheduleRule.getKey();
            return msg;
        }

        if (Objects.isNull(scheduleRule.getRules())) {
            msg = "[RiskEngine][FilterConfig] schedule configuration failed to get validation rules: scheduleRule=" + scheduleRule.getKey();
            return msg;
        }
        Map<String, Set<BaseRulesConfig.ValidationRule>> validationRules = baseRulesConfig.parseValidationRuleJson(baseRulesConfig.getRules());
        Set<RiskGroupType> riskGroupTypes = new HashSet<>();
        for (String validationRuleName: scheduleRule.getRules()) {
            if (!validationRules.containsKey(validationRuleName)) {
                msg = "[RiskEngine][RulesConfig] schedule configuration failed to get validation rule: scheduleRule=" + scheduleRule.getKey() + ", validationRule=" + validationRuleName;
                return msg;
            }
            else {
                for (BaseRulesConfig.ValidationRule validationRule: validationRules.get(validationRuleName))
                    riskGroupTypes.add(validationRule.getRiskGroup());
            }
        }

        if (riskGroupTypes.contains(RiskGroupType.HOLDING) || riskGroupTypes.contains(RiskGroupType.SUBACCOUNTSUMMARY) || riskGroupTypes.contains(RiskGroupType.CASHINFOMAP)) {
            if (Objects.isNull(scheduleRule.getAccountFilters())) {
                msg = "[RiskEngine][FilterConfig] schedule configuration failed to get account filters: scheduleRule=" + scheduleRule.getKey() + ", riskGroupTypes=" + riskGroupTypes.toString();
                return msg;
            }
            Map<String, Set<BaseFilterConfig.AccountFilter>> accountFilters = baseFilterConfig.parseAccountJson(baseFilterConfig.getAccounts());
            for (String accountFilterName : scheduleRule.getAccountFilters()) {
                if (!accountFilters.containsKey(accountFilterName)) {
                    msg = "[RiskEngine][RulesConfig] schedule configuration failed to get account filter: scheduleRule=" + scheduleRule.getKey() + ", accountFilter=" + accountFilterName + ", riskGroupTypes=" + riskGroupTypes.toString();
                    return msg;
                }
            }
        }

        if (riskGroupTypes.contains(RiskGroupType.HOLDING) || riskGroupTypes.contains(RiskGroupType.QUOTE) || riskGroupTypes.contains(RiskGroupType.INSTRUMENTFIELD)) {
            if (Objects.isNull(scheduleRule.getSymbolFilters())) {
                msg = "[RiskEngine][FilterConfig] schedule configuration failed to get symbol filters: scheduleRule=" + scheduleRule.getKey() + ", riskGroupTypes=" + riskGroupTypes.toString();
                return msg;
            }
            Map<String, Set<BaseFilterConfig.SymbolFilter>> symbolFilters = baseFilterConfig.parseSymbolJson(baseFilterConfig.getSymbols());
            for (String symbolFilterName : scheduleRule.getSymbolFilters()) {
                if (!symbolFilters.containsKey(symbolFilterName)) {
                    msg = "[RiskEngine][RulesConfig] schedule configuration failed to get symbol filter: scheduleRule=" + scheduleRule.getKey() + ", symbolFilter=" + symbolFilterName + ", riskGroupTypes=" + riskGroupTypes.toString();
                    return msg;
                }
            }
        }

        if (Objects.isNull(scheduleRule.getSource())) {
            msg = "[RiskEngine][FilterConfig] schedule configuration failed to get source datasource: scheduleRule=" + scheduleRule.getKey();
            return msg;
        }
        if (Objects.isNull(scheduleRule.getTarget())) {
            msg = "[RiskEngine][FilterConfig] schedule configuration failed to get target datasource: scheduleRule=" + scheduleRule.getKey();
            return msg;
        }
        DataSourceType source = scheduleRule.getSource();
        DataSourceType target = scheduleRule.getTarget();
        for (RiskGroupType type: riskGroupTypes) {
            if (!((((type == RiskGroupType.HOLDING || type == RiskGroupType.SUBACCOUNTSUMMARY || type == RiskGroupType.CASHINFOMAP) &&
                        ((source == DataSourceType.BROKER || source == DataSourceType.DATABASE || source == DataSourceType.DEFAULT)  &&
                         (target == DataSourceType.BROKER || target == DataSourceType.DATABASE || target == DataSourceType.DEFAULT))) ||
                   ((type == RiskGroupType.QUOTE) &&
                        ((source == DataSourceType.DATAMASTER || source == DataSourceType.SNAPSHOT || source == DataSourceType.DEFAULT)  &&
                         (target == DataSourceType.DATAMASTER || target == DataSourceType.SNAPSHOT || target == DataSourceType.DEFAULT)))) ||
                   ((type == RiskGroupType.INSTRUMENTFIELD || type == RiskGroupType.SPREAD) &&
                        ((source == DataSourceType.DATAMASTER || source == DataSourceType.DEFAULT)  &&
                         (target == DataSourceType.DATAMASTER || target == DataSourceType.DEFAULT))))) {
                msg = "[RiskEngine][FilterConfig] schedule configuration has invalid dataSourceType for riskGroupType: scheduleRule=" + scheduleRule.getKey() + ", source=" + source + ", target=" + target + ", riskGroupType" + type.name();
                return msg;
            }
        }

        if (Objects.isNull(scheduleRule.getCronSchedules())) {
            msg = "[RiskEngine][FilterConfig] schedule configuration failed to get cronSchedules: scheduleRule=" + scheduleRule.getKey();
            return msg;
        }
        for (String cronSchedule: scheduleRule.getCronSchedules()) {
            if (!CronExpression.isValidExpression(cronSchedule)) {
                msg = "[RiskEngine][FilterConfig] schedule configuration has invalid cronSchedules: scheduleRule=" + scheduleRule.getKey() + ", cronSchedule=" + cronSchedule;
                return msg;
            }
        }
        return null;
    }
}
