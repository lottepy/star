package com.magnumresearch.aqumon.riskengine.config;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.magnumresearch.aqumon.common.feign.DataMasterFeignClient;
import com.magnumresearch.aqumon.riskengine.dao.AccountDao;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.Account;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;

import java.util.*;

@Data
@RefreshScope
@ConfigurationProperties(prefix = "risk.health.filter")
public class BaseFilterConfig {
    public String accounts;
    public String fields;
    public String symbols;
    public String ports;
    public static final String ALL = "All";

    @Autowired
    AccountDao accountDao;
    @Autowired
    DataMasterFeignClient dataMasterFeignClient;

    static Logger log = LoggerFactory.getLogger(BaseFilterConfig.class);

    @Data
    public static class AccountFilter {
        String key;
        String name;
        BrokerType broker;
        Set<String> accounts;
    }

    @Data
    public static class SymbolFilter {
        String key;
        String name;
        Set<String> symbols;
    }

    @Data
    public static class FieldFilter {
        String key;
        String name;
        Set<String> fields;
    }

    @Data
    public static class PortFilter {
        String key;
        String name;
        Set<String> ports;
    }

    /**
     * parse json string from config into a map of config name and accountfilter objects
     * @param jsonStr json string from config
     * @return a map of config name and accountfilter objects
     */
    public Map<String, Set<AccountFilter>> parseAccountJson(String jsonStr) {
        Map<String, Set<AccountFilter>> accountFilterMap = new HashMap<>();
        if (Objects.isNull(jsonStr) || jsonStr.trim().length() == 0)
            return accountFilterMap;

        List<AccountFilter> accountFilters;
        try {
            accountFilters = new Gson().fromJson(jsonStr, new TypeToken<List<AccountFilter>>() {}.getType());
        } catch (JsonSyntaxException jsonSyntaxException) {
            log.error("[RiskEngine][FilterConfig] account filter configuration failed to parse: accountFilters=" + jsonStr, jsonSyntaxException);
            return accountFilterMap;
        }

        for (AccountFilter accountFilter: accountFilters) {
            if (Objects.nonNull(accountFilter)) {
                accountFilter.setKey(buildAccountFilterKey(accountFilter));
                if (accountFilterMap.containsKey(accountFilter.getName()))
                    accountFilterMap.get(accountFilter.getName()).add(accountFilter);
                else {
                    Set<AccountFilter> accountList = new HashSet<>();
                    accountList.add(accountFilter);
                    accountFilterMap.put(accountFilter.getName(), accountList);
                }
            }
        }
        return  accountFilterMap;
    }

    /**
     * parse json string from config into a map of config name and symbolFilter objects
     * @param jsonStr json string from config
     * @return a map of config name and symbolFilter objects
     */
    public Map<String, Set<SymbolFilter>> parseSymbolJson(String jsonStr) {
        Map<String, Set<SymbolFilter>> symbolFilterHashMap = new HashMap<>();
        if (Objects.isNull(jsonStr) || jsonStr.trim().length() == 0)
            return symbolFilterHashMap;

        List<SymbolFilter> symbolFilters;
        try {
            symbolFilters = new Gson().fromJson(jsonStr, new TypeToken<List<SymbolFilter>>() {}.getType());
        } catch (JsonSyntaxException jsonSyntaxException) {
            log.error("[RiskEngine][FilterConfig] symbol filter configuration failed to parse: symbolFilters=" + jsonStr, jsonSyntaxException);
            return symbolFilterHashMap;
        }

        for (SymbolFilter symbolFilter: symbolFilters) {
            if (Objects.nonNull(symbolFilter)) {
                symbolFilter.setKey(buildSymbolFilterKey(symbolFilter));
                if (symbolFilterHashMap.containsKey(symbolFilter.getName()))
                    symbolFilterHashMap.get(symbolFilter.getName()).add(symbolFilter);
                else {
                    Set<SymbolFilter> symbolList = new HashSet<>();
                    symbolList.add(symbolFilter);
                    symbolFilterHashMap.put(symbolFilter.getName(), symbolList);
                }
            }
        }
        return symbolFilterHashMap;
    }

    /**
     * parse json string from config into a map of config name and fieldFilter objects
     * @param jsonStr json string from config
     * @return a map of config name and fieldFilter objects
     */
    public Map<String, Set<FieldFilter>> parseFieldJson(String jsonStr) {
        Map<String, Set<FieldFilter>> fieldFilterHashMap = new HashMap<>();
        if (Objects.isNull(jsonStr) || jsonStr.trim().length() == 0)
            return fieldFilterHashMap;

        List<FieldFilter> fieldFilters;
        try {
            fieldFilters = new Gson().fromJson(jsonStr, new TypeToken<List<FieldFilter>>() {}.getType());
        } catch (JsonSyntaxException jsonSyntaxException) {
            log.error("[RiskEngine][FilterConfig] field filter configuration failed to parse: fieldFilters=" + jsonStr, jsonSyntaxException);
            return fieldFilterHashMap;
        }

        for (FieldFilter fieldFilter: fieldFilters) {
            if (Objects.nonNull(fieldFilter)) {
                fieldFilter.setKey(buildFieldFilterKey(fieldFilter));
                if (fieldFilterHashMap.containsKey(fieldFilter.getName()))
                    fieldFilterHashMap.get(fieldFilter.getName()).add(fieldFilter);
                else {
                    Set<FieldFilter> fieldList = new HashSet<>();
                    fieldList.add(fieldFilter);
                    fieldFilterHashMap.put(fieldFilter.getName(), fieldList);
                }
            }
        }
        return fieldFilterHashMap;
    }

    /**
     * parse json string from config into a map of config name and portFilter objects
     * @param jsonStr json string from config
     * @return a map of config name and portFilter objects
     */
    public Map<String, Set<PortFilter>> parsePortJson(String jsonStr) {
        Map<String, Set<PortFilter>> portFilterHashMap = new HashMap<>();
        if (Objects.isNull(jsonStr) || jsonStr.trim().length() == 0)
            return portFilterHashMap;

        List<PortFilter> portFilters;
        try {
            portFilters = new Gson().fromJson(jsonStr, new TypeToken<List<PortFilter>>() {}.getType());
        } catch (JsonSyntaxException jsonSyntaxException) {
            log.error("[RiskEngine][FilterConfig] port filter configuration failed to parse: symbolFilters=" + jsonStr, jsonSyntaxException);
            return portFilterHashMap;
        }

        for (PortFilter portFilter: portFilters) {
            if (Objects.nonNull(portFilter)) {
                portFilter.setKey(buildPortFilterKey(portFilter));
                if (portFilterHashMap.containsKey(portFilter.getName()))
                    portFilterHashMap.get(portFilter.getName()).add(portFilter);
                else {
                    Set<PortFilter> ipList = new HashSet<>();
                    ipList.add(portFilter);
                    portFilterHashMap.put(portFilter.getName(), ipList);
                }
            }
        }
        return portFilterHashMap;
    }

    /**
     * generate key for account filter object
     * @param accountFilter account filter object
     * @return key for account filter object
     */
    public String buildAccountFilterKey(AccountFilter accountFilter) {
        String brokerName = Objects.nonNull(accountFilter.getBroker()) ? accountFilter.getBroker().name() : "NULL";
        String accounts;
        if (Objects.nonNull(accountFilter.getAccounts()) && accountFilter.getAccounts().size() > 0) {
            if (accountFilter.getAccounts().contains("all") || accountFilter.getAccounts().contains("All") || accountFilter.getAccounts().contains("ALL"))
                accounts = ALL;
            else
                accounts = String.join(",", accountFilter.getAccounts());
        }
        else {
            accounts = "NULL";
        }
        return accountFilter.getName() + "|" + brokerName + "|" + accounts;
    }

    /**
     * generate key for symbol filter object
     * @param symbolFilter symbol filter object
     * @return key for symbol filter object
     */
    public String buildSymbolFilterKey(SymbolFilter symbolFilter) {
        String symbols;
        if (Objects.nonNull(symbolFilter.getSymbols()) && symbolFilter.getSymbols().size() > 0) {
            if (symbolFilter.getSymbols().contains("all") || symbolFilter.getSymbols().contains("All") || symbolFilter.getSymbols().contains("ALL"))
                symbols = ALL;
            else
                symbols = String.join(",", symbolFilter.getSymbols());
        }
        else {
            symbols = "NULL";
        }
        return symbolFilter.getName() + "|" + symbols;
    }

    /**
     * generate key for field filter object
     * @param fieldFilter field filter object
     * @return key for field filter object
     */
    public String buildFieldFilterKey(FieldFilter fieldFilter) {
        String fields = Objects.nonNull(fieldFilter.getFields()) && fieldFilter.getFields().size() > 0  ? String.join(",", fieldFilter.getFields()) : "NULL";
        return fieldFilter.getName() + "|" + fields;
    }

    /**
     * generate key for port filter object
     * @param portFilter port filter object
     * @return key for port filter object
     */
    public String buildPortFilterKey(PortFilter portFilter) {
        String fields = Objects.nonNull(portFilter.getPorts()) && portFilter.getPorts().size() > 0  ? String.join(",", portFilter.getPorts()) : "NULL";
        return portFilter.getName() + "|" + fields;
    }

    /**
     * validate account filter object
     * @param accountFilter account filter object
     * @return error message for account filter object validation
     */
    public String validateAccountFilter(AccountFilter accountFilter) {
        String msg;
        if (Objects.isNull(accountFilter.getName())) {
            msg = "[RiskEngine][FilterConfig] account filter configuration failed to get filter name: accountFilter=" + accountFilter.getKey();
            return msg;
        }
        if (Objects.isNull(accountFilter.getBroker())) {
            msg = "[RiskEngine][FilterConfig] account filter configuration failed to get broker: accountFilter=" + accountFilter.getKey();
            return msg;
        }
        if (Objects.isNull(accountFilter.getAccounts()) || accountFilter.getAccounts().size() <= 0) {
            msg = "[RiskEngine][FilterConfig] account filter configuration failed to get account lists: accountFilter=" + accountFilter.getKey();
            return msg;
        }
        for (String account: accountFilter.getAccounts()) {
            if (!(account.equalsIgnoreCase("all") || account.equalsIgnoreCase("All") || account.equalsIgnoreCase("ALL"))) {
                List<Account> accountList = accountDao.findByBrokerTypeAndBrokerAccount(accountFilter.getBroker(), account);
                if (accountList.size() <= 0) {
                    msg = "[RiskEngine][FilterConfig] account filter configuration has invalid account: accountFilter=" + accountFilter.getKey() + ", account=" + account;
                    return msg;
                }
            }
        }
        return null;
    }

    /**
     * validate symbol filter object
     * @param symbolFilter symbol filter object
     * @return error message for symbol filter object validation
     */
    public String validateSymbolFilter(SymbolFilter symbolFilter) {
        String msg;
        if (Objects.isNull(symbolFilter.getName())) {
            msg = "[RiskEngine][FilterConfig] symbol filter configuration failed to get filter name: symbolFilter=" + symbolFilter.getKey();
            return msg;
        }
        if (Objects.isNull(symbolFilter.getSymbols()) || symbolFilter.getSymbols().size() <= 0) {
            msg = "[RiskEngine][FilterConfig] symbol filter configuration failed to get symbol lists: symbolFilter=" + symbolFilter.getKey();
            return msg;
        }
        for (String symbol: symbolFilter.getSymbols()) {
            if (!(symbol.equalsIgnoreCase("all") || symbol.equalsIgnoreCase("All") || symbol.equalsIgnoreCase("ALL"))) {
                List<String> symbolList = new ArrayList<>();
                symbolList.add(symbol);
                if (dataMasterFeignClient.getInstrumentInfo(symbolList).getData().size() <= 0) {
                    msg = "[RiskEngine][FilterConfig] symbol filter configuration failed to get symbol from datamaster: symbolFilter=" + symbolFilter.getKey() + ", symbol=" + symbol;
                    return msg;
                }
            }
        }
        return null;
    }

    /**
     * validate field filter object
     * @param fieldFilter field filter object
     * @return error message for field filter object validation
     */
    public String validateFieldFilter(FieldFilter fieldFilter) {
        String msg;
        if (Objects.isNull(fieldFilter.getName())) {
            msg = "[RiskEngine][FilterConfig] field filter configuration failed to get filter name: fieldFilter=" + fieldFilter.getKey();
            return msg;
        }
        if (Objects.isNull(fieldFilter.getFields()) || fieldFilter.getFields().size() <= 0) {
            msg = "[RiskEngine][FilterConfig] symbol filter configuration failed to get field lists: fieldFilter=" + fieldFilter.getKey();
            return msg;
        }
        return null;
    }

    /**
     * validate port filter object
     * @param portFilter port filter object
     * @return error message for port filter object validation
     */
    public String validatePortFilter(PortFilter portFilter) {
        String msg;
        if (Objects.isNull(portFilter.getName())) {
            msg = "[RiskEngine][FilterConfig] port filter configuration failed to get filter name: portFilter=" + portFilter.getKey();
            return msg;
        }
        if (Objects.isNull(portFilter.getPorts()) || portFilter.getPorts().size() <= 0) {
            msg = "[RiskEngine][FilterConfig] port filter configuration failed to get field lists: portFilter=" + portFilter.getKey();
            return msg;
        }

        return null;
    }
}
