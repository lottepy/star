package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.common.feign.LarkRobotClient;
import com.magnumresearch.aqumon.riskengine.adapter.BaseBrokerAdapter;
import com.magnumresearch.aqumon.riskengine.config.BaseFilterConfig;
import com.magnumresearch.aqumon.riskengine.dao.AccountDao;
import com.magnumresearch.aqumon.riskengine.dao.BrokerAccountConfigDao;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.constants.SubAccountType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.Account;
import com.magnumresearch.aqumon.trading.model.BrokerAccountConfig;
import com.magnumresearch.aqumon.trading.model.SubAccount;
import com.sun.corba.se.impl.oa.toa.TOA;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;

@Service
public class BrokerAccountService {

    @Autowired
    AccountDao accountDao;
    @Autowired
    BrokerAccountConfigDao brokerAccountConfigDao;
    @Autowired
    BrokerAdapterService brokerAdapterService;
    @Autowired
    LarkRobotClient larkRobotClient;
    @Autowired
    BaseFilterConfig baseFilterConfig;
    @Autowired
    BrokerAccountService brokerAccountService;

    public static final String CONNECTABLE = "CONNECTABLE";
    public static final String UNCONNECTABLE ="UNCONNECTABLE";
    public static final String CHECKABLE = "CHECKABLE";
    public static final String UNCHECKABLE ="UNCHECKABLE";
    public static final String TOACTIVE = "TO_ACTIVE";
    public static final String TOINACTIVE ="TO_INACTIVE";

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * query all active accounts from brokers
     * @param brokerType broker type
     * @return a set of broker accounts
     */
    public Set<String> queryActiveBrokerAccounts(BrokerType brokerType) {
        List<Account> accountObjs = accountDao.findAllByBrokerType(brokerType);
        Set<String> activeAccounts = new HashSet<>();
        for (Account account: accountObjs) {
            if (account.isActive())
                activeAccounts.add(account.getBrokerAccount());
        }
        return activeAccounts;
    }

    /**
     * Check if accounts are connectable
     * @param brokerType broker type
     * @param brokerAccounts input account list
     * @return a map of connectable/non_connectable accounts
     */
    public Map<String, Set<String>> queryConnectableBrokerAccounts(BrokerType brokerType, Set<String> brokerAccounts) {
        if (Objects.isNull(brokerAccounts) || brokerAccounts.contains("All") || brokerAccounts.contains("all") || brokerAccounts.contains("ALL"))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);
        Map<String, Set<String>> results = new HashMap<>();
        Set<String> connectableAccounts = new HashSet<>();
        Set<String> unconnectableAccounts = new HashSet<>();
        for (String account: brokerAccounts) {
            if (isBrokerAccountConnectable(brokerType, account))
                connectableAccounts.add(account);
            else
                unconnectableAccounts.add(account);
        }
        results.put(CONNECTABLE, connectableAccounts);
        results.put(UNCONNECTABLE, unconnectableAccounts);
        return results;
    }

    /**
     * set invalid broker accounts as inactive
     * @param brokerType broker type
     * @param accounts input account list
     * @param excludeWhiteList exclude accounts being whitelisted
     * @param toReset to reset the account status
     * @return a map of invalid accounts to be deactivated
     */
    public Map<String, Set<String>> listIncorrectBrokerAccountActiveStatus(BrokerType brokerType, Set<String> accounts, Boolean excludeWhiteList, Boolean toReset) {
        Map<String, Set<Account>> accountMap = queryIncorrectBrokerAccountActiveStatus(brokerType, accounts, excludeWhiteList);
        Map<String, Set<String>> results = new HashMap<>();
        Set<String> toInactiveAccounts = new HashSet<>();
        Set<String> toActiveAccounts = new HashSet<>();
        for (Account account: accountMap.get(TOINACTIVE))
            toInactiveAccounts.add(account.getBrokerAccount());
        for (Account account: accountMap.get(TOACTIVE))
            toActiveAccounts.add(account.getBrokerAccount());
        results.put(TOINACTIVE, toInactiveAccounts);
        results.put(TOACTIVE, toActiveAccounts);
        if (toReset) {
            setBrokerAccountActiveStatus(accountMap.get(TOINACTIVE), false);
            setBrokerAccountActiveStatus(accountMap.get(TOACTIVE), true);
            larkRobotClient.sendMessage("[RiskEngine] reset active status for accounts: BrokerType=" + brokerType + ", ToInactiveAccountSize:" + toInactiveAccounts.size() + ", ToActiveAccountSize:" + toActiveAccounts.size() + "\n"
                    + TOINACTIVE + ":\n" + String.join(", ", toInactiveAccounts) + "\n" + TOACTIVE + ":\n" + String.join(", ", toActiveAccounts));
        }
        return results;
    }

    /**
     * query incorrect broker account active status
     * @param brokerType broker type
     * @param accountList input account list
     * @param excludeWhiteList true to include whitelist, false otherwise
     * @return a set of accounts with invalid status
     */
    public Map<String, Set<Account>> queryIncorrectBrokerAccountActiveStatus(BrokerType brokerType, Set<String> accountList, Boolean excludeWhiteList) {
        List<Account> accountObjs = accountDao.findAllByBrokerType(brokerType);
        Map<String, Set<Account>> results = new HashMap<>();
        Set<Account> toActives = new HashSet<>();
        Set<Account> toInActives = new HashSet<>();
        for (Account account: accountObjs) {
            if (!excludeWhiteList || (excludeWhiteList && !isBrokerAccountWhiteListed(brokerType, account.getBrokerAccount()))) {
                if (Objects.isNull(accountList) || accountList.size() == 0 || accountList.contains(account.getBrokerAccount())) {
                    if (account.isActive() && !isBrokerAccountConnectable(brokerType, account.getBrokerAccount()))
                        toInActives.add(account);
                    else if (!account.isActive() && isBrokerAccountConnectable(brokerType, account.getBrokerAccount()))
                        toActives.add(account);
                }
            }
        }
        results.put(TOINACTIVE, toInActives);
        results.put(TOACTIVE, toActives);
        return results;
    }

    /**
     * set broker account active status
     * @param accounts a list of account object from database
     * @param toActive true to set active status to true, false to set active status to false
     */
    @Transactional
    public void setBrokerAccountActiveStatus(Set<Account> accounts, Boolean toActive) {
        for (Account account: accounts) {
            account.setActive(toActive);
        }
        accountDao.saveAll(accounts);
    }

    /**
     * query all sub accounts for broker type and account
     * @param brokerType broker type
     * @param brokerAccount broker account
     * @return sub accounts for this broker type and account
     */
    public List<SubAccount> querySubAccounts(BrokerType brokerType, String brokerAccount) {
        List<SubAccount> subAccountList = new ArrayList<>();
        List<Account> accountList = accountDao.findByBrokerTypeAndBrokerAccount(brokerType, brokerAccount);
        for (Account account : accountList) {
            subAccountList.addAll(account.getSubAccountSet());
        }
        return subAccountList;
    }

    /**
     * query normal sub account for broker type and account
     * @param brokerType broker type
     * @param brokerAccount broker account
     * @return normal sub accounts for this broker type and account
     */
    public SubAccount queryNormalSubAccount(BrokerType brokerType, String brokerAccount) {
        if (brokerType == BrokerType.KR) {
            log.error("[RiskEngine][Broker] KR cannot find subaccount: BrokerAccount=" + brokerAccount);
            return null;
        }
        List<SubAccount> subAccountList = querySubAccounts(brokerType, brokerAccount);
        for (SubAccount subAccount : subAccountList) {
            if (subAccount.getSubAccountType() == SubAccountType.NORMAL) {
                return subAccount;
            }
        }
        return null;
    }

    /**
     * query subaccount summary from broker
     * @param brokerType broker type
     * @param brokerAccount broker account
     * @return subaccount onject
     */
    public SubAccount querySubAccountFromBroker(BrokerType brokerType, String brokerAccount) {
        BaseBrokerAdapter adapter;
        try {
            adapter = brokerAdapterService.getAdapter(brokerType);
            SubAccount subAccount = queryNormalSubAccount(brokerType, brokerAccount);
            if (Objects.isNull(subAccount))
                return null;
            subAccount = adapter.querySubAccountSummary(subAccount);
            return subAccount;
        } catch (TradingException tradingException) {
            log.error("[RiskEngine][Broker] Broker Adapter failed to query: Exception=", tradingException);
            return null;
        }
    }


    /**
     * check if an broker account is whitelisted
     * @param brokerType broker type
     * @param brokerAccount broker account
     * @return true if the broker account is whitelisted
     */
    public boolean isBrokerAccountWhiteListed(BrokerType brokerType, String brokerAccount) {
        if (brokerType == BrokerType.IB) {
            Map<String, Set<BaseFilterConfig.PortFilter>> portFilterMap = baseFilterConfig.parsePortJson(baseFilterConfig.getPorts());
            List<BrokerAccountConfig> accountConfigs = brokerAccountConfigDao.findAllByBrokerAccount(brokerAccount);
            for (Map.Entry<String, Set<BaseFilterConfig.PortFilter>> entry : portFilterMap.entrySet()) {
                for (BaseFilterConfig.PortFilter portFilter : entry.getValue()) {
                    log.info("[RiskEngine][Broker][PortFilter] Adapter port whitelist: size=" + accountConfigs.size() + ", ips=" + portFilter.getPorts());
                    for (String port : portFilter.getPorts()) {
                        for (BrokerAccountConfig accountConfig : accountConfigs) {
                            if (port.equalsIgnoreCase(String.valueOf(accountConfig.getPort()))) {
                                log.error("[RiskEngine][Broker] Broker Account is valid as adapter IP address is whitelisted: account=" + brokerType + "|" + brokerAccount + ", port=" + port);
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    /**
     * check if an broker account is connectable
     * @param brokerType broker type
     * @param brokerAccount broker account
     * @return true if the broker account is connectable
     */
    public boolean isBrokerAccountConnectable(BrokerType brokerType, String brokerAccount) {
        BaseBrokerAdapter adapter;
        try {
            adapter = brokerAdapterService.getAdapter(brokerType);
        } catch (TradingException tradingException) {
            log.error("[RiskEngine][Broker] Broker Adapter failed to get Adapter: account=" + brokerType + "|" + brokerAccount + ", Exception=", tradingException);
            return false;
        }
        try {
            return adapter.isAccountValid(brokerAccount);
        } catch (TradingException tradingException) {
            log.error("[RiskEngine][Broker] Broker Adapter failed to connect on broker account=" + brokerAccount);
            return false;
        }
    }

    /**
     * check if an broker account included for health check
     * @param brokerType broker type
     * @param brokerAccounts broker accounts
     * @param excludeWhiteList exclude accounts if this is in whitelist
     * @return a map of broker accounts included/not_included for health check
     */
    public Map<String, Set<String>> queryCheckableBrokerAccounts(BrokerType brokerType, Set<String> brokerAccounts, Boolean excludeWhiteList) {
        if (Objects.isNull(brokerAccounts) || brokerAccounts.contains("All") || brokerAccounts.contains("all") || brokerAccounts.contains("ALL"))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);
        Map<String, Set<String>> results = new HashMap<>();
        Set<String> checkable = new HashSet<>();
        Set<String> uncheckable = new HashSet<>();
        for (String brokerAccount : brokerAccounts) {
            if (excludeWhiteList && isBrokerAccountWhiteListed(brokerType, brokerAccount))
                uncheckable.add(brokerAccount);
            else if (!isBrokerAccountConnectable(brokerType, brokerAccount))
                uncheckable.add(brokerAccount);
            else
                checkable.add(brokerAccount);
        }
        results.put(CHECKABLE, checkable);
        results.put(UNCHECKABLE, uncheckable);
        return results;
    }
}
