package com.magnumresearch.aqumon.riskengine.service;

import com.magnumresearch.aqumon.common.feign.LarkRobotClient;
import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.DataSourceType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.dao.SubAccountSummaryDao;
import com.magnumresearch.aqumon.riskengine.pojo.ComparisonResult;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationData;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationKey;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.constants.CurrencyType;
import com.magnumresearch.aqumon.trading.model.SubAccount;
import com.magnumresearch.aqumon.trading.model.SubAccountSummary;
import com.magnumresearch.aqumon.trading.pojo.CashInfo;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class SubAccountDataService {

    @Autowired
    BrokerAccountService brokerAccountService;
    @Autowired
    SubAccountValidationService subAccountValidationService;
    @Autowired
    BaseConfigService baseConfigService;
    @Autowired
    SubAccountSummaryDao subAccountSummaryDao;
    @Autowired
    LarkRobotClient larkRobotClient;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * get a map of subaccountsummary of broker account from broker
     * @param brokerType broker type
     * @param brokerAccounts broker account
     * @return a map of key and subaccountsummary
     */
    public Map<String, SubAccountSummary> getSubAccountSummariesFromBroker(BrokerType brokerType, Set<String> brokerAccounts) {
        Map<String, SubAccountSummary> resultMap = new HashMap<>();
        for (String account: brokerAccounts) {
            SubAccount subAccountFromBroker = brokerAccountService.querySubAccountFromBroker(brokerType, account);
            SubAccountSummary subAccountSummary = null;
            Long subAccountId = null;
            if (Objects.nonNull(subAccountFromBroker)) {
                subAccountSummary = subAccountFromBroker.getSubAccountSummary();
                subAccountSummary.setSubAccount(subAccountFromBroker);
                if (Objects.nonNull(subAccountFromBroker.getAccount_id()))
                    subAccountId = subAccountFromBroker.getAccount_id();
            }
            ValidationKey validationKey = new ValidationKey(RiskGroupType.SUBACCOUNTSUMMARY, brokerType, account, subAccountId, null);
            String key = validationKey.toString();
            resultMap.put(key, subAccountSummary);
        }
        return resultMap;
    }

    /**
     * get a map of subaccountsummary of broker account from DB
     * @param brokerType broker type
     * @param brokerAccounts broker account
     * @return a map of key and subaccountsummary
     */
    public Map<String, SubAccountSummary> getSubAccountSummariesFromDB(BrokerType brokerType, Set<String> brokerAccounts) {
        Map<String, SubAccountSummary> resultMap = new HashMap<>();
        for (String account: brokerAccounts) {
            SubAccount subAccount = brokerAccountService.queryNormalSubAccount(brokerType, account);
            SubAccountSummary subAccountSummary = null;
            SubAccountSummary subAccountSummaryCopy = null;

            Long subAccountId = Objects.nonNull(subAccount) ? subAccount.getAccount_id() : null;
            List<SubAccountSummary> subAccountSummaries = subAccountSummaryDao.findBySubAccount(subAccount);
            if (subAccountSummaries.size() > 0)
                subAccountSummary = subAccountSummaries.get(0);
            if (Objects.nonNull(subAccountSummary))
                subAccountSummaryCopy = (SubAccountSummary) SerializationUtils.clone(subAccountSummary);

            ValidationKey validationKey = new ValidationKey(RiskGroupType.SUBACCOUNTSUMMARY, brokerType, account, subAccountId, null);
            String key = validationKey.toString();
            resultMap.put(key, subAccountSummaryCopy);
        }
        return resultMap;
    }

    /**
     * get a map of cashinfomap of broker account from Broker
     * @param brokerType broker type
     * @param brokerAccounts broker account
     * @return a map of key and subaccountsummary
     */
    public Map<String, CashInfo> getCashInfoMapFromBroker(BrokerType brokerType, Set<String> brokerAccounts) {
        Map<String, CashInfo> resultMap = new HashMap<>();
        for (String account: brokerAccounts) {
            SubAccount subAccount = brokerAccountService.querySubAccountFromBroker(brokerType, account);
            SubAccountSummary subAccountSummary = Objects.nonNull(subAccount) ? subAccount.getSubAccountSummary() : null;
            Map<CurrencyType, CashInfo> cashInfoMap = Objects.nonNull(subAccountSummary) ? subAccountSummary.getCashInfoMap() : null;
            for (Map.Entry<CurrencyType, CashInfo> entry : cashInfoMap.entrySet()) {
                ValidationKey validationKey = new ValidationKey(RiskGroupType.CASHINFOMAP, brokerType, account, subAccountSummary.getSubAccountId(), entry.getKey().getCurrency());
                String key = validationKey.toString();
                resultMap.put(key, entry.getValue());
            }
        }
        return resultMap;
    }

    /**
     * sync SubAccountSummary objects from broker to database based on rules
     * @param brokerType broker type
     * @param brokerAccounts broker account
     * @param ruleName rule name from configuration
     * @return a list of SubAccountSummary objects that being updated
     */
    public synchronized List<SubAccountSummary> syncSubAccountSummary(BrokerType brokerType, Set<String> brokerAccounts, String ruleName) {
        if (!(brokerType == BrokerType.AYERS || brokerType == BrokerType.IB)) {
            log.error("[RiskEngine][Sync][SubAccountSummary] sync SubAccountSummary is not supported for BrokerType=" + brokerType.name());
            return new ArrayList<>();
        }

        log.info("[RiskEngine][Sync][SubAccountSummary] sync SubAccountSummary from from Broker to current holding table starts: BrokerType=" + brokerType.name() + ", Accounts=" + brokerAccounts + ", RuleName=" + ruleName);
        Set<BaseRulesConfig.ValidationRule> ruleSet = baseConfigService.getRulesFromName(ruleName, RiskGroupType.SUBACCOUNTSUMMARY);
        if (Objects.isNull(brokerAccounts) || brokerAccounts.contains("All") || brokerAccounts.contains("all") || brokerAccounts.contains("ALL"))
            brokerAccounts = brokerAccountService.queryActiveBrokerAccounts(brokerType);

        Set<String> validAccounts = brokerAccountService.queryCheckableBrokerAccounts(brokerType, brokerAccounts, true).get(BrokerAccountService.CHECKABLE);
        if (validAccounts.size() == 0)
            return null;
        
        Map<String, SubAccountSummary> sourceSubAccountSummaries = getSubAccountSummariesFromDB(brokerType, validAccounts);
        Map<String, SubAccountSummary> targetSubAccountSummaries = getSubAccountSummariesFromBroker(brokerType, validAccounts);
        Map<String, Object> sources = new HashMap<>(sourceSubAccountSummaries);
        Map<String, Object> targets = new HashMap<>(targetSubAccountSummaries);
        List<ValidationData> diffData = subAccountValidationService.compareObjectMap(sources, targets, ruleSet, RiskGroupType.SUBACCOUNTSUMMARY, DataSourceType.DATABASE, DataSourceType.BROKER, null, true);

        List<SubAccountSummary> updateSubAccountSummaries = new ArrayList<>();
        List<SubAccountSummary> removeSubAccountSummaries = new ArrayList<>();
        for (ValidationData validationData: diffData) {
            String brokerAccount = Objects.nonNull(validationData.getValidationKey()) ? validationData.getValidationKey().getBrokerAccount() : null;
            for (Map.Entry<String, ComparisonResult> entry: validationData.getFieldData().entrySet()) {
                SubAccountSummary currentSubAccountSummary = (SubAccountSummary) entry.getValue().getSource();
                SubAccountSummary brokerSubAccountSummary = (SubAccountSummary) entry.getValue().getTarget();
                if (Objects.isNull(brokerSubAccountSummary) && Objects.nonNull(currentSubAccountSummary)) {
                    removeSubAccountSummaries.add(currentSubAccountSummary);
                    log.warn("[RiskEngine][Sync][SubAccountSummary] SubAccountSummary to be removed: brokerType=" + brokerType + ", brokerAccount=" + brokerAccount + ", currency=" + currentSubAccountSummary.getBaseCurrency() + ", cash=" + currentSubAccountSummary.getCash() + ", equities=" + currentSubAccountSummary.getEquities());
                }
                else if (Objects.nonNull(brokerSubAccountSummary.getSubAccount())) {
                    updateSubAccountSummaries.add(brokerSubAccountSummary);
                    log.warn("[RiskEngine][Sync][SubAccountSummary] SubAccountSummary to be updated: brokerType=" + brokerType + ", brokerAccount=" + brokerAccount + ", currency=" + brokerSubAccountSummary.getBaseCurrency() + ", cash=" + brokerSubAccountSummary.getCash() + ", equities=" + brokerSubAccountSummary.getEquities());
                }
            }
        }
        for (SubAccountSummary subAccountSummary: removeSubAccountSummaries)
            subAccountSummaryDao.deleteById(subAccountSummary.getId());
        List<SubAccountSummary> results = subAccountSummaryDao.saveAll(updateSubAccountSummaries);
        log.info("[RiskEngine][Sync][SubAccountSummary] sync SubAccountSummary completed: UpdatedRecordsSize=" + updateSubAccountSummaries.size() + ", RemovedRecordsSize=" + removeSubAccountSummaries.size() + ", BrokerType=" + brokerType.name() + ", Accounts=" + validAccounts + ", RuleName=" + ruleName);

        String updateMessage = buildDingDingMessage(updateSubAccountSummaries, "Update");
        String removeMessage = buildDingDingMessage(removeSubAccountSummaries, "Remove");
        larkRobotClient.sendMessage("[RiskEngine] SubAccountSummary sync complete: broker type=" + brokerType.name() + ", total updated records=" + (updateSubAccountSummaries.size() + removeSubAccountSummaries.size()) + "\n" + updateMessage + removeMessage);
        return results;
    }

    /**
     * combine a list of sync data into a single dingding message
     * @param subAccountSummaries a list of updated subAccountSummary object for dingding alert
     * @return a string of dingding message
     */
    public String buildDingDingMessage(List<SubAccountSummary> subAccountSummaries, String action) {
        StringBuilder message = new StringBuilder();
        int index = 0;
        if (subAccountSummaries.size() > 0) {
            message.append(action).append(" records: size=").append(subAccountSummaries.size()).append("\n");
            for (SubAccountSummary subAccountSummary: subAccountSummaries) {
                if (index < 10) {
                    BrokerType brokerType = Objects.nonNull(subAccountSummary.getSubAccount()) ? subAccountSummary.getSubAccount().getBrokerType() : null;
                    String brokerAccount = Objects.nonNull(subAccountSummary.getSubAccount()) ? subAccountSummary.getSubAccount().getBrokerAccount() : null;
                    message.append("#").append(index + 1).append(": broker=").append(brokerType).append(", account=").append(brokerAccount).
                            append(", currency=").append(subAccountSummary.getBaseCurrency()).append(", cash=").append(subAccountSummary.getCash()).append("\n");
                    index = index + 1;
                }
                else
                    break;
            }
            message.append("...\n");
        }
        return message.toString();
    }
}
