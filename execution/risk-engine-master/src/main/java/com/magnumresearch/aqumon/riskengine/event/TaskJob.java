package com.magnumresearch.aqumon.riskengine.event;

import com.magnumresearch.aqumon.riskengine.config.BaseRulesConfig;
import com.magnumresearch.aqumon.riskengine.config.BaseSchedulesConfig;
import com.magnumresearch.aqumon.riskengine.constants.DataSourceType;
import com.magnumresearch.aqumon.riskengine.constants.RiskGroupType;
import com.magnumresearch.aqumon.riskengine.service.*;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.SubAccountSummary;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TaskJob extends QuartzJobBean {

    @Autowired
    HoldingValidationService holdingValidationService;
    @Autowired
    SubAccountValidationService subAccountValidationService;
    @Autowired
    QuoteValidationService quoteValidationService;
    @Autowired
    InstrumentValidationService instrumentValidationService;
    @Autowired
    HoldingDataService holdingDataService;
    @Autowired
    SubAccountDataService subAccountDataService;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * scheduled job execution details
     *
     * @param jobExecutionContext scheduled job execution context
     */
    @Override
    protected void executeInternal(JobExecutionContext jobExecutionContext) {
        JobDataMap jobDataMap = jobExecutionContext.getMergedJobDataMap();
        String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
        log.info("[RiskEngine][Schedule] execute job: Key=" + jobExecutionContext.getJobDetail().getKey() + ", TimeStamp=" + currentTime);
        runValidation(jobDataMap);
    }

    /**
     * find and run validation codes based on validation type
     *
     * @param jobDataMap job related data
     */
    private void runValidation(JobDataMap jobDataMap) {
        BaseSchedulesConfig.ScheduleRule scheduleRule = (BaseSchedulesConfig.ScheduleRule)jobDataMap.get(TaskScheduleService.SCHEDULERULE);

        Set<String> symbols = scheduleRule.getSymbolNames();
        if (symbols.contains("all") || symbols.contains("All") || symbols.contains("ALL"))
            symbols = null;

        Map<BrokerType, Set<String>> accountMap = scheduleRule.getAccountMap();
        for (Map.Entry<BrokerType, Set<String>> accountEntry: accountMap.entrySet()) {
            if (Objects.nonNull(accountEntry.getValue()) && (accountEntry.getValue().contains("all") || accountEntry.getValue().contains("All") || accountEntry.getValue().contains("ALL")))
                accountEntry.setValue(null);
        }

        System.out.println("test");

        for (BaseRulesConfig.ValidationRule validationRule: scheduleRule.getRuleList()) {
            RiskGroupType riskGroup = validationRule.getRiskGroup();
            String rule = validationRule.getName();

            BrokerType brokerType = null;
            if (riskGroup == RiskGroupType.QUOTE || riskGroup == RiskGroupType.SPREAD )
                brokerType = BrokerType.NEUTRAL;

            if (riskGroup == RiskGroupType.HOLDING || riskGroup == RiskGroupType.SUBACCOUNTSUMMARY || riskGroup == RiskGroupType.CASHINFOMAP) {
                for (Map.Entry<BrokerType, Set<String>> accountEntry : accountMap.entrySet()) {
                    runValidationByRiskGroupType(validationRule.getRiskGroup(), scheduleRule.getSource(), scheduleRule.getTarget(), accountEntry.getKey(), accountEntry.getValue(), symbols, rule, scheduleRule.getEnableSync());
                }
            }
            else if (riskGroup == RiskGroupType.QUOTE || riskGroup == RiskGroupType.SPREAD || riskGroup == RiskGroupType.INSTRUMENTFIELD) {
                if (Objects.nonNull(symbols) && symbols.size() > 0)
                    runValidationByRiskGroupType(validationRule.getRiskGroup(), scheduleRule.getSource(), scheduleRule.getTarget(), brokerType, null, symbols, rule, scheduleRule.getEnableSync());
                else
                    log.warn("[RiskEngine][Schedule] execute job failed as no symbols specified: RiskGroup=" + validationRule.getRiskGroup().name());
            }
            else {
                log.error("[RiskEngine][Schedule] execute job failed as RiskGroupType is not supported in task scheduling: RiskGroup=" + validationRule.getRiskGroup().name());
            }
        }

    }

    /**
     * select and run validation by risk group type
     * @param riskGroupType risk group type
     * @param source        source datasource
     * @param target        target datasource
     * @param brokerType    broker type
     * @param accounts      account ids
     * @param rule          rules for validation
     * @param symbols       symbols list
     */
    private void runValidationByRiskGroupType(RiskGroupType riskGroupType, DataSourceType source, DataSourceType target, BrokerType brokerType, Set<String> accounts, Set<String> symbols, String rule, Boolean enableSync) {
        if (riskGroupType == RiskGroupType.HOLDING)
            runHoldingValidation(source, target, brokerType, accounts, symbols, rule, enableSync);
        else if (riskGroupType == RiskGroupType.SUBACCOUNTSUMMARY)
            runSubAccountSummaryValidation(source, target, brokerType, accounts, rule, enableSync);
        else if (riskGroupType == RiskGroupType.CASHINFOMAP)
            runCashInfoMapValidation(source, target, brokerType, accounts, rule);
        else if (riskGroupType == RiskGroupType.QUOTE)
            runQuoteValidation(source, target, brokerType, symbols, rule);
        else if (riskGroupType == RiskGroupType.INSTRUMENTFIELD)
            runInstFieldValidation(source, target, symbols, rule);
        else if (riskGroupType == RiskGroupType.SPREAD)
            runSpreadValidation(source, target, brokerType, symbols);
    }

    /**
     * schedule holding validation jobs
     * @param source     original data source type
     * @param target     target data source
     * @param brokerType broker type
     * @param accounts   accounts
     * @param symbols    symbols
     * @param rule       to be run
     */
    private void runHoldingValidation(DataSourceType source, DataSourceType target, BrokerType brokerType, Set<String> accounts, Set<String> symbols, String rule, Boolean enableSync) {
        if ((source == DataSourceType.BROKER && target == DataSourceType.DATABASE) || (source == DataSourceType.DATABASE && target == DataSourceType.BROKER))
            if (Objects.nonNull(enableSync) && enableSync)
                holdingDataService.syncHoldings(brokerType, accounts, symbols, rule);
            else
                holdingValidationService.compareCurrentToBrokerHoldings(brokerType, accounts, symbols, rule, false, false);
        else if ((source == DataSourceType.DEFAULT && target == DataSourceType.DATABASE) || (source == DataSourceType.DATABASE && target == DataSourceType.DEFAULT))
            holdingValidationService.compareCurrentToDefaultHoldings(brokerType, accounts, symbols, rule, false, false);
        else if ((source == DataSourceType.DEFAULT && target == DataSourceType.BROKER) || (source == DataSourceType.BROKER && target == DataSourceType.DEFAULT))
            holdingValidationService.compareBrokerToDefaultHoldings(brokerType, accounts, symbols, rule, false, false);
        }

    /**
     * schedule sub-account summary validation jobs
     * @param source     original data source type
     * @param target     target data source
     * @param brokerType broker type
     * @param accounts   accounts
     * @param rule       to be run
     */
    private void runSubAccountSummaryValidation(DataSourceType source, DataSourceType target, BrokerType brokerType, Set<String> accounts, String rule, Boolean enableSync) {
        if ((source == DataSourceType.BROKER && target == DataSourceType.DATABASE) || (source == DataSourceType.DATABASE && target == DataSourceType.BROKER))
            if (Objects.nonNull(enableSync) && enableSync)
                subAccountDataService.syncSubAccountSummary(brokerType, accounts, rule);
            else
                subAccountValidationService.compareDBToBrokerSubAccountSummary(brokerType, accounts, rule, false, false);
        else if ((source == DataSourceType.DEFAULT && target == DataSourceType.DATABASE) || (source == DataSourceType.DATABASE && target == DataSourceType.DEFAULT))
            subAccountValidationService.compareDBToDefaultSubAccountSummary(brokerType, accounts, rule, false, false);
        else if ((source == DataSourceType.DEFAULT && target == DataSourceType.BROKER) || (source == DataSourceType.BROKER && target == DataSourceType.DEFAULT))
            subAccountValidationService.compareBrokerToDefaultSubAccountSummary(brokerType, accounts, rule, false, false);
    }

    /**
     * schedule cashinfomap validation jobs
     * @param source     original data source type
     * @param target     target data source
     * @param brokerType broker type
     * @param accounts   accounts
     * @param rule       to be run
     */
    private void runCashInfoMapValidation(DataSourceType source, DataSourceType target, BrokerType brokerType, Set<String> accounts, String rule) {
        if ((source == DataSourceType.DEFAULT && target == DataSourceType.BROKER) || (source == DataSourceType.BROKER && target == DataSourceType.DEFAULT))
            subAccountValidationService.compareBrokerToDefaultCashInfoMap(brokerType, accounts, rule, false, false);
    }

    /**
     * schedule quote validation jobs
     * @param source     original data source type
     * @param target     target data source
     * @param brokerType broker type
     * @param symbols    symbols
     * @param rule       to be run
     */
    private void runQuoteValidation(DataSourceType source, DataSourceType target, BrokerType brokerType, Set<String> symbols, String rule) {
        if ((source == DataSourceType.DATAMASTER && target == DataSourceType.SNAPSHOT) || (source == DataSourceType.SNAPSHOT && target == DataSourceType.DATAMASTER))
            quoteValidationService.compareDMToSnapshotQuotes(brokerType, symbols, rule, false, false);
        else if ((source == DataSourceType.DEFAULT && target == DataSourceType.DATAMASTER) || (source == DataSourceType.DATAMASTER && target == DataSourceType.DEFAULT))
            quoteValidationService.compareDMToDefaultQuotes(brokerType, symbols, rule, false, true, false);
        else if ((source == DataSourceType.DEFAULT && target == DataSourceType.BROKER) || (source == DataSourceType.BROKER && target == DataSourceType.DEFAULT))
            quoteValidationService.compareDMToDefaultQuotes(brokerType, symbols, rule, false, false, false);
    }

    /**
     * schedule instrument fields validation jobs
     * @param source  original data source type
     * @param target  target data source
     * @param symbols symbols
     */
    private void runInstFieldValidation(DataSourceType source, DataSourceType target, Set<String> symbols, String rule) {
        if ((source == DataSourceType.DEFAULT && target == DataSourceType.DATAMASTER) || (source == DataSourceType.DATAMASTER && target == DataSourceType.DEFAULT))
            instrumentValidationService.validateInstrumentFieldsFromDM(symbols, null, rule, false);
    }

    /**
     * schedule numeric validation jobs
     * @param source  original data source type
     * @param target  target data source
     * @param brokerType broker type
     * @param symbols symbols
     */
    private void runSpreadValidation(DataSourceType source, DataSourceType target, BrokerType brokerType, Set<String> symbols) {
        if ((source == DataSourceType.DEFAULT && target == DataSourceType.DATAMASTER) || (source == DataSourceType.DATAMASTER && target == DataSourceType.DEFAULT))
            quoteValidationService.compareDMToDefaultSpread(brokerType, symbols, true, false);
    }
}
