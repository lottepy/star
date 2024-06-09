package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.riskengine.service.HoldingValidationService;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/holdings/compare")
public class HoldingValidationController {

    @Autowired
    HoldingValidationService holdingValidationService;

    @RequestMapping(value = "/currentDBToHistoryDB", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "validate holdings between current DB and a given date snapshot")
    public Result<ValidationResult> compareCurrentToHistoricalHoldings(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                       @RequestParam(value = "date") @DateTimeFormat(pattern = "yyyyMMdd") Date date,
                                                                       @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                       @RequestParam(value = "symbols",  required = false) Set<String> symbols,
                                                                       @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                       @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                       @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(holdingValidationService.compareCurrentToHistoricalHoldings(brokerType, date, brokerAccounts, symbols, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/historyDBToHistoryDB", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "validate holdings between current DB and a given date snapshot")
    public Result<ValidationResult> compareHistoricalHoldings(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                              @RequestParam(value = "date1") @DateTimeFormat(pattern = "yyyyMMdd") Date date1,
                                                              @RequestParam(value = "date2") @DateTimeFormat(pattern = "yyyyMMdd") Date date2,
                                                              @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                              @RequestParam(value = "symbols",  required = false) Set<String> symbols,
                                                              @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                              @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                              @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(holdingValidationService.compareHistoricalHoldings(brokerType, date1, date2, brokerAccounts, symbols, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/brokerToCurrentDB", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "compare holdings between broker and current DB")
    public Result<ValidationResult> compareCurrentToBrokerHoldings(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                   @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                   @RequestParam(value = "symbols",  required = false) Set<String> symbols,
                                                                   @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                   @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                   @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(holdingValidationService.compareCurrentToBrokerHoldings(brokerType, brokerAccounts, symbols, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/brokerToHistoryDB", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "compare holdings between broker and historical DB")
    public Result<ValidationResult> compareHistoryToBrokerHoldings(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                   @RequestParam(value = "date") @DateTimeFormat(pattern = "yyyyMMdd") Date date,
                                                                   @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                   @RequestParam(value = "symbols",  required = false) Set<String> symbols,
                                                                   @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                   @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                   @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(holdingValidationService.compareHistoryToBrokerHoldings(brokerType, date, brokerAccounts, symbols, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/brokerToDefault", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "compare holdings between broker and default value")
    public Result<ValidationResult> compareBrokerToDefaultHoldings(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                   @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                   @RequestParam(value = "symbols",  required = false) Set<String> symbols,
                                                                   @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                   @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                   @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(holdingValidationService.compareBrokerToDefaultHoldings(brokerType, brokerAccounts, symbols, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/currentDBToDefault", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "compare holdings between current DB and default value")
    public Result<ValidationResult> compareCurrentToDefaultHoldings(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                    @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                    @RequestParam(value = "symbols",  required = false) Set<String> symbols,
                                                                    @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                    @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                    @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(holdingValidationService.compareCurrentToDefaultHoldings(brokerType, brokerAccounts, symbols, ruleName, includeObjects, useCache));
    }
}
