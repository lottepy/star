package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.riskengine.service.SubAccountValidationService;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/subaccount/compare")
public class SubAccountValidationController {

    @Autowired
    SubAccountValidationService subAccountValidationService;

    @RequestMapping(value = "/summary/databaseToBroker", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "validate sub-accounts between broker and DB")
    public Result<ValidationResult> compareSubAccountSummaryToBroker(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                     @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                     @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                     @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                     @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(subAccountValidationService.compareDBToBrokerSubAccountSummary(brokerType, brokerAccounts, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/summary/databaseToDefault", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "validate sub-accounts between database and default values")
    public Result<ValidationResult> compareDBToDefaultSubAccountSummary(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                        @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                        @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                        @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                        @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(subAccountValidationService.compareDBToDefaultSubAccountSummary(brokerType, brokerAccounts, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/summary/brokerToDefault", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "validate sub-accounts between broker and default values")
    public Result<ValidationResult> compareBrokerToDefaultSubAccountSummary(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                            @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                            @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                            @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                            @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(subAccountValidationService.compareBrokerToDefaultSubAccountSummary(brokerType, brokerAccounts, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/cashinfo/brokerToDefault", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "validate sub-accounts cashinfo between broker and default values")
    public Result<ValidationResult> compareBrokerToDefaultCashInfoMap(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                      @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                      @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                      @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                      @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(subAccountValidationService.compareBrokerToDefaultCashInfoMap(brokerType, brokerAccounts, ruleName, includeObjects, useCache));
    }
}
