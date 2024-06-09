package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.service.BrokerAccountService;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.SubAccount;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/accounts")
public class BrokerAccountController {

    @Autowired
    BrokerAccountService brokerAccountService;

    @RequestMapping(value = "/active", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get active accounts from database")
    public Result<Set<String>> queryActiveBrokerAccounts(@RequestParam(value = "brokerType") BrokerType brokerType) {
        return ResultUtil.success(brokerAccountService.queryActiveBrokerAccounts(brokerType));
    }

    @RequestMapping(value = "/connectable", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "check if accounts are connectable from broker")
    public Result<Map<String, Set<String>>> checkConnectableBrokerAccounts(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                           @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts) {
        return ResultUtil.success(brokerAccountService.queryConnectableBrokerAccounts(brokerType, brokerAccounts));
    }

    @RequestMapping(value = "/checkable", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "check if accounts are included for health check")
    public Result<Map<String, Set<String>>> checkConnectableBrokerAccounts(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                           @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                           @RequestParam(value = "excludeWhiteList", defaultValue = "false") Boolean excludeWhiteList) {
        return ResultUtil.success(brokerAccountService.queryCheckableBrokerAccounts(brokerType, brokerAccounts, excludeWhiteList));
    }

    @RequestMapping(value = "/list/incorrectActiveStatus", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "list all accounts with incorrect isActive status")
    public Result<Map<String, Set<String>>> queryIncorrectBrokerAccountsActiveStatus(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                                     @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                                     @RequestParam(value = "excludeWhiteList", defaultValue = "false") Boolean excludeWhiteList) {
        return ResultUtil.success(brokerAccountService.listIncorrectBrokerAccountActiveStatus(brokerType, brokerAccounts, excludeWhiteList, false));
    }

    @RequestMapping(value = "/reset/activeStatus", method = RequestMethod.POST)
    @CrossOrigin
    @ApiOperation(value = " reset accounts isActive status")
    public Result<Map<String, Set<String>>> resetBrokerAccountsActiveStatus(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                            @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                            @RequestParam(value = "excludeWhiteList", defaultValue = "false") Boolean excludeWhiteList) {
        return ResultUtil.success(brokerAccountService.listIncorrectBrokerAccountActiveStatus(brokerType, brokerAccounts, excludeWhiteList, true));
    }

    @RequestMapping(value = "/subAccount", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get normal sub-accounts from database")
    public Result<SubAccount> queryActiveBrokerAccounts(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                        @RequestParam(value = "brokerAccount") String brokerAccount) {
        return ResultUtil.success(brokerAccountService.queryNormalSubAccount(brokerType, brokerAccount));
    }

    @RequestMapping(value = "/subAccountSummary", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get sub-accounts summary from database")
    public Result<SubAccount> querySubAccountSummary(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                     @RequestParam(value = "brokerAccount") String brokerAccount) {
        return ResultUtil.success(brokerAccountService.querySubAccountFromBroker(brokerType, brokerAccount));
    }
}
