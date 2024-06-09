package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.quoteengine.pojo.SnapshotResponse;
import com.magnumresearch.aqumon.riskengine.service.SubAccountDataService;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.SubAccountSummary;
import com.magnumresearch.aqumon.trading.pojo.CashInfo;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/subaccounts")
public class SubAccountDataController {

    @Autowired
    SubAccountDataService subAccountDataService;

    @RequestMapping(value = "/summary/broker", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get sub-accounts summary from broker")
    public Result<Map<String, SubAccountSummary>> getSubAccountSummariesFromBroker(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                                  @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts) {
        return ResultUtil.success(subAccountDataService.getSubAccountSummariesFromBroker(brokerType, brokerAccounts));
    }

    @RequestMapping(value = "/summary/db", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get sub-accounts summary from db")
    public Result<Map<String, SubAccountSummary>> getSubAccountSummariesFromDB(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                               @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts) {
        return ResultUtil.success(subAccountDataService.getSubAccountSummariesFromDB(brokerType, brokerAccounts));
    }

    @RequestMapping(value = "/cashinfo/broker", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get cashinfo from broker")
    public Result<Map<String, CashInfo>> getCashInfoMapFromBroker(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                  @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts) {
        return ResultUtil.success(subAccountDataService.getCashInfoMapFromBroker(brokerType, brokerAccounts));
    }

    @RequestMapping(value = "/summary/sync", method = RequestMethod.POST)
    @CrossOrigin
    @ApiOperation(value = "sync subAccountSummary from Broker")
    public Result<List<SubAccountSummary>> syncSubAccountSummary(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                 @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                                 @RequestParam(value = "ruleName",  required = false) String ruleName) {
        return ResultUtil.success(subAccountDataService.syncSubAccountSummary(brokerType, brokerAccounts, ruleName));
    }
}
