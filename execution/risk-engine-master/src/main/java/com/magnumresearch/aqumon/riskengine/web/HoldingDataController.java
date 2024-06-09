package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.service.HoldingDataService;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.model.Holding;
import com.magnumresearch.aqumon.trading.model.HoldingHistory;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/holdings")
public class HoldingDataController {

    @Autowired
    HoldingDataService holdingDataService;

    @RequestMapping(value = "/database/current", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get current holdings from database")
    public Result<List<Holding>> getHoldingsFromDB(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                   @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                   @RequestParam(value = "symbols",  required = false) Set<String> symbols ) {
        return ResultUtil.success(holdingDataService.getCurrentHoldingsFromDB(brokerType, brokerAccounts, symbols));
    }


    @RequestMapping(value = "/database/history", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get historical holdings from database")
    public Result<List<HoldingHistory>> getHoldingsFromDB(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                          @RequestParam(value = "date") @DateTimeFormat(pattern = "yyyyMMdd") Date date,
                                                          @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                          @RequestParam(value = "symbols",  required = false) Set<String> symbols) {
        return ResultUtil.success(holdingDataService.getHistoricalHoldingsFromDB(brokerType, date, brokerAccounts, symbols));
    }

    @RequestMapping(value = "/database/sync", method = RequestMethod.POST)
    @CrossOrigin
    @ApiOperation(value = "get current holdings from database")
    public Result<List<Holding>> syncHoldingsFromDB(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                    @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                    @RequestParam(value = "symbols", required = false) Set<String> symbols,
                                                    @RequestParam(value = "ruleName",  required = false) String ruleName) {
        return ResultUtil.success(holdingDataService.syncHoldings(brokerType, brokerAccounts, symbols, ruleName));
    }

    @RequestMapping(value = "/broker", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get holdings from broker")
    public Result<List<Holding>> getHoldingsByBrokerAccount(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                            @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                            @RequestParam(value = "symbols",  required = false) Set<String> symbols) {
        return ResultUtil.success(holdingDataService.getHoldingsFromBroker(brokerType, brokerAccounts, symbols));
    }

    @RequestMapping(value = "/findDuplications", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "find duplicated holdings in databse")
    public Result<Map<String, Integer>> findDuplicatedHoldings(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                        @RequestParam(value = "brokerAccounts", required = false) Set<String> brokerAccounts,
                                                        @RequestParam(value = "symbols",  required = false) Set<String> symbols) {
        return ResultUtil.success(holdingDataService.findDuplicatedHoldings(brokerType, brokerAccounts, symbols));
    }
}
