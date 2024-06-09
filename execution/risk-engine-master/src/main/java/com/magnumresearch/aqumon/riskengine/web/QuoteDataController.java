package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.quoteengine.pojo.SnapshotResponse;
import com.magnumresearch.aqumon.riskengine.service.QuoteDataService;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/quotes")
public class QuoteDataController {
    @Autowired
    QuoteDataService quoteDataService;

    @RequestMapping(value = "/datamaster", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get symbol quotes from datamaster")
    public Result<Map<String, SnapshotResponse>> getQuotesFromDM(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                 @RequestParam(value = "symbols") Set<String> symbols) {
        return ResultUtil.success(quoteDataService.getQuotesFromDM(brokerType, symbols, true));
    }

    @RequestMapping(value = "/snapshot", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get symbol quotes from snapshot")
    public Result<Map<String, SnapshotResponse>> getQuotesFromSnapshot(@RequestParam(value = "brokerType") BrokerType brokerType,
                                                                       @RequestParam(value = "symbols") Set<String> symbols) {
        return ResultUtil.success(quoteDataService.getQuotesFromDM(brokerType, symbols, false));
    }
}
