package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.riskengine.service.QuoteValidationService;
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
@RequestMapping("/quotes/compare")
public class QuoteValidationController {

    @Autowired
    QuoteValidationService quoteValidationService;

    @RequestMapping(value = "/datamasterToSnapshot", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "show differences of symbols quotes between DM and Snapshot")
    public Result<ValidationResult> compareDMToSnapshotQuotes(@RequestParam(value = "brokerType", defaultValue = "NEUTRAL") BrokerType brokerType,
                                                              @RequestParam(value = "symbols") Set<String> symbols,
                                                              @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                              @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                              @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(quoteValidationService.compareDMToSnapshotQuotes(brokerType, symbols, ruleName, includeObjects, useCache));
    }

    @RequestMapping(value = "/datamasterToDefault", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "show differences of symbols quotes between DM and default values")
    public Result<ValidationResult> compareDMToDefaultQuotes(@RequestParam(value = "brokerType", defaultValue = "NEUTRAL") BrokerType brokerType,
                                                             @RequestParam(value = "symbols") Set<String> symbols,
                                                             @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                             @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                             @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(quoteValidationService.compareDMToDefaultQuotes(brokerType, symbols, ruleName, includeObjects, true, useCache));
    }

    @RequestMapping(value = "/snapshotToDefault", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "show differences of symbol quotes between snapshot and default values")
    public Result<ValidationResult> compareSnapshotToDefaultQuotes(@RequestParam(value = "brokerType", defaultValue = "NEUTRAL") BrokerType brokerType,
                                                                   @RequestParam(value = "symbols") Set<String> symbols,
                                                                   @RequestParam(value = "ruleName",  required = false) String ruleName,
                                                                   @RequestParam(value = "includeObjects", defaultValue = "false") boolean includeObjects,
                                                                   @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(quoteValidationService.compareDMToDefaultQuotes(brokerType, symbols, ruleName, includeObjects, false, useCache));
    }

    @RequestMapping(value = "/spreadToDefault", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "show spread of bid and ask of each symbol")
    public Result<ValidationResult> compareSpreadFromDM(@RequestParam(value = "brokerType", defaultValue = "NEUTRAL") BrokerType brokerType,
                                                        @RequestParam(value = "symbols") Set<String> symbols,
                                                        @RequestParam(value = "enableStreaming", defaultValue = "true") boolean enableStreaming,
                                                        @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(quoteValidationService.compareDMToDefaultSpread(brokerType, symbols, enableStreaming, useCache));
    }
}
