package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.riskengine.service.InstrumentValidationService;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/instrument/validate")
public class InstrumentValidationController {

    @Autowired
    InstrumentValidationService instrumentValidationService;

    @RequestMapping(value = "/fieldsFromDatamaster", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "validate fields existence for instruments from datamaster")
    public Result<ValidationResult> validateInstrumentFieldsFromDM(@RequestParam(value = "symbols") Set<String> symbols,
                                                                   @RequestParam(value = "fields", required = false) Set<String> fields,
                                                                   @RequestParam(value = "ruleName", required = false) String ruleName,
                                                                   @RequestParam(value = "useCache", defaultValue = "false") boolean useCache) {
        return ResultUtil.success(instrumentValidationService.validateInstrumentFieldsFromDM(symbols, fields, ruleName, useCache));
    }
}
