package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.pojo.InstrumentDataMaster;
import com.magnumresearch.aqumon.trading.utils.InstrumentUtil;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/instrument/")
public class InstrumentDataController {

    @Autowired
    InstrumentUtil instrumentUtil;

    @RequestMapping(value = "/instrumentId", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get instrument data from Cache or DM")
    public Result<InstrumentDataMaster> getInstrumentFromDM(@RequestParam(value = "instrumentId") String instrumentId) throws TradingException {
        return ResultUtil.success(instrumentUtil.getInstrumentFromDM(instrumentId));
    }
}
