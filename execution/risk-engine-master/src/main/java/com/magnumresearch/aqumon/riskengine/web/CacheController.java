package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.pojo.ValidationResult;
import com.magnumresearch.aqumon.riskengine.service.BaseCacheService;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import com.magnumresearch.aqumon.trading.utils.InstrumentUtil;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.asm.Advice;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Set;

@Slf4j
@RestController
@RequestMapping("/cache")
public class CacheController {

    @Autowired
    InstrumentUtil instrumentUtil;

    @RequestMapping(value = "/validation/query", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "get latest cache result by key")
    public Result<ValidationResult> listCacheKey(@RequestParam(value = "key") String key) {
        return ResultUtil.success(BaseCacheService.getResultFromCache(key));
    }

    @RequestMapping(value = "/validation/list", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "list cache key and size of cache queue")
    public Result<Map<String, Integer>> listCacheKey() {
        return ResultUtil.success(BaseCacheService.listCacheKey());
    }

    @RequestMapping(value = "/instrument/sync", method = RequestMethod.PUT)
    @CrossOrigin
    @ApiOperation("sync redis instrument cache")
    public Result<String> syncInstrumentCache() throws TradingException {
        instrumentUtil.syncInstrumentCache();
        return ResultUtil.success();
    }

    @RequestMapping(value = "/instrument/clear", method = RequestMethod.PUT)
    @CrossOrigin
    @ApiOperation("clear redis instrument cache")
    public Result<String> clearInstrumentCache() {
        instrumentUtil.clearInstrumentCache();
        return ResultUtil.success();
    }
}
