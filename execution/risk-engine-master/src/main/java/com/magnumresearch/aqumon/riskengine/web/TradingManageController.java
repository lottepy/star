package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.service.TradingManageService;
import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.exception.TradingException;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/trading/manage")
public class TradingManageController {

    @Autowired
    TradingManageService tradingManageService;

    @RequestMapping(value = "/connnectAyers", method = RequestMethod.POST)
    @CrossOrigin
    @ApiOperation("connect to Ayers")
    Result connectAyers() {
        tradingManageService.connectAyers();
        return ResultUtil.success();
    }

    @RequestMapping(value = "/disconnnectAyers", method = RequestMethod.DELETE)
    @CrossOrigin
    @ApiOperation("disconnect Ayers connections")
    Result disconnnect() throws TradingException {
        tradingManageService.disconnect(BrokerType.AYERS, null);
        return ResultUtil.success();
    }

    @RequestMapping(value = "/disconnnectIB", method = RequestMethod.DELETE)
    @CrossOrigin
    @ApiOperation("disconnect IB connection based on account")
    Result disconnnect(@RequestParam(name = "brokerAccount") String brokerAccount) throws TradingException {
        tradingManageService.disconnect(BrokerType.IB, brokerAccount);
        return ResultUtil.success();
    }

    @RequestMapping(value = "/disconnectAllIB", method = RequestMethod.DELETE)
    @CrossOrigin
    @ApiOperation("disconnect all IB connections")
    Result disconnectAllIB() {
        tradingManageService.disconnectAllIB();
        return ResultUtil.success();
    }

    @RequestMapping(value = "/refreshIbBrokerAccountConfig", method = RequestMethod.PUT)
    @CrossOrigin
    @ApiOperation("refresh Ib brokerAccount Config")
    Result refreshIbBrokerAccountConfig() {
        tradingManageService.refreshIbBrokerAccountConfig();
        return ResultUtil.success();
    }

    @RequestMapping(value = "/keepAlive", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation("test connections")
    public Result<Object> keepAlive() throws Exception {
        tradingManageService.keepAlive();
        return ResultUtil.success();
    }

    @RequestMapping(value = "/setMaintenance", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation("set maintenance mode for particular broker")
    public Result<Object> setBrokerMaintenance(@RequestParam(name = "brokerType") BrokerType brokerType) throws TradingException{
        tradingManageService.setMaintenance(brokerType);
        return ResultUtil.success();
    }

    @RequestMapping(value = "/unsetMaintenance", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation("unset maintenance mode for particular broker")
    public Result<Object> unsetBrokerMaintenance(@RequestParam(name = "brokerType") BrokerType brokerType) throws TradingException{
        tradingManageService.unsetMaintenance(brokerType);
        return ResultUtil.success();
    }
}
