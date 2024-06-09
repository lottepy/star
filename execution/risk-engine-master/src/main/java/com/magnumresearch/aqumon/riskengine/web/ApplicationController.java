package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import io.swagger.annotations.ApiOperation;
import org.springframework.boot.devtools.restart.Restarter;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/application")
public class ApplicationController {

    @RequestMapping(value = "/restart", method = RequestMethod.POST)
    @CrossOrigin
    @ApiOperation(value = "restart application")
    public Result<String> restart() {
        Restarter.getInstance().restart();
        return ResultUtil.success("Application Restarted");
    }
}
