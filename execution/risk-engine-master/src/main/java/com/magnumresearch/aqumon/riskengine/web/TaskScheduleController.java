package com.magnumresearch.aqumon.riskengine.web;

import com.magnumresearch.aqumon.common.pojo.Result;
import com.magnumresearch.aqumon.common.utils.ResultUtil;
import com.magnumresearch.aqumon.riskengine.service.ConfigValidationService;
import com.magnumresearch.aqumon.riskengine.service.TaskScheduleService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/task/schedule")
public class TaskScheduleController {

    @Autowired
    TaskScheduleService taskScheduleService;
    @Autowired
    ConfigValidationService configValidationService;

    @RequestMapping(value = "/reschedule", method = RequestMethod.POST)
    @CrossOrigin
    @ApiOperation(value = "reschedule jobs")
    public Result<Map<String, String>> rescheduleJobs() {
        return ResultUtil.success(taskScheduleService.rescheduleJobs());
    }

    @RequestMapping(value = "/list", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "list scheduled jobs")
    public Result<Map<String, String>> listScheduledJobs() {
        return ResultUtil.success(taskScheduleService.listScheduledJobs());
    }

    @RequestMapping(value = "/clear", method = RequestMethod.DELETE)
    @CrossOrigin
    @ApiOperation(value = "clear scheduled jobs")
    public Result<Map<String, String>> clearScheduledJobs() {
        return ResultUtil.success(taskScheduleService.clearScheduledJobs());
    }

    @RequestMapping(value = "/validate", method = RequestMethod.GET)
    @CrossOrigin
    @ApiOperation(value = "validate configurations")
    public Result<Map<String, String>> validateConfigurations() {
        return ResultUtil.success(configValidationService.validateConfigs());
    }
}
