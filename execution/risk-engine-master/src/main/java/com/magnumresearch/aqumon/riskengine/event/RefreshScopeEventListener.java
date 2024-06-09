package com.magnumresearch.aqumon.riskengine.event;

import com.magnumresearch.aqumon.riskengine.service.TaskScheduleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.environment.EnvironmentChangeEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
public class RefreshScopeEventListener implements ApplicationListener<EnvironmentChangeEvent> {

    @Autowired
    TaskScheduleService taskScheduleService;

    Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * listener on the change of refreshscope annotation
     * @param event environment change event once refreshscope is called
     */
    @Override
    public void onApplicationEvent(EnvironmentChangeEvent event) {
        if (Objects.nonNull(event.getKeys()) && event.getKeys().size() > 0) {
            for (String key: event.getKeys()) {
                if (key.startsWith("risk.health.")) {
                    log.info("[RiskEngine][RefreshScope] detected environment change event: Event=" + key);
                    taskScheduleService.rescheduleJobs();
                }
            }
        }
    }
}
