package com.magnumresearch.aqumon.riskengine;

import com.magnumresearch.aqumon.riskengine.config.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.SpringCloudApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;

@SpringCloudApplication
@EntityScan(basePackages = {"com.magnumresearch.aqumon.trading.model", "com.magnumresearch.aqumon.taskengine.model", "com.magnumresearch.aqumon.riskengine.model"})
@ComponentScan(basePackages = {"com.magnumresearch.aqumon.common", "com.magnumresearch.aqumon.trading", "com.magnumresearch.aqumon.riskengine"})
@EnableFeignClients(basePackages = {"com.magnumresearch.aqumon.common.feign", "com.magnumresearch.aqumon.riskengine"})
@EnableConfigurationProperties({BaseFilterConfig.class, BaseRulesConfig.class, BaseSchedulesConfig.class})
public class RiskEngineApplication {
    public static void main(String[] args) {
        SpringApplication.run(RiskEngineApplication.class, args);
    }
}
