package com.magnumresearch.aqumon.riskengine.adapter.kuanrui.config;

import com.quant360.api.client.MdsClient;
import com.quant360.api.client.OesClient;
import com.quant360.api.client.impl.MdsClientImpl;
import com.quant360.api.client.impl.OesClientImpl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

@Configuration
@Slf4j
public class KuanruiClientConfiguration {

//    @Value("${adapter.kuanrui.configpath}")
    //TODO why zk config not working???
//    private String oesConfigPath = "/home/aqumon/config/kuanrui-config.json";
//    private String mdsConfigPath = "/home/aqumon/config/kuanrui-mds-config.json";
    private String oesConfigPath = "/etc/kuanrui-config.json";
    private String mdsConfigPath = "/etc/kuanrui-mds-config.json";
//    private String oesConfigPath = "kuanrui-config.json";
//    private String mdsConfigPath = "kuanrui-mds-config.json";

    @Bean(name = "oesClient")
    public OesClient getOesClient() {
        try {
            //Resource resource = new ClassPathResource(oesConfigPath);
            //return new OesClientImpl(1, resource.getFile().getAbsolutePath());
            return new OesClientImpl(1, oesConfigPath);
        } catch (Exception e) {
            log.error("Failed to initiate Kuanrui OES client due to: {}", e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Bean(name = "mdsClient")
    public MdsClient getMdsClient() {
        try {
            //Resource resource = new ClassPathResource(mdsConfigPath);
            //return new MdsClientImpl(resource.getFile().getAbsolutePath());
            return new MdsClientImpl(mdsConfigPath);
        } catch (Exception e) {
            log.error("Failed to initiate Kuanrui MDS client due to: {}", e.getMessage());
            e.printStackTrace();
            return null;
        }
    }
}
