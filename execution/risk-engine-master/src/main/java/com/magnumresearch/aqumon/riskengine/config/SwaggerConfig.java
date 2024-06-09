package com.magnumresearch.aqumon.riskengine.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import static springfox.documentation.builders.PathSelectors.regex;

@Configuration
@EnableSwagger2
public class SwaggerConfig {

    @Value("${api2doc.title:Risk Engine}")
    String docTitle;

    @Value("${api2doc.description:Risk Engine APIs}")
    String docDescription;

    @Bean
    public Docket productApi() {
        return new Docket(DocumentationType.SWAGGER_2).apiInfo(produceApiInfo()).select()
                .apis(RequestHandlerSelectors.basePackage("com.magnumresearch.aqumon.riskengine."))
                .apis(RequestHandlerSelectors.withClassAnnotation(RestController.class)).paths(regex("/.*")).build();
    }

    private ApiInfo produceApiInfo() {
        return new ApiInfoBuilder().title(docTitle).description(docDescription).build();
    }

}
