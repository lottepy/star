FROM maven:3.6.3-openjdk-8 AS builder
WORKDIR /code
RUN mkdir task-engine trading-engine cash-engine execution-engine quote-engine eureka-server risk-engine
ARG firstHalfMinimalPom="<project>\n<modelVersion>4.0.0</modelVersion>\n<groupId>com.magnumresearch.aqumon</groupId>\n<artifactId>"
ARG secondHalfMinimalPom="</artifactId>\n<version>1</version>\n</project>"
RUN echo $firstHalfMinimalPom'task-engine'$secondHalfMinimalPom >> task-engine/pom.xml && \
    echo $firstHalfMinimalPom'trading-engine'$secondHalfMinimalPom >> trading-engine/pom.xml && \
    echo $firstHalfMinimalPom'cash-engine'$secondHalfMinimalPom >> cash-engine/pom.xml && \
    echo $firstHalfMinimalPom'execution-engine'$secondHalfMinimalPom >> execution-engine/pom.xml && \
    echo $firstHalfMinimalPom'quote-engine'$secondHalfMinimalPom >> quote-engine/pom.xml && \
    echo $firstHalfMinimalPom'eureka-server'$secondHalfMinimalPom >> eureka-server/pom.xml && \
    echo $firstHalfMinimalPom'risk-engine'$secondHalfMinimalPom >> risk-engine/pom.xml
ADD pom.xml .
RUN mvn dependency:go-offline de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -pl .

ADD risk-engine/pom.xml risk-engine/
RUN mvn dependency:go-offline de.qaware.maven:go-offline-maven-plugin:resolve-dependencies -pl risk-engine

ADD risk-engine risk-engine/
RUN mvn -T 1C package spring-boot:repackage -pl risk-engine

FROM openjdk:8 AS risk-engine
WORKDIR /data
RUN echo "Asia/Hong_Kong" > /etc/timezone
COPY --from=builder /code/risk-engine/target/risk-engine.jar .
EXPOSE 15572
ADD ["https://aqm-algo.oss-cn-hongkong.aliyuncs.com/OMS/kr_configs/test_env/kuanrui-config.json", "https://aqm-algo.oss-cn-hongkong.aliyuncs.com/OMS/kr_configs/test_env/kuanrui-mds-config.json", "/etc/"]
RUN chmod 644 /etc/kuanrui-config.json /etc/kuanrui-mds-config.json
ENTRYPOINT ["java", "-jar", "/data/risk-engine.jar"]
