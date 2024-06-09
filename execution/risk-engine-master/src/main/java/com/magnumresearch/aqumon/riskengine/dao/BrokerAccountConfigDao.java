package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.BrokerAccountConfig;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface BrokerAccountConfigDao extends JpaRepository<BrokerAccountConfig, Long> {

    BrokerAccountConfig findFirstByIpAndPortOrderByBrokerAccountDesc(String ip, Integer port);

    List<BrokerAccountConfig> findAllByBrokerAccount(String brokerAccount);

    @Query("SELECT DISTINCT CONCAT(i.ip, ':', i.port) FROM BrokerAccountConfig i")
    List<String> findAllDistinctHostPort();

    boolean existsByBrokerAccount(String brokerAccount);
}
