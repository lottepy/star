package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.Holding;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface HoldingDao extends JpaRepository<Holding,Long> {
    List<Holding> findBySubAccountId(Long subAccountId);
    List<Holding> findByBrokerType(BrokerType brokerType);
    List<Holding> findByBrokerTypeAndBrokerAccount(BrokerType brokerType, String brokerAccount);
    List<Holding> findByBrokerTypeAndInstrumentId(BrokerType brokerType, String instrumentId);
    List<Holding> findByBrokerTypeAndBrokerAccountAndInstrumentId(BrokerType brokerType, String brokerAccount, String instrumentId);
}
