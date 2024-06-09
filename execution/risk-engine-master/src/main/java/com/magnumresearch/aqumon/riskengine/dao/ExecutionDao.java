package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.Execution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ExecutionDao extends JpaRepository<Execution, Long>, JpaSpecificationExecutor<Execution> {
    List<Execution> findByBrokerTypeAndBrokerAccountAndInstrumentId(BrokerType brokerType, String brokerAccount, String instrumentId);
}
