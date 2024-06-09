package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.HoldingHistory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface HoldingHistoryDao extends JpaRepository<HoldingHistory,Long> {
    List<HoldingHistory> findByDate(String date);
    List<HoldingHistory> findByDateAndBrokerType(String date, BrokerType brokerType);
    List<HoldingHistory> findByDateAndBrokerAccount(String date, String brokerAccount);
    List<HoldingHistory> findByDateAndBrokerTypeAndBrokerAccount(String date, BrokerType brokerType, String brokerAccount);
    List<HoldingHistory> findByDateAndInstrumentId(String date, String instrumentId);
    List<HoldingHistory> findByDateAndBrokerTypeAndInstrumentId(String date, BrokerType brokerType, String instrumentId);
    List<HoldingHistory> findByDateAndBrokerAccountAndInstrumentId(String date, String brokerAccount, String instrumentId);
    List<HoldingHistory> findByDateAndBrokerTypeAndBrokerAccountAndInstrumentId(String date, BrokerType brokerType, String brokerAccount, String instrumentId);
}
