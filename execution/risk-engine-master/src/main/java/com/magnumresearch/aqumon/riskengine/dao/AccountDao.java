package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.model.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author zhuazhua
 */
@Repository
public interface AccountDao extends JpaRepository<Account,Long>, JpaSpecificationExecutor<Account> {
    List<Account> findByBrokerTypeAndBrokerAccount(BrokerType brokerType, String brokerAccount);
    List<Account> findAllByBrokerAccount(String brokerAccount);
    List<Account> findAllByBrokerType(BrokerType brokerType);
}
