package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.trading.constants.SubAccountType;
import com.magnumresearch.aqumon.trading.model.SubAccount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface SubAccountDao extends JpaRepository<SubAccount, Long> {

    //查找独立子账户
    SubAccount findFirstByAccountIdAndSubAccountType(Long accountId, SubAccountType accountType);

    List<SubAccount> findBySubAccountType(SubAccountType accountType);

    String sql = "SELECT * FROM account_manager WHERE broker_account=:broker_account";
    @Query(value = sql, nativeQuery = true)
    List<SubAccount> findAllAccountManagerByBrokerAccount(@Param("broker_account") String broker_account);
}