package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.trading.model.SubAccount;
import com.magnumresearch.aqumon.trading.model.SubAccountSummary;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SubAccountSummaryDao extends JpaRepository<SubAccountSummary, Long> {
    List<SubAccountSummary> findBySubAccount(SubAccount subAccount);
}
