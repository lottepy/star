package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.taskengine.model.PortfolioTask;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PortfolioTaskDao extends JpaRepository<PortfolioTask, Long> {
}
