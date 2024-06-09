package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.riskengine.model.ExtExecution;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * @author zhuazhua
 */
@Repository
public interface ExtExecutionDao extends JpaRepository<ExtExecution, Long>, JpaSpecificationExecutor<ExtExecution> {

    String sql = "SELECT * FROM trading_ext_execution WHERE instrument_id=:instrumentId AND ext_order_id=:extOrderId AND ext_execution_id=:ext_execution_id";
    @Query(value=sql, nativeQuery = true)
    List<ExtExecution> findExtExecutionByInstrumentIdANDExtOrderIdANDExtExecutionId(@Param("instrumentId") String instrumentId, @Param("extOrderId") String extOrderId, @Param("ext_execution_id") String ext_execution_id);

}
