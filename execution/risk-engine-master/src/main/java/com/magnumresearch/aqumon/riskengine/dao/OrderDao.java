package com.magnumresearch.aqumon.riskengine.dao;

import com.magnumresearch.aqumon.trading.constants.BrokerType;
import com.magnumresearch.aqumon.trading.constants.OrderStatusType;
import com.magnumresearch.aqumon.trading.model.Order;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OrderDao extends JpaRepository<Order, Long>, JpaSpecificationExecutor<Order> {

    List<Order> findByEnvIdAndExtOrderIdAndBrokerTypeAndBrokerAccount(
            Integer envId, String extOrderId, BrokerType brokerType, String brokerAccount
    );

    List<Order> findByBatchId(String batchId);

    List<Order> findByIdBetween(long startId, long endId);

    List<Order> findByBrokerType(BrokerType brokerType);

    List<Order> findByBrokerTypeAndBrokerAccount(BrokerType brokerType, String brokerAccount);

    Boolean existsByBrokerTypeAndBrokerAccountAndExtOrderId(BrokerType brokerType, String brokerAccount, String extOrderId);

    List<Order> findByStatus(OrderStatusType status);

    String sql_1 = "SELECT *, CAST(ext_order_id AS UNSIGNED) AS ext_id FROM trading_order WHERE broker_type=:brokerType AND ext_order_id is NOT NULL ORDER BY ext_id DESC LIMIT 1";

    //String sql_new = "SELECT *, CAST(ext_order_id AS UNSIGNED) AS ext_id FROM trading_order WHERE broker_id=:brokerId AND ext_order_id is NOT NULL AND (ext_order_id REGEXP '[^0-9.]') = 0 ORDER BY ext_id DESC LIMIT 1";
    @Query(value = sql_1, nativeQuery = true)
    Order findFirstByBrokerTypeOrderByExtOrderIdAsIntDesc(@Param("brokerType") BrokerType brokerType);

    String sql_2 = "SELECT * FROM trading_order WHERE instrument_id=:instrumentId AND ext_order_id=:ext_order_id";

    @Query(value = sql_2, nativeQuery = true)
    List<Order> findByInstrumentIdANDExtOrderId(@Param("instrumentId") String instrumentId, @Param("ext_order_id") String ext_order_id);

    String sql_3 = "select MAX(id) from trading_order";

    @Query(value = sql_3, nativeQuery = true)
    Long findMaxId();

}
