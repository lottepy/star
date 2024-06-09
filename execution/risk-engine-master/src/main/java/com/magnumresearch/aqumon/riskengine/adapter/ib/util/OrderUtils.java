package com.magnumresearch.aqumon.riskengine.adapter.ib.util;

import com.ib.client.Order;
import com.ib.client.OrderType;
import com.ib.client.Types;
import com.magnumresearch.aqumon.trading.constants.OrderDirectionType;
import com.magnumresearch.aqumon.trading.constants.OrderPriceType;
import org.springframework.cloud.commons.util.InetUtils;
import org.springframework.cloud.commons.util.InetUtilsProperties;

import java.sql.Timestamp;
import java.util.Date;
import java.util.Random;

public class OrderUtils {
    public static Order aqumonOrderToIbOrder(int orderId, com.magnumresearch.aqumon.trading.model.Order order) {
        Order ibOrder = new Order();
        ibOrder.orderId(orderId);
        ibOrder.orderRef(String.valueOf(orderId));
        if (order.getDirection().equals(OrderDirectionType.BUY)) {
            ibOrder.action(Types.Action.BUY);
        } else if (order.getDirection().equals(OrderDirectionType.SELL)) {
            ibOrder.action(Types.Action.SELL);
        }
        if (order.getPriceType().equals(OrderPriceType.LIMIT)) {
            ibOrder.orderType(OrderType.LMT);
            ibOrder.lmtPrice(order.getPriceSent().doubleValue());
        } else if (order.getPriceType().equals(OrderPriceType.MARKET)) {
            ibOrder.orderType(OrderType.MKT);
        }
        ibOrder.totalQuantity(order.getQuantitySent().intValue());
        ibOrder.account(order.getBrokerAccount());
        return ibOrder;
    }

    public static Integer getEnvId(boolean ibIsLive, String brokerAcount) {
        // 环境号生成: (IB) envId = 环境开头号 + IP(保留3位) + brokerAccount(保留5位)
        String subBrokerAccountId = brokerAcount.substring(brokerAcount.length() - 5);
        InetUtils inetUtils = new InetUtils(new InetUtilsProperties());
        try {
            String envIp = inetUtils.findFirstNonLoopbackAddress().getHostAddress().replace(".", "");
            envIp = envIp.substring(envIp.length() - 3);
            if (ibIsLive) { //区分docker
                return Integer.valueOf("2" + envIp + subBrokerAccountId); //实盘1开头
            } else {
                return Integer.valueOf("8" + envIp + subBrokerAccountId); //模拟盘9开头
            }
        } catch (Exception ignored) {
            return (int) (new Timestamp(new Date().getTime()).getTime() % 10000L) + new Random().nextInt(1000);
        }
    }


//    public staticW Execution ibExecutionToAqumonExecution(com.ib.client.Execution ibExecution) {
//        Execution execution = new Execution();
//        execution.setExtOrderId(String.valueOf(ibExecution.orderId()));
//        execution.setExtExecutionId(ibExecution.execId());
//        execution.setPriceFilled(BigDecimal.valueOf(ibExecution.price()));
//        execution.setQuantityFilled(BigDecimal.valueOf(ibExecution.shares()));
//        execution.setAmountFilled(
//                BigDecimal.valueOf(ibExecution.price()).multiply(BigDecimal.valueOf(ibExecution.shares())));
//        execution.setBrokerAccount(ibExecution.acctNumber());
////        execution.setDirection(
////                "BOT".equalsIgnoreCase(ibExecution.side()) ? OrderDirectionType.BUY : OrderDirectionType.SELL);
//        execution.setBrokerType(BrokerType.IB);
//        return execution;
//    }
}
